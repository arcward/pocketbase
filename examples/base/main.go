package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/tools/cron"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/forms"
	"github.com/pocketbase/pocketbase/models"
	"github.com/pocketbase/pocketbase/models/schema"
	"github.com/pocketbase/pocketbase/plugins/ghupdate"
	"github.com/pocketbase/pocketbase/plugins/jsvm"
	"github.com/pocketbase/pocketbase/plugins/migratecmd"
	"github.com/pocketbase/pocketbase/tools/filesystem"
	"github.com/pocketbase/pocketbase/tools/types"
)

const (
	fileCollectionName          = "downloads"
	jobRelationName             = "job"
	downloadMimetypeFieldName   = "mimetype"
	downloadFilenameFieldName   = "filename"
	downloadContentFieldName    = "content"
	downloadDownloadedFieldName = "downloaded"
	downloadRetriesFieldName    = "retries"
	downloadHashFieldName       = "hash"
	downloadErrorFieldName      = "error"
	downloadEncodingFieldName   = "encoding"
	downloadSizeFieldName       = "size"
	downloadStatusFieldName     = "status"
	jobURLFieldName             = "url"
	jobCompressFieldName        = "compress"
	jobCollectionName           = "jobs"
)

var (
	maxFileSelect        = 1
	lockedRule           = "locked"
	downloadCfg          = downloadConfig{}
	pendingDownloadQuery = `
		SELECT id, created 
		FROM downloads
		WHERE downloaded IS NULL OR downloaded = ''
		ORDER BY created ASC;
`
	pendingDownloadQueryLimitRetries = `
		SELECT id, created 
		FROM downloads
		WHERE 
		    (downloaded IS NULL OR downloaded = '')
		AND retries < {:max_retries}
		ORDER BY created ASC;
`

	jobCollection = &models.Collection{
		Name:       jobCollectionName,
		Type:       models.CollectionTypeBase,
		UpdateRule: &lockedRule,
		DeleteRule: &lockedRule,
		Schema: schema.NewSchema(
			&schema.SchemaField{
				Name:        jobURLFieldName,
				Type:        schema.FieldTypeUrl,
				Required:    true,
				Presentable: true,
			},
			&schema.SchemaField{
				Name:     jobCompressFieldName,
				Type:     schema.FieldTypeBool,
				Required: false,
			},
		),
	}
)

type Download struct {
	models.BaseModel

	ID         string         `db:"id" json:"id"`
	File       []byte         `db:"file" json:"file"`
	Downloaded types.DateTime `db:"downloaded" json:"downloaded"`
	Error      string         `db:"error" json:"error"`
	Retries    int            `db:"retries" json:"retries"`
	VerifySSL  bool           `db:"verify_ssl" json:"verify_ssl"`
	Hash       string         `db:"hash" json:"hash"`
	Filename   string         `db:"filename" json:"filename"`
	MimeType   string         `db:"mimetype" json:"mimetype"`
	Encoding   string         `db:"encoding" json:"encoding"`
	Size       int            `db:"size" json:"size"`
}

type downloadConfig struct {
	MaxRetries int
	Schedule   string
}

func main() {
	app := pocketbase.New()

	// ---------------------------------------------------------------
	// Optional plugin flags:
	// ---------------------------------------------------------------
	app.RootCmd.PersistentFlags().StringVar(
		&downloadCfg.Schedule,
		"download-schedule",
		"*/5 * * * *",
		"the cron schedule for checking for pending downloads",
	)

	app.RootCmd.PersistentFlags().IntVar(
		&downloadCfg.MaxRetries,
		"download-max-retries",
		5,
		"Max number of retries for a download. 0=no limit",
	)

	var hooksDir string
	app.RootCmd.PersistentFlags().StringVar(
		&hooksDir,
		"hooksDir",
		"",
		"the directory with the JS app hooks",
	)

	var hooksWatch bool
	app.RootCmd.PersistentFlags().BoolVar(
		&hooksWatch,
		"hooksWatch",
		true,
		"auto restart the app on pb_hooks file change",
	)

	var hooksPool int
	app.RootCmd.PersistentFlags().IntVar(
		&hooksPool,
		"hooksPool",
		25,
		"the total prewarm goja.Runtime instances for the JS app hooks execution",
	)

	var migrationsDir string
	app.RootCmd.PersistentFlags().StringVar(
		&migrationsDir,
		"migrationsDir",
		"",
		"the directory with the user defined migrations",
	)

	var automigrate bool
	app.RootCmd.PersistentFlags().BoolVar(
		&automigrate,
		"automigrate",
		true,
		"enable/disable auto migrations",
	)

	var publicDir string
	app.RootCmd.PersistentFlags().StringVar(
		&publicDir,
		"publicDir",
		defaultPublicDir(),
		"the directory to serve static files",
	)

	var indexFallback bool
	app.RootCmd.PersistentFlags().BoolVar(
		&indexFallback,
		"indexFallback",
		true,
		"fallback the request to index.html on missing static path (eg. when pretty urls are used with SPA)",
	)

	var queryTimeout int
	app.RootCmd.PersistentFlags().IntVar(
		&queryTimeout,
		"queryTimeout",
		30,
		"the default SELECT queries timeout in seconds",
	)

	app.RootCmd.ParseFlags(os.Args[1:])

	// ---------------------------------------------------------------
	// Plugins and hooks:
	// ---------------------------------------------------------------

	// load jsvm (hooks and migrations)
	jsvm.MustRegister(
		app, jsvm.Config{
			MigrationsDir: migrationsDir,
			HooksDir:      hooksDir,
			HooksWatch:    hooksWatch,
			HooksPoolSize: hooksPool,
		},
	)

	// migrate command (with js templates)
	migratecmd.MustRegister(
		app, app.RootCmd, migratecmd.Config{
			TemplateLang: migratecmd.TemplateLangJS,
			Automigrate:  automigrate,
			Dir:          migrationsDir,
		},
	)

	// GitHub selfupdate
	ghupdate.MustRegister(app, app.RootCmd, ghupdate.Config{})

	app.OnAfterBootstrap().PreAdd(
		func(e *core.BootstrapEvent) error {
			app.Dao().ModelQueryTimeout = time.Duration(queryTimeout) * time.Second
			return nil
		},
	)

	app.OnBeforeServe().Add(
		func(e *core.ServeEvent) error {
			// serves static files from the provided public dir (if exists)
			e.Router.GET(
				"/*",
				apis.StaticDirectoryHandler(os.DirFS(publicDir), indexFallback),
			)
			return nil
		},
	)

	downloadCtx, downloadCancel := context.WithCancel(context.Background())
	defer downloadCancel()
	downloads := make(chan string, 1)

	wg := &sync.WaitGroup{}

	app.OnBeforeServe().Add(
		func(e *core.ServeEvent) error {
			// collectionID is required to create the relation
			var collectionID string
			downloadJobCollection, _ := app.Dao().FindCollectionByNameOrId(jobCollectionName)
			if downloadJobCollection == nil {
				err := app.Dao().SaveCollection(jobCollection)
				if err != nil {
					app.Logger().Error("error saving collection", "error", err)
					panic(err)
				}
				downloadJobCollection, _ = app.Dao().FindCollectionByNameOrId(jobCollectionName)
				collectionID = downloadJobCollection.Id
			} else {
				collectionID = downloadJobCollection.Id
			}

			if collectionID == "" {
				panic("unable to find or create job collection")
			}

			downloadCollection, _ := app.Dao().FindCollectionByNameOrId(fileCollectionName)

			fileCollection := &models.Collection{
				Name:       fileCollectionName,
				Type:       models.CollectionTypeBase,
				UpdateRule: &lockedRule,
				DeleteRule: &lockedRule,
				CreateRule: &lockedRule,
				Schema: schema.NewSchema(
					&schema.SchemaField{
						Name: jobRelationName,
						Type: schema.FieldTypeRelation,
						Options: &schema.RelationOptions{
							CollectionId: collectionID,
						},
					},
					&schema.SchemaField{
						Name: downloadMimetypeFieldName,
						Type: schema.FieldTypeText,
					},
					&schema.SchemaField{
						Name: downloadFilenameFieldName,
						Type: schema.FieldTypeText,
					},
					&schema.SchemaField{
						Name:     downloadContentFieldName,
						Type:     schema.FieldTypeFile,
						Required: false,
						Options: &schema.FileOptions{
							Protected: true,
							MaxSelect: maxFileSelect,
							MaxSize:   5242880,
						},
					},
					&schema.SchemaField{
						Name:        downloadDownloadedFieldName,
						Type:        schema.FieldTypeDate,
						Required:    false,
						Presentable: true,
					},
					&schema.SchemaField{
						Name:     downloadErrorFieldName,
						Type:     schema.FieldTypeText,
						Required: false,
					},
					&schema.SchemaField{
						Name:        downloadRetriesFieldName,
						Type:        schema.FieldTypeNumber,
						Required:    false,
						Presentable: true,
					},
					&schema.SchemaField{
						Name:        downloadHashFieldName,
						Type:        schema.FieldTypeText,
						Required:    false,
						Presentable: true,
					},
					&schema.SchemaField{
						Name:     downloadEncodingFieldName,
						Type:     schema.FieldTypeText,
						Required: false,
					},
					&schema.SchemaField{
						Name:     downloadSizeFieldName,
						Type:     schema.FieldTypeNumber,
						Required: false,
					},
					&schema.SchemaField{
						Name:     downloadStatusFieldName,
						Type:     schema.FieldTypeText,
						Required: false,
					},
				),
			}
			if downloadCollection == nil {
				err := app.Dao().SaveCollection(fileCollection)
				if err != nil {
					app.Logger().Error("error saving collection", "error", err)
					panic(err)
				}
			}

			runDownloadWorkers(app, wg, downloads, runtime.GOMAXPROCS(0))

			scheduler := cron.New()
			scheduler.MustAdd(
				"download_pending", downloadCfg.Schedule, func() {
					app.Logger().Info(
						"checking for pending downloads",
						"max_retries", downloadCfg.MaxRetries,
					)

					var data []Download
					var query *dbx.Query
					switch downloadCfg.MaxRetries {
					case 0:
						query = app.Dao().DB().NewQuery(pendingDownloadQuery)
					default:
						query = app.Dao().DB().NewQuery(pendingDownloadQueryLimitRetries).Bind(
							dbx.Params{"max_retries": downloadCfg.MaxRetries},
						)
					}
					if err := query.All(&data); err != nil {
						app.Logger().Error(
							"error getting pending downloads",
							"error",
							err,
						)
						return
					}

					for _, d := range data {
						if time.Since(d.Created.Time()) <= 5*time.Minute {
							continue
						}
						if downloadCtx.Err() != nil {
							break
						}
						downloads <- d.ID
					}
				},
			)
			scheduler.Start()

			return nil
		},
	)

	app.OnRecordAfterCreateRequest(jobCollectionName).Add(
		func(e *core.RecordCreateEvent) error {
			app.Logger().Info(
				"got event",
				"event",
				e.Collection.Name,
				"rec",
				e.Record.Id,
			)

			coll, err := app.Dao().FindCollectionByNameOrId(fileCollectionName)
			if err != nil {
				return err
			}

			rec := models.NewRecord(coll)
			rec.Set(downloadRetriesFieldName, 0)
			rec.Set(jobRelationName, []string{e.Record.Id})
			rec.Set(downloadStatusFieldName, "pending")
			if err = app.Dao().SaveRecord(rec); err != nil {
				app.Logger().Error("unable to save record", "error", err)
				return err
			}
			downloads <- rec.Id
			return nil
		},
	)

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
	close(downloads)
	app.Logger().Info("waiting for download workers to stop")
	wg.Wait()
}

// runDownloadWorkers starts listening on the download channel
func runDownloadWorkers(
	app *pocketbase.PocketBase,
	wg *sync.WaitGroup,
	downloads <-chan string,
	workers int,
) {
	client := &http.Client{}
	app.Logger().Info("starting workers", "workers", workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range downloads {
				app.Logger().Info("received download job", "id", id)
				if downloadErr := downloadRecord(
					app,
					client,
					id,
				); downloadErr != nil {
					app.Logger().Error(
						"error downloading record",
						"id",
						id,
						"error",
						downloadErr,
					)
					continue
				}
				app.Logger().Info("downloaded record", "id", id)
			}
		}()
	}
	app.Logger().Info("started workers")
}

// downloadRecord downloads the URL from the job record associated
// with the given download record ID, and populates the download record
func downloadRecord(
	app *pocketbase.PocketBase,
	client *http.Client,
	id string,
) error {
	rec, err := app.Dao().FindRecordById(
		fileCollectionName,
		id,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error finding record: %w", err)
	}

	jobErr := app.Dao().ExpandRecord(rec, []string{jobRelationName}, nil)
	if jobErr != nil {
		errs := []error{}
		for k, v := range jobErr {
			if v == nil {
				continue
			}
			errs = append(errs, fmt.Errorf("%s: %w", k, v))
		}
		err = errors.Join(errs...)
		if err != nil {
			return fmt.Errorf("error expanding record: %w", err)
		}
	}

	jobRec := rec.ExpandedOne(jobRelationName)
	fileURL := jobRec.Get(jobURLFieldName).(string)

	u, err := url.Parse(fileURL)
	if err != nil {
		return fmt.Errorf("error parsing url '%s': %w", fileURL, err)
	}

	rec.Set(downloadStatusFieldName, "downloading")
	saveErr := app.Dao().SaveRecord(rec)
	if saveErr != nil {
		app.Logger().Error(
			"error saving record download status",
			"error",
			saveErr,
		)
	}

	app.Logger().Info(
		"downloading",
		"id",
		id,
		"url",
		u.String(),
	)
	downloadResp, downloadErr := client.Get(fileURL)

	if downloadErr != nil {
		rec.Set(downloadErrorFieldName, downloadErr.Error())
		retries := rec.GetInt(downloadRetriesFieldName)
		retries++
		rec.Set(downloadRetriesFieldName, retries)

		saveErr := app.Dao().SaveRecord(rec)
		if saveErr != nil {
			saveErr = fmt.Errorf("error saving record: %w", saveErr)
		}
		return errors.Join(downloadErr, saveErr)
	}

	if downloadResp.StatusCode != http.StatusOK {
		statusErr := fmt.Errorf(
			"expected http status code %d, got: %d",
			http.StatusOK,
			downloadResp.StatusCode,
		)
		rec.Set(downloadErrorFieldName, statusErr.Error())
		retries := rec.GetInt(downloadRetriesFieldName)
		retries++
		rec.Set(downloadRetriesFieldName, retries)
		saveErr := app.Dao().SaveRecord(rec)
		if saveErr != nil {
			saveErr = fmt.Errorf("error saving record: %w", saveErr)
		}
		return errors.Join(statusErr, saveErr)
	}

	var mimeType string
	var filename string
	var body []byte

	b := downloadResp.Body
	body, err = io.ReadAll(b)
	if err != nil {
		rec.Set(downloadErrorFieldName, err.Error())
		retries := rec.GetInt(downloadRetriesFieldName)
		retries++
		rec.Set(downloadRetriesFieldName, retries)
		saveErr := app.Dao().SaveRecord(rec)
		if saveErr != nil {
			saveErr = fmt.Errorf("error saving record: %w", saveErr)
		}
		return errors.Join(err, saveErr)
	}
	_ = b.Close()

	hash := md5.Sum(body)
	app.Logger().Info(
		"downloaded file",
		downloadHashFieldName, hash,
		jobURLFieldName, u,
		"id", id,
	)

	cd := downloadResp.Header.Get("Content-Disposition")
	if cd != "" {
		_, params, cdErr := mime.ParseMediaType(cd)
		if cdErr == nil {
			filename = params["filename"]
		}
	}

	if filename == "" {
		filename = path.Base(u.Path)
		filename = strings.Split(filename, "?")[0]
	}

	mimeType = downloadResp.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = http.DetectContentType(body)
	}

	// if there's no file extension (ex: downloading URL example.com/foo/),
	// try to guess it from the mimetype
	if ext := filepath.Ext(filename); ext == "" {
		extensions, extErr := mime.ExtensionsByType(mimeType)
		if extErr == nil && extensions != nil {
			filename = fmt.Sprintf("%s%s", filename, extensions[0])
		}
	}

	compress := jobRec.GetBool(jobCompressFieldName)
	if compress && !strings.HasSuffix(
		strings.ToLower(filename),
		".gz",
	) && !strings.HasSuffix(strings.ToLower(filename), ".gzip") {
		var buf bytes.Buffer
		gzipper := gzip.NewWriter(&buf)
		if _, err = gzipper.Write(body); err != nil {
			return fmt.Errorf("error compressing file: %w", err)
		}
		if err = gzipper.Close(); err != nil {
			return fmt.Errorf("error compressing file: %w", err)
		}
		body = buf.Bytes()
		filename = fmt.Sprintf("%s.gz", filename)
	}

	ups := forms.NewRecordUpsert(app, rec)
	ups.LoadData(
		map[string]any{
			downloadHashFieldName:       hex.EncodeToString(hash[:]),
			downloadDownloadedFieldName: time.Now(),
			downloadMimetypeFieldName:   mimeType,
			downloadFilenameFieldName:   filename,
			downloadEncodingFieldName:   downloadResp.Header.Get("Content-Encoding"),
			downloadSizeFieldName:       len(body),
			downloadStatusFieldName:     "complete",
		},
	)
	fs, fsErr := filesystem.NewFileFromBytes(body, filename)
	if fsErr != nil {
		return fmt.Errorf("error creating file: %w", fsErr)
	}
	if err = ups.AddFiles(downloadContentFieldName, fs); err != nil {
		return fmt.Errorf("error adding file: %w", err)
	}
	if err = ups.Submit(); err != nil {
		return fmt.Errorf("error saving record: %w", err)
	}

	app.Logger().Info("base storage path", "path", rec.BaseFilesPath())
	return nil
}

// the default pb_public dir location is relative to the executable
func defaultPublicDir() string {
	if strings.HasPrefix(os.Args[0], os.TempDir()) {
		// most likely ran with go run
		return "./pb_public"
	}

	return filepath.Join(os.Args[0], "../pb_public")
}
