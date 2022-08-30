package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/sirupsen/logrus"
)

var BQ *BigQueryHandle

type BigQueryHandle struct {
	cli     *bigquery.Client
	dataset *bigquery.Dataset
}

type partition struct {
	CREATED_AT time.Time
}

// Check lasted partition
func (bq *BigQueryHandle) LastPartition(tablename, origin string) (time.Time, error) {
	ctx := context.Background()

	q := bq.cli.Query(fmt.Sprintf(`
		SELECT CAST(MAX(CREATED_AT) AS TIMESTAMP) AS CREATED_AT
		  FROM %s.%s.%s
		 WHERE DATE(CREATED_AT) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
		   AND ORIGIN = "%s"
	`, os.Getenv("PROJECT_ID"), os.Getenv("DATASET_NAME"), tablename, origin))
	it, err := q.Read(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("error getting last partition from %s: %v", tablename, err)
	}

	for {
		var row partition
		err := it.Next(&row)
		if err == nil {
			return row.CREATED_AT, nil
		} else {
			return time.Time{}, nil
		}
	}
}

// Add new partition
func (bq *BigQueryHandle) NewPartition(origin string, updated_at time.Time) {

}

// Upload local csv
func (bq *BigQueryHandle) UploadLocalData(filename, tablename string) error {
	t := Timer{}
	t.Start()
	defer t.Close(fmt.Sprintf("Upload from '%s' to BigQuery '%s'", filename, tablename), "INFO")

	ctx := context.Background()

	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening '%s': %v", filename, err)
	}
	source := bigquery.NewReaderSource(f)
	source.AutoDetect = false
	source.SkipLeadingRows = 0
	source.FieldDelimiter = ";"

	jsonFile, err := os.ReadFile(fmt.Sprintf("resources/%s.json", tablename))
	if err != nil {
		return fmt.Errorf("error loading table schema '%s': %v", tablename, err)
	}
	schema, err := bigquery.SchemaFromJSON(jsonFile)
	if err != nil {
		return err
	}
	source.Schema = schema

	loader := BQ.dataset.Table(tablename).LoaderFrom(source)

	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("error loading data from '%s' to BigQuery '%s': %v", filename, tablename, err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error loading data from '%s' to BigQuery '%s': %v", filename, tablename, err)
	}
	err = status.Err()
	if err != nil {
		return fmt.Errorf("error loading data from '%s' to BigQuery '%s': %v", filename, tablename, err)
	}

	return nil
}

func InitBQ() {
	logrus.Debug("Creating client BigQuery")

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, os.Getenv("PROJECT_ID"))
	if err != nil {
		logrus.Fatalf("Failed to create BigQuery client: %v", err)
	}

	BQ = &BigQueryHandle{
		client,
		client.Dataset(os.Getenv("DATASET_NAME")),
	}

	logrus.Debug("Created client BigQuery")
}
