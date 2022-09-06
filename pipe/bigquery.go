package main

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/sirupsen/logrus"
)

var BQ *BigQueryHandle

type BigQueryHandle struct {
	cli     *bigquery.Client
	dataset *bigquery.Dataset
}

// Run a query
func (bq *BigQueryHandle) Run(query string) (*bigquery.RowIterator, error) {
	ctx := context.Background()

	q := bq.cli.Query(query)
	j, err := q.Run(ctx)
	if err != nil {
		return &bigquery.RowIterator{}, err
	}

	_, err = j.Wait(ctx)
	if err != nil {
		return &bigquery.RowIterator{}, err
	}

	return j.Read(ctx)
}

// Upload local csv
func (bq *BigQueryHandle) UploadLocalData(filename, tablename string) error {
	var err error
	timer := StartTimer()
	defer func(e error) {
		timer.Close("Upload from '%s' to BigQuery '%s'", "INFO", err, filename, tablename)
	}(err)

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
