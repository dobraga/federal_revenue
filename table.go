package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"cloud.google.com/go/bigquery"
)

type Table struct {
	Name string
}

func (t *Table) Create() error {
	var err error
	ctx := context.Background()

	tableRef := BQ.dataset.Table(t.Name)
	_, err = tableRef.Metadata(ctx)

	// Occurs error when table not exits
	if err != nil {
		metaData, err := tableMetadata(fmt.Sprintf("resources/%s.json", t.Name))
		if err != nil {
			return fmt.Errorf("error creating metadata table '%s': %v", t.Name, err)
		}

		err = tableRef.Create(ctx, metaData)
		if err != nil {
			return fmt.Errorf("error creating table '%s': %v", t.Name, err)
		}

		return nil
	}
	return nil
}

func tableMetadata(file string) (*bigquery.TableMetadata, error) {
	// Read json file and decode
	jsonFile, err := os.Open(file)
	if err != nil {
		return &bigquery.TableMetadata{}, err
	}

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return &bigquery.TableMetadata{}, err
	}

	schema, err := bigquery.SchemaFromJSON(byteValue)
	if err != nil {
		return &bigquery.TableMetadata{}, err
	}

	metaData := &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:                   bigquery.DayPartitioningType,
			Field:                  "CREATED_AT",
			RequirePartitionFilter: true,
		},
	}

	return metaData, nil
}
