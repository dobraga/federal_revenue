package main

import (
	"context"
	"encoding/json"
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
		metaData, err := TableMetadata(fmt.Sprintf("resources/%s.json", t.Name))
		if err != nil {
			return fmt.Errorf("error creating metadata table '%s': %v", t.Name, err)
		}

		tableRef.Create(ctx, metaData)

		return nil
	}
	return nil
}

func TableMetadata(file string) (*bigquery.TableMetadata, error) {
	var map_schema []map[string]interface{}

	// Read json file and decode
	jsonFile, err := os.Open(file)
	if err != nil {
		return &bigquery.TableMetadata{}, err
	}

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return &bigquery.TableMetadata{}, err
	}
	err = json.Unmarshal(byteValue, &map_schema)
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
		Clustering: &bigquery.Clustering{
			Fields: getCluster(map_schema, []string{"ORIGIN", "CNPJ_BASICO", "CNPJ_CPF", "CODIGO"}),
		},
	}

	return metaData, nil
}

func getCluster(schema []map[string]interface{}, all_clusters []string) []string {
	var cluster []string

	for _, s := range schema {
		for _, c := range all_clusters {
			if s["name"] == c {
				cluster = append(cluster, s["name"].(string))
			}

		}
	}

	return cluster
}
