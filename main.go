package main

import (
	"fmt"
	"os"
)

// Url and path
const URL = "http://200.152.38.155/CNPJ/"
const PATH = "data"
const PATH_TEMP = "data/temp"
const GCS_PATH = ""

// Download the partitions
const MAX_GOROUTINES = 500
const CHUNK_SIZE = 10_000_000 // 10mb

// Retry configuration
const REQUEST_TIMEOUT_MINUTES = 30
const MAX_RETRY = 5

func main() {
	InitBQ()
	InitStorage()

	t := StartTimer()

	err := os.MkdirAll(PATH_TEMP, 0777)
	if err != nil {
		panic(err)
	}

	files, err := ExtractUrls(URL)
	if err != nil {
		panic(err)
	}

	errs := files.Run(PATH, PATH_TEMP, GCS_PATH, CHUNK_SIZE)
	if len(errs) == 0 {
		t.Close(fmt.Sprintf("Downloaded %d files", files.Len()), "INFO", nil)
	} else {
		t.Close(fmt.Sprintf("Downloaded %d/%d files with errors: %+v", len(errs), files.Len(), errs), "WARN", nil)
		os.Exit(1)
	}
}
