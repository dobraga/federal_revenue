package main

import (
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

	var qtd_downloaded, qtd_total int
	var errors []error
	t := StartTimer()

	err := os.MkdirAll(PATH_TEMP, 0777)
	if err != nil {
		panic(err)
	}

	files, err := ExtractUrls(URL)
	if err != nil {
		panic(err)
	}

	qtd_total = files.Len()
	errors = files.Run(PATH, PATH_TEMP, GCS_PATH, CHUNK_SIZE)
	qtd_downloaded = qtd_total - len(errors)
	t.Close("Processed %d/%d files", "INFO", errors, qtd_downloaded, qtd_total)

	if len(errors) > 0 {
		os.Exit(1)
	}
}
