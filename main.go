package main

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// Url and path
const URL = "http://200.152.38.155/CNPJ/"
const PATH = "data"
const PATH_TEMP = "data/temp"

// Download the partitions
const MAX_GOROUTINES = 500
const CHUNK_SIZE = 10_000_000 // 10mb

// Retry configuration
const REQUEST_TIMEOUT_MINUTES = 30
const MAX_RETRY = 5

func main() {
	tini := time.Now()

	files, err := ExtractUrls(URL)
	if err != nil {
		panic(err)
	}

	errs := files.Run()
	timer := time.Since(tini).Minutes()
	if len(errs) == 0 {
		os.RemoveAll(PATH_TEMP)
		logrus.Infof("Downloaded %d files in %.2f minutes", files.Len(), timer)
	} else {
		logrus.Warnf("Downloaded %d(%d total) files with errors in %.2f minutes", len(errs), files.Len(), timer)
	}
}
