package main

import (
	"os"

	"github.com/sirupsen/logrus"
)

const URL = "http://200.152.38.155/CNPJ/"
const PATH = "data"
const PATH_TEMP = "data/temp"
const MAX_GOROUTINES = 250
const CHUNK_SIZE = 10_000_000 // 10mb

func main() {
	os.MkdirAll(PATH, 0755)
	os.MkdirAll(PATH_TEMP, 0755)

	logrus.SetLevel(logrus.InfoLevel)
	formatter := &logrus.TextFormatter{
		TimestampFormat:        "15:04:05",
		FullTimestamp:          true,
		DisableLevelTruncation: true,
	}
	logrus.SetFormatter(formatter)

	urls, err := ExtractUrls(URL)
	if err != nil {
		panic(err)
	}

	Downloads(urls)
	os.RemoveAll(PATH_TEMP)
}
