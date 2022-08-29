package main

import (
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

func init() {
	err := os.MkdirAll(PATH_TEMP, 0777)
	if err != nil {
		panic(err)
	}

	logrus.SetLevel(logrus.InfoLevel)
	formatter := &logrus.TextFormatter{
		TimestampFormat:        "15:04:05",
		FullTimestamp:          true,
		DisableLevelTruncation: true,
	}
	logrus.SetFormatter(formatter)

	err = godotenv.Load()
	if err != nil {
		logrus.Fatal("Error loading .env file")
	}
}
