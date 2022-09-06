package main

import (
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetLevel(logrus.InfoLevel)
	formatter := &logrus.TextFormatter{
		TimestampFormat:        "15:04:05",
		FullTimestamp:          true,
		DisableLevelTruncation: true,
	}
	logrus.SetFormatter(formatter)

	err := godotenv.Load()
	if err != nil {
		logrus.Fatal("Error loading .env file")
	}
}
