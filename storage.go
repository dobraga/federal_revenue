package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
)

var Storage *StorageHandle

type StorageHandle struct {
	storage *storage.BucketHandle
}

func InitStorage() {
	logrus.Debug("Creating client storage")
	client, err := storage.NewClient(context.Background())
	if err != nil {
		logrus.Fatalf("Failed to create bucket client: %v", err)
	}

	Storage = &StorageHandle{
		client.Bucket(os.Getenv("BUCKET_NAME")),
	}
	logrus.Debug("Created client storage")
}

// Upload local file to google cloud storage
func (s *StorageHandle) Upload(file, output string) error {
	var err error
	timer := StartTimer()
	defer func(e error) {
		timer.Close("Upload from '%s' to Storage '%s'", "INFO", err, file, output)
	}(err)

	// Set timeout
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// Upload an object with storage.Writer
	wc := s.storage.Object(output).NewWriter(ctx)
	defer wc.Close()

	reader, err := os.Open(file)
	if err != nil {
		err = fmt.Errorf("failed uploading '%s' to Storage '%s': %v", file, output, err)
		logrus.Error(err)
		return err
	}
	defer reader.Close()

	_, err = io.Copy(wc, reader)
	if err != nil {
		err = fmt.Errorf("failed uploading '%s' to Storage '%s': %v", file, output, err)
		logrus.Error(err)
		return err
	}

	return nil
}

// Download google cloud storage to local
func (s *StorageHandle) Download(file, output string) error {
	var err error
	timer := StartTimer()
	defer func(e error) {
		timer.Close("Downloaded from Storage '%s' to '%s'", "INFO", err, file, output)
	}(err)

	// Check file exists
	_, err = os.Stat(output)
	if err == nil {
		err = fmt.Errorf("'%s' already exists, delete it before run it", output)
		logrus.Error(err)
		return err
	}

	// Set timeout
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// Reader
	rc, err := s.storage.Object(file).NewReader(ctx)
	if err != nil {
		err = fmt.Errorf("error downloading from Storage '%s' to '%s': %v", file, output, err)
		logrus.Error(err)
		return err
	}
	defer rc.Close()

	// New file
	f, err := os.Create(output)
	if err != nil {
		err = fmt.Errorf("error downloading from Storage '%s' to '%s': %v", file, output, err)
		logrus.Error(err)
		return err
	}
	defer f.Close()

	// Write
	_, err = io.Copy(f, rc)
	if err != nil {
		err = fmt.Errorf("error downloading from Storage '%s' to '%s': %v", file, output, err)
		logrus.Error(err)
		return err
	}

	return nil
}

// Remove file
func (s *StorageHandle) Remove(file string) error {
	ctx := context.Background()
	obj := s.storage.Object(file)
	return obj.Delete(ctx)
}

// Check file exists in google cloud storage
func (s *StorageHandle) Exists(file string) bool {
	ctx := context.Background()
	obj, _ := s.storage.Object(file).Attrs(ctx)

	if obj == nil {
		return false
	} else {
		return true
	}
}
