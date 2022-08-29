package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type File struct {
	Url          string
	Size         int
	Chunks       [][2]int
	UpdatedAtStr string
	UpdatedAt    time.Time
	Filename     string
	LocalOutput  string
	BucketOutput string
}

var time_layout = "2006-01-02 15:04"

func (f *File) Defaults() error {
	// Set filename if not passed
	if f.Filename == "" {
		split := strings.Split(f.Url, "/")
		f.Filename = split[len(split)-1]
	}

	// Convert string to date time
	updated, err := time.Parse(time_layout, strings.Trim(f.UpdatedAtStr, " "))
	if err != nil {
		return err
	}
	f.UpdatedAt = updated

	// Local and Bucket output path
	f.LocalOutput = filepath.Join(PATH, f.Filename)
	f.BucketOutput = filepath.Join(updated.Format("200601"), f.Filename)

	return nil
}

// Check already downloaded, Download and upload to storage
func (f *File) Run() error {
	uploaded := f.CheckUploaded()
	downloaded := f.CheckDownloaded()

	// Uploaded
	if uploaded {
		if downloaded {
			// Downloaded
			logrus.Infof("Already processed '%s'", f.Url)
			return nil
		} else {
			// Not downloaded
			err := Storage.Download(f.BucketOutput, f.LocalOutput)
			return err
		}
	}

	if downloaded {
		// Downloaded but not uploaded
		err := Storage.Upload(f.LocalOutput, f.BucketOutput)
		return err
	}

	// Not downloaded and not uploaded
	err := f.Download(CHUNK_SIZE)
	if err != nil {
		return err
	}
	err = Storage.Upload(f.LocalOutput, f.BucketOutput)
	return err
}

// Download to local file
func (f *File) Download(chunk_size int) error {
	tini := time.Now()

	chunks := f.SetChunks(chunk_size)

	logrus.Infof("Downloading '%s' to '%s' %d bytes in %d parts", f.Url, f.LocalOutput, f.Size, len(chunks))

	// Concurrent download
	err := ParallelDownload(HttpClient, f.Url, f.Filename, chunks)
	if err != nil {
		err = fmt.Errorf("error downloading '%s': %v", f.Url, err)
		logrus.Error(err)
		return err
	}

	// Merge parts of file
	files := []string{}
	for i := range chunks {
		files = append(files, filepath.Join(PATH_TEMP, fmt.Sprintf("%s.part%d", f.Filename, i)))
	}

	err = MergeParts(files, f.LocalOutput)
	if err != nil {
		return err
	}
	for _, file := range files {
		os.Remove(file)
	}

	timer := time.Since(tini).Minutes()
	logrus.Infof("Downloaded '%s' to '%s' in %.2f minutes", f.Url, f.LocalOutput, timer)

	return nil
}

// Check if this file exists in cloud storage
func (f *File) CheckUploaded() bool {
	return Storage.Exists(f.BucketOutput)
}

// Check file exists in local
func (f *File) CheckDownloaded() bool {
	stat, err := os.Stat(f.LocalOutput)
	if err == nil {
		filesize, _ := f.FileSize()
		if stat.Size() == int64(filesize) {
			return true
		}
	}
	os.Remove(f.LocalOutput)
	return false
}

// Set chunks to download
func (f *File) SetChunks(chunk_size int) [][2]int {
	end, _ := f.FileSize()

	r := [][2]int{}
	i := 0

	for i < end {
		end_step := i + chunk_size - 1
		if end_step > end {
			end_step = end
		}

		r = append(r, [2]int{i, end_step})
		i += chunk_size
	}

	f.Chunks = r
	return r
}

// Get file size
func (f *File) FileSize() (int, error) {
	if f.Size != 0 {
		return f.Size, nil
	}

	resp, err := HttpClient.Head(f.Url)
	if err != nil {
		return 0, err
	}

	size, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return 0, err
	}

	f.Size = size

	return size, nil
}
