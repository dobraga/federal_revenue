package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/charmap"
)

type File struct {
	Type            string
	Url             string
	Size            int
	Chunks          [][2]int
	UpdatedAtStr    string
	UpdatedAt       time.Time
	Filename        string
	Path            string
	LocalOutput     string
	LocalUnziped    string
	ProcessedOutput string
	LocalTempOutput string
	BucketOutput    string
}

var time_layout = "2006-01-02 15:04"

func (f *File) Defaults(path, path_temp, gcs_path string) error {
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
	f.Path = path
	f.LocalOutput = filepath.Join(path, f.Filename)
	f.LocalTempOutput = filepath.Join(path_temp, f.Filename)
	f.BucketOutput = filepath.Join(gcs_path, updated.Format("200601"), f.Filename)

	return nil
}

// Check already downloaded, Download and upload to storage
func (f *File) Run(chunk_size int) error {
	var err error
	timer := StartTimer()
	defer func(e error) { timer.Close("Processing '%s'", "INFO", err, f.Url) }(err)

	t := Table{f.Type}
	err = t.Create()
	if err != nil {
		return err
	}

	last_partition, err := BQ.LastPartition(f.Type, f.Filename)
	if err != nil {
		return err
	}
	if last_partition.Year() > 1 && f.UpdatedAt.After(last_partition) {
		logrus.Infof("'%s' already processed in bigquery", f.Url)
		return nil
	}

	uploaded := f.CheckUploaded()
	downloaded := f.CheckDownloaded()

	// Uploaded
	if uploaded {
		if downloaded {
			// Downloaded
			logrus.Infof("'%s' already processed in storage", f.Url)
		} else {
			// Not downloaded
			err = Storage.Download(f.BucketOutput, f.LocalOutput)
			if err != nil {
				return err
			}
		}
	} else {
		if downloaded {
			// Downloaded but not uploaded
			err = Storage.Upload(f.LocalOutput, f.BucketOutput)
			if err != nil {
				return err
			}
		} else {
			// Not downloaded and not uploaded
			err = f.Download(chunk_size)
			if err != nil {
				return err
			}
		}

		err = Storage.Upload(f.LocalOutput, f.BucketOutput)
		if err != nil {
			return err
		}

	}

	// Unzip
	err = f.Extract()
	if err != nil {
		return err
	}
	defer os.Remove(f.LocalUnziped)

	// Add date and origin to file
	err = f.AddFields()
	if err != nil {
		return err
	}
	err = f.RemoveNonASCII()
	if err != nil {
		return err
	}

	err = BQ.UploadLocalData(f.ProcessedOutput, f.Type)
	if err == nil {
		os.Remove(f.ProcessedOutput)
		os.Remove(f.LocalOutput)
	}

	return err
}

// Download to local file
func (f *File) Download(chunk_size int) error {
	var err error
	timer := StartTimer()
	defer func(e error) { timer.Close("Downloaded '%s' to '%s'", "INFO", err, f.Url, f.LocalOutput) }(err)

	f.SetChunks(chunk_size)
	logrus.Infof("Downloading '%s' to '%s' %d bytes in %d parts", f.Url, f.LocalOutput, f.Size, len(f.Chunks))

	// Concurrent download
	err = ParallelDownload(HttpClient, f.Url, f.LocalTempOutput, f.Chunks)
	if err != nil {
		err = fmt.Errorf("error downloading '%s': %v", f.Url, err)
		logrus.Error(err)
		return err
	}

	// Merge parts of file
	files := []string{}
	for i := range f.Chunks {
		files = append(files, fmt.Sprintf("%s.part%d", f.LocalTempOutput, i))
	}

	err = MergeParts(files, f.LocalOutput)
	if err != nil {
		return err
	}
	for _, file := range files {
		os.Remove(file)
	}

	return nil
}

func (f *File) Extract() error {
	var err error
	timer := StartTimer()
	defer func(e error) { timer.Close("Unzip and decode '%s'", "DEBUG", err, f.LocalOutput) }(err)

	zf, err := zip.OpenReader(f.LocalOutput)
	if err != nil {
		return err
	}

	for i, file := range zf.File {
		if i > 0 {
			return fmt.Errorf("zipfile '%s' contains multiple files", f.LocalOutput)
		}

		// Destination
		f.LocalUnziped = filepath.Join(f.Path, file.Name)
		f.ProcessedOutput = fmt.Sprintf("%s_processed", f.LocalUnziped)
		destinationFile, err := os.OpenFile(f.LocalUnziped, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer destinationFile.Close()

		// Zipped
		zipped_file, err := file.Open()
		if err != nil {
			return err
		}
		defer zipped_file.Close()

		// Extract and decode to UTF-8
		r := charmap.Windows1250.NewDecoder().Reader(zipped_file)

		_, err = io.Copy(destinationFile, r)
		if err != nil {
			return err
		}
	}

	return nil
}

// Add fields to processed file
func (f *File) AddFields() error {
	var err error
	timer := StartTimer()
	defer func(e error) { timer.Close("Add fields to '%s'", "DEBUG", err, f.Filename) }(err)

	command := fmt.Sprintf("sed s/$/';\"%s\";\"%s\"'/ '%s' > '%s'", f.Filename, f.UpdatedAt.Format("2006-01-02"), f.LocalUnziped, f.ProcessedOutput)

	cmd := exec.Command("bash", "-c", command)
	cmd.Start()
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("failed to add fields to '%s': %v", f.ProcessedOutput, err)
	}
	return nil
}

// Add fields to processed file
func (f *File) RemoveNonASCII() error {
	var err error
	timer := StartTimer()
	defer func(e error) { timer.Close("Remove non ASCII characters from '%s'", "DEBUG", err, f.Filename) }(err)

	command := fmt.Sprintf("perl -i -pe 's/[^[:ascii:]]//g' '%s'", f.ProcessedOutput)

	cmd := exec.Command("bash", "-c", command)
	cmd.Start()
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("failed to remove non ASCII characters in '%s': %v", f.ProcessedOutput, err)
	}
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
