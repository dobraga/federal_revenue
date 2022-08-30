package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var semaphore = make(chan int, MAX_GOROUTINES)
var HttpClient = http.Client{
	Timeout: REQUEST_TIMEOUT_MINUTES * time.Minute,
}

// Download chunks in parallel
func ParallelDownload(c http.Client, url, output string, chunks [][2]int) error {

	wg := new(sync.WaitGroup)
	errors := make(chan error, len(chunks))

	for i, chunk := range chunks {
		wg.Add(1)
		semaphore <- 1

		go func(c http.Client, u, o string, i int, chunk [2]int, w *sync.WaitGroup) {
			defer w.Done()
			defer func() { <-semaphore }()

			part_file := fmt.Sprintf("%s.part%d", o, i)
			err := retry_download_range(c, u, part_file, chunk[0], chunk[1], MAX_RETRY)
			if err != nil {
				log.Error(err)
				errors <- err
			}
		}(c, url, output, i, chunk, wg)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		return err
	}

	return nil
}

func retry_download_range(client http.Client, url, filename string, ini, end, retries int) error {
	var err error

	for i := 0; i <= retries; i++ {
		err = download_range(client, url, filename, ini, end)
		if err == nil {
			return nil
		}
		err = fmt.Errorf("error fetching '%s' in %d attempt error: %v", filename, i, err)
		log.Warn(err)
	}

	return err
}

func download_range(client http.Client, url, filename string, ini, end int) error {
	t := StartTimer()
	filesize := end - ini + 1

	// Check file exists or Create the file
	out, ok, err := checkFileCreate(filename, filesize)
	if ok {
		log.Debugf(fmt.Sprintf("Already downloaded '%s'", filename))
		return nil
	}
	if err != nil {
		return err
	}
	defer out.Close()

	// Make a partial request
	log.Debugf("Downloading '%s' with %d bytes", filename, filesize)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", ini, end))

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 206 {
		return fmt.Errorf("partial request '%s' return status code %d", filename, resp.StatusCode)
	}

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	t.Close(fmt.Sprintf("Downloaded part '%s'", filename), "DEBUG")
	return nil
}

func checkFileCreate(filename string, filesize int) (*os.File, bool, error) {
	stat, err := os.Stat(filename)
	if err == nil {
		if stat.Size() == int64(filesize) {
			return &os.File{}, true, nil
		}
	}
	out, err := os.Create(filename)
	return out, false, err
}
