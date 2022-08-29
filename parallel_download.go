package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

			err := retry_download_range(c, u, o, i, chunk[0], chunk[1], MAX_RETRY)
			<-semaphore
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

func retry_download_range(client http.Client, url, filename string, part, ini, end, retries int) error {
	var err error

	for i := 0; i <= retries; i++ {
		err = download_range(client, url, filename, part, ini, end)
		if err == nil {
			return nil
		}
		err = fmt.Errorf("error fetching '%s' part %d in %d attempt error: %v", url, part, i, err)
		log.Warn(err)
	}

	return err
}

func download_range(client http.Client, url, filename string, part, ini, end int) error {
	tini := time.Now()
	filesize := end - ini + 1

	// Check file exists or Create the file
	file := filepath.Join(PATH_TEMP, fmt.Sprintf("%s.part%d", filename, part))
	out, ok, err := checkFileCreate(file, filesize)
	if ok {
		log.Debugf(fmt.Sprintf("Already downloaded '%s' part %d", url, part))
		return nil
	}
	if err != nil {
		return err
	}
	defer out.Close()

	// Make a partial request
	log.Debugf("Downloading part %d from '%s' with %d bytes", part, url, filesize)
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
		return fmt.Errorf("partial request from '%s' part %d return status code %d", url, part, resp.StatusCode)
	}

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	timer := time.Since(tini).Minutes()
	log.Debugf("Downloaded part %d from '%s' in %.2f minutes", part, url, timer)
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
