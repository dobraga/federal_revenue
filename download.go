package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var semaphore = make(chan int, MAX_GOROUTINES)

func Download(url string) error {
	tini := time.Now()

	filename := get_filename_from_url(url)
	output := PATH + "/" + filename

	_, err := os.Stat(output)
	if err == nil {
		log.Infof("File '%s' already downloaded", output)
		return nil
	}

	client := http.Client{
		Timeout: 60 * time.Minute,
	}

	size, err := file_size(client, url)
	if err != nil {
		return err
	}

	chunks := Range(0, size, CHUNK_SIZE)

	log.Infof("Downloading '%s' %d bytes in %d parts", url, size, len(chunks))

	err = concurrent_download(client, url, filename, chunks)
	if err != nil {
		return err
	}

	files := []string{}
	for i := range chunks {
		files = append(files, fmt.Sprintf("%s/%s.part%d", PATH_TEMP, filename, i))
	}

	err = MergeParts(files, output)
	if err != nil {
		return err
	}

	timer := time.Since(tini).Minutes()
	log.Infof("Downloaded '%s' to '%s' in %.2f minutes", url, output, timer)

	return nil
}

func concurrent_download(c http.Client, u, o string, chunks [][2]int) error {

	wg := new(sync.WaitGroup)
	errors := make(chan error, len(chunks))

	for i, chunk := range chunks {
		wg.Add(1)
		semaphore <- 1

		go func(
			c http.Client, u, o string, i int, chunk [2]int, w *sync.WaitGroup) {
			defer w.Done()

			err := retry_download_range(c, u, o, i, chunk[0], chunk[1], 10)
			<-semaphore
			if err != nil {
				log.Error(err)
				errors <- err
			}
		}(c, u, o, i, chunk, wg)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		return err
	}

	return nil
}

func file_size(client http.Client, url string) (int, error) {
	resp, err := client.Head(url)
	if err != nil {
		return 0, err
	}

	size, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return 0, err
	}

	return size, nil
}

func retry_download_range(client http.Client, url, filename string, part, ini, end, retries int) error {
	var err error

	for i := 0; i <= retries; i++ {
		err = download_range(client, url, filename, part, ini, end)
		if err == nil {
			return nil
		}

		log.Warnf("Error fetching '%s' part %d in %d attempt: %v", url, part, i, err)
	}

	return err
}

func download_range(client http.Client, url, filename string, part, ini, end int) error {
	tini := time.Now()
	filesize := int64(end - ini + 1)

	// Create the file
	file := fmt.Sprintf("%s/%s.part%d", PATH_TEMP, filename, part)

	out, ok, err := CheckFile(file, filesize)
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

func get_filename_from_url(url string) string {
	split := strings.Split(url, "/")
	filename := split[len(split)-1]
	return filename
}
