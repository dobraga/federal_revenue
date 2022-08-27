package main

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

func Downloads(urls []string) error {
	wg := new(sync.WaitGroup)
	errors := make(chan error, len(urls))

	for _, url := range urls {
		wg.Add(1)

		go func(u string, w *sync.WaitGroup) {
			defer w.Done()

			err := Download(u)
			if err != nil {
				log.Error(err)
				errors <- err
			}

		}(url, wg)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		return err
	}

	return nil

}
