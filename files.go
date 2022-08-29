package main

import (
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

type Files struct {
	f []File
}

func (fs *Files) Run() []error {
	wg := new(sync.WaitGroup)
	errors := make(chan error, len(fs.f))
	all_errors := []error{}

	for _, file := range fs.f {
		wg.Add(1)

		go func(f File, w *sync.WaitGroup) {
			defer w.Done()

			err := f.Run()
			if err != nil {
				log.Error(err)
				errors <- err
			}

		}(file, wg)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		all_errors = append(all_errors, err)
	}

	if len(errors) == 0 {
		os.RemoveAll(PATH_TEMP)
	}

	return all_errors
}

func (fs *Files) Len() int {
	return len(fs.f)
}
