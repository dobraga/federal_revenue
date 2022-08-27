package main

import (
	"os"
)

func CheckFile(filename string, filesize int64) (*os.File, bool, error) {
	stat, err := os.Stat(filename)
	if err == nil {
		if stat.Size() == filesize {
			return &os.File{}, true, nil
		}
	}

	os.Remove(filename)
	out, err := os.Create(filename)

	return out, false, err
}
