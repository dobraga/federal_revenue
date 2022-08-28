package main

import (
	"os"
)

func CheckFile(filename string, filesize int) bool {
	stat, err := os.Stat(filename)
	if err == nil {
		if stat.Size() == int64(filesize) {
			return true
		}
	}
	os.Remove(filename)

	return false
}

func CheckFileCreate(filename string, filesize int) (*os.File, bool, error) {
	valid_size := CheckFile(filename, filesize)
	if valid_size {
		return &os.File{}, true, nil
	}

	out, err := os.Create(filename)
	return out, false, err
}
