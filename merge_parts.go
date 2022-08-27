package main

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

func MergeParts(files []string, output string) error {
	os.Remove(output)
	dest, err := os.OpenFile(output, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer dest.Close()

	for _, file := range files {
		o_file, err := os.Open(file)
		if err != nil {
			dest.Close()
			os.Remove(output)
			return err
		}
		bytes_writes, err := io.Copy(dest, o_file)
		if err != nil {
			dest.Close()
			os.Remove(output)
			return err
		}
		logrus.Debugf("Writes %d bytes into '%s'", bytes_writes, file)
		o_file.Close()
	}

	return nil
}
