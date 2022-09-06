package main

import (
	"html/template"
	"os"
	"strings"
	"time"
)

type partition struct {
	UpdatedAt time.Time
}

func IsUpdated(step string, newDate time.Time) (bool, error) {
	lastUpdate, err := GetLastUpdate(step)
	if err != nil {
		return false, err
	}

	if lastUpdate.Year() > 1 && (newDate.After(lastUpdate) || newDate.Equal(lastUpdate)) {
		return true, nil
	}

	return false, nil
}

func GetLastUpdate(step string) (time.Time, error) {
	query := new(strings.Builder)

	temp, err := template.ParseFiles("./resources/BookmarkGet.sql")
	if err != nil {
		return time.Time{}, err
	}

	err = temp.Execute(query, map[string]string{
		"Project": os.Getenv("PROJECT_ID"),
		"Dataset": os.Getenv("DATASET_NAME"),
		"Step":    step})
	if err != nil {
		return time.Time{}, err
	}

	it, err := BQ.Run(query.String())
	if err != nil {
		return time.Time{}, err
	}

	for {
		var row partition
		err := it.Next(&row)
		if err == nil {
			return row.UpdatedAt, nil
		} else {
			return time.Time{}, nil
		}
	}

}

func SetLastUpdate(step string, updatedAt time.Time) error {
	query := new(strings.Builder)

	temp, err := template.ParseFiles("./resources/BookmarkSet.sql")
	if err != nil {
		return err
	}

	err = temp.Execute(query, map[string]string{
		"Project":   os.Getenv("PROJECT_ID"),
		"Dataset":   os.Getenv("DATASET_NAME"),
		"UpdatedAt": updatedAt.Format("2006-01-02 15:04:05"),
		"Step":      step})
	if err != nil {
		return err
	}

	_, err = BQ.Run(query.String())
	return err
}
