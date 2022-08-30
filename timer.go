package main

import (
	"time"

	"github.com/sirupsen/logrus"
)

type Timer struct {
	Ini     time.Time
	Minutes float64
}

func StartTimer() Timer {
	t := Timer{}
	t.Start()
	return t
}

func (t *Timer) Start() {
	t.Ini = time.Now()
}

func (t *Timer) Close(message, level string, err error) {
	t.Minutes = time.Since(t.Ini).Minutes()

	if err != nil {
		logrus.Errorf("%s in %.2f minutes: %v", message, t.Minutes, err)
		return
	}

	if level == "INFO" {
		logrus.Infof("%s in %.2f minutes", message, t.Minutes)
		return
	}
	if level == "DEBUG" {
		logrus.Debugf("%s in %.2f minutes", message, t.Minutes)
		return
	}
	if level == "WARN" {
		logrus.Warnf("%s in %.2f minutes", message, t.Minutes)
		return
	}
}
