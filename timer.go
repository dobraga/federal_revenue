package main

import (
	"fmt"
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

func (t *Timer) Close(format, level string, err interface{}, args ...interface{}) {
	t.Minutes = time.Since(t.Ini).Minutes()

	mult_err, mult_ok := err.([]error)
	one_err, one_ok := err.(error)

	if mult_ok && len(mult_err) > 0 {
		format = "Failed: " + format + fmt.Sprintf(" in %.2f minutes: %d errors: %+v", t.Minutes, len(mult_err), mult_err)
		logrus.Errorf(format, args...)
		return
	}
	if one_ok && one_err != nil {
		format = "Failed: " + format + fmt.Sprintf(" in %.2f minutes error: %+v", t.Minutes, err)
		logrus.Errorf(format, args...)
		return
	}

	message := format + fmt.Sprintf(" in %.2f minutes", t.Minutes)
	if level == "INFO" {
		logrus.Infof(message, args...)
		return
	}
	if level == "DEBUG" {
		logrus.Debugf(message, args...)
		return
	}
	if level == "WARN" {
		logrus.Warnf(message, args...)
		return
	}
}
