package conf

import (
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

var (
	logger = logrus.New()
	entry  = logrus.NewEntry(logger)
)

var entryFields logrus.Fields

func Debug(args ...interface{}) {
	entry.Info(args...)
}

func Info(args ...interface{}) {
	entry.Info(args...)
}

func Warn(args ...interface{}) {
	entry.Info(args...)
}

func Error(args ...interface{}) {
	entry.Error(args...)
}

func Fatal(args ...interface{}) {
	entry.Fatal(args...)
}

func Debugf(format string, args ...interface{}) {
	entry.Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	entry.Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	entry.Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	entry.Errorf(format, args...)
}

func Fatalf(format string, args ...interface{}) {
	entry.Fatalf(format, args...)
}

func WithFields(fields logrus.Fields) {
	entryFields = fields
	entry = logger.WithFields(fields)
}

func SetMultiOutPut(writers ...io.Writer) {
	// 去除重复 writers
	writersSet := make(map[io.Writer]struct{}, len(writers))
	for _, writer := range writers {
		writersSet[writer] = struct{}{}
	}
	writersSet[os.Stdout] = struct{}{}

	allWriters := make([]io.Writer, 0, len(writersSet))
	for writer := range writersSet {
		allWriters = append(allWriters, writer)
	}

	// 设置multiWriter
	multiWriter := io.MultiWriter(allWriters...)

	// 设置output
	logger.SetOutput(multiWriter)
	entry = logger.WithFields(entryFields)
}
