package logger

import (
	"os"

	log "github.com/sirupsen/logrus"
)

var Logger *log.Logger

func Init(level string) {
	Logger = log.New()

	Logger.SetOutput(os.Stdout)
	Logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	switch level {
	case "debug":
		Logger.SetLevel(log.DebugLevel)
	case "info":
		Logger.SetLevel(log.InfoLevel)
	case "warn":
		Logger.SetLevel(log.WarnLevel)
	case "error":
		Logger.SetLevel(log.ErrorLevel)
	default:
		Logger.SetLevel(log.InfoLevel)
	}
}

func Debug(msg string, fields ...log.Fields) {
	if len(fields) > 0 {
		Logger.WithFields(fields[0]).Debug(msg)
		return
	}
	Logger.Debug(msg)
}

func Info(msg string, fields ...log.Fields) {
	if len(fields) > 0 {
		Logger.WithFields(fields[0]).Info(msg)
		return
	}
	Logger.Info(msg)
}

func Warn(msg string, fields ...log.Fields) {
	if len(fields) > 0 {
		Logger.WithFields(fields[0]).Warn(msg)
		return
	}
	Logger.Warn(msg)
}

func Error(msg string, fields ...log.Fields) {
	if len(fields) > 0 {
		Logger.WithFields(fields[0]).Error(msg)
		return
	}
	Logger.Error(msg)
}
