package main

import (
	"io"
	"os"

	log "github.com/InPoint-Cloud/esm/internal/log"
)

// setupLogging logs to stderr, and also to logFile when it is set. The returned func closes the log file.
func setupLogging(level string, logFile string) (func(), error) {
	lvl, err := log.ParseLevel(level)
	if err != nil {
		return nil, err
	}
	if logFile == "" {
		log.Setup(os.Stderr, lvl)
		return func() {}, nil
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	log.Setup(io.MultiWriter(os.Stderr, f), lvl)
	return func() { f.Close() }, nil
}
