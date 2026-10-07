package main

// Set at build time by GoReleaser via -ldflags "-X main.version=...".
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)
