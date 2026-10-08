package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	log "github.com/InPoint-Cloud/esm/internal/log"
	goflags "github.com/jessevdk/go-flags"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run does the migration with the given command line arguments and returns the exit code
func run(args []string) int {
	c := &Config{}
	if _, err := goflags.ParseArgs(c, args); err != nil {
		if flagsErr, ok := err.(*goflags.Error); ok && flagsErr.Type == goflags.ErrHelp {
			return 0
		}
		log.Error(err)
		return 1
	}

	if c.ShowVersion {
		fmt.Printf("esm %s (commit %s, built %s)\n", version, commit, buildDate)
		return 0
	}

	closeLog, err := setupLogging(c.LogLevel, c.LogFile)
	if err != nil {
		log.Error(err)
		return 1
	}
	defer closeLog()

	insecureTLS = c.Insecure

	if c.Pprof != "" {
		go func() {
			// net/http/pprof registers its handlers on the default mux
			log.Infof("pprof listening on http://%s/debug/pprof/", c.Pprof)
			log.Error("pprof server stopped: ", http.ListenAndServe(c.Pprof, nil))
		}()
	}

	if err := validateConfig(c); err != nil {
		log.Error(err)
		return 1
	}

	m := &Migrator{Config: c}
	switch {
	case c.Sync:
		return m.runSync()
	case c.DiffCounts:
		return m.runDiffCounts()
	default:
		return m.runMigration()
	}
}

func validateConfig(c *Config) error {
	if len(c.SourceEs) == 0 && len(c.DumpInputFile) == 0 {
		return fmt.Errorf("no input, type --help for more details")
	}
	if len(c.TargetEs) == 0 && len(c.DumpOutFile) == 0 {
		return fmt.Errorf("no output, type --help for more details")
	}
	if c.SourceEs == c.TargetEs && c.SourceIndexNames == c.TargetIndexName {
		return fmt.Errorf("migration output is the same as the output")
	}
	return nil
}

// connect sets the source and target es apis, it returns false if one of them can not be reached
func (m *Migrator) connect() bool {
	c := m.Config
	m.SourceESAPI = m.ParseEsApi(true, c.SourceEs, c.SourceEsAuthStr, c.SourceEsApiKey, c.SourceProxy)
	if m.SourceESAPI == nil {
		log.Error("can not connect to the source elasticsearch")
		return false
	}
	m.TargetESAPI = m.ParseEsApi(false, c.TargetEs, c.TargetEsAuthStr, c.TargetEsApiKey, c.TargetProxy)
	if m.TargetESAPI == nil {
		log.Error("can not connect to the target elasticsearch")
		return false
	}
	return true
}

// runSync makes the target index equal to the source index
func (m *Migrator) runSync() int {
	c := m.Config
	if len(c.SourceIndexNames) == 0 {
		log.Error("migration sync only support source 1 index to 1 target index")
		return 1
	}
	if len(c.TargetIndexName) == 0 {
		c.TargetIndexName = c.SourceIndexNames
	}
	// sync walks source and target in the same order to compare them
	if c.SortField == "" {
		c.SortField = "_id"
	}
	if !m.connect() {
		return 1
	}
	m.SyncBetweenIndex(m.SourceESAPI, m.TargetESAPI, c)
	m.Stats.PrintSummary()
	if m.Stats.HasFailures() {
		return 1
	}
	return 0
}

// runDiffCounts prints the indexes whose document counts differ between source and target
func (m *Migrator) runDiffCounts() int {
	if !m.connect() {
		return 1
	}
	m.DiffCounts(m.SourceESAPI, m.TargetESAPI)
	return 0
}
