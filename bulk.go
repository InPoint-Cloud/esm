// SPDX-License-Identifier: Apache-2.0
// See LICENSE.md in the repository root for license details.

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/cihub/seelog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// number of bulk item errors that are logged, the rest are only counted
const maxLoggedBulkErrors = 10

// MigrationStats counts the documents of a run, shared by all bulk workers
type MigrationStats struct {
	Sent    int64
	Indexed int64
	Failed  int64
	// set when the source could not be read completely, ie: a scroll request failed
	ReadFailed int32
	// set when copying settings or mappings failed
	SetupFailed int32

	lock         sync.Mutex
	loggedErrors int

	// failed documents are written to this file in the dump format, see --failed_output
	failedLock   sync.Mutex
	failedFile   *os.File
	failedPath   string
	failedSaved  int64
	failedErrors int64
}

func (s *MigrationStats) MarkSetupFailed() {
	atomic.StoreInt32(&s.SetupFailed, 1)
}

func (s *MigrationStats) OpenFailedOutput(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	s.failedFile = f
	s.failedPath = path
	return nil
}

func (s *MigrationStats) CloseFailedOutput() {
	s.failedLock.Lock()
	defer s.failedLock.Unlock()
	if s.failedFile != nil {
		s.failedFile.Close()
		s.failedFile = nil
	}
}

// saveFailed writes a failed bulk item (action line plus source line) as a dump line that -i can read
func (s *MigrationStats) saveFailed(item []byte) {
	if s.failedFile == nil {
		return
	}
	actionLine, source, found := bytes.Cut(item, []byte{'\n'})
	action := map[string]Document{}
	if err := json.Unmarshal(actionLine, &action); err != nil {
		atomic.AddInt64(&s.failedErrors, 1)
		return
	}
	doc, ok := action["index"]
	if !ok || !found {
		// deletes have no document to save
		return
	}
	doc.Source = bytes.TrimRight(source, "\n")
	line, err := json.Marshal(doc)
	if err != nil {
		atomic.AddInt64(&s.failedErrors, 1)
		return
	}
	line = append(line, '\n')

	s.failedLock.Lock()
	defer s.failedLock.Unlock()
	if s.failedFile == nil {
		return
	}
	if _, err := s.failedFile.Write(line); err != nil {
		atomic.AddInt64(&s.failedErrors, 1)
		log.Error("can not write failed document: ", err)
		return
	}
	s.failedSaved++
}

func (s *MigrationStats) recordFailure(reason string) {
	atomic.AddInt64(&s.Failed, 1)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.loggedErrors++
	if s.loggedErrors <= maxLoggedBulkErrors {
		log.Errorf("failed to index document: %s", reason)
	} else if s.loggedErrors == maxLoggedBulkErrors+1 {
		log.Errorf("more documents failed, only the first %d errors are logged", maxLoggedBulkErrors)
	}
}

func (s *MigrationStats) HasFailures() bool {
	return atomic.LoadInt64(&s.Failed) > 0 || atomic.LoadInt32(&s.ReadFailed) > 0 || atomic.LoadInt32(&s.SetupFailed) > 0
}

func (s *MigrationStats) PrintSummary() {
	fmt.Printf("documents sent: %d, indexed: %d, failed: %d\n",
		atomic.LoadInt64(&s.Sent), atomic.LoadInt64(&s.Indexed), atomic.LoadInt64(&s.Failed))
	if atomic.LoadInt32(&s.ReadFailed) > 0 {
		fmt.Println("reading from source failed, not all documents were migrated, check the log for details")
	}
	if atomic.LoadInt32(&s.SetupFailed) > 0 {
		fmt.Println("copying index settings or mappings failed, check the log for details")
	}
	if len(s.failedPath) > 0 {
		s.failedLock.Lock()
		saved := s.failedSaved
		s.failedLock.Unlock()
		fmt.Printf("failed documents saved to %s: %d\n", s.failedPath, saved)
		if n := atomic.LoadInt64(&s.failedErrors); n > 0 {
			fmt.Printf("%d failed documents could not be saved, check the log for details\n", n)
		}
	}
}

// isRetryable returns true for errors worth retrying: overloaded or unavailable
// cluster (429/502/503/504) and network errors
func isRetryable(err error) bool {
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		switch statusErr.Code {
		case 429, 502, 503, 504:
			return true
		}
		return false
	}
	return true
}

// backoff returns the wait time before retry number attempt (starting at 1): 1s, 2s, 4s, ... capped at 30s
func backoff(attempt int) time.Duration {
	d := time.Second << uint(attempt-1)
	if d > 30*time.Second || d <= 0 {
		d = 30 * time.Second
	}
	return d
}

// withRetry runs fn and retries it on retryable errors, up to Config.MaxRetries times
func (m *Migrator) withRetry(operation string, fn func() error) error {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil || !isRetryable(err) || attempt >= m.Config.MaxRetries {
			return err
		}
		wait := backoff(attempt + 1)
		log.Warnf("%s failed, retry %d/%d in %s: %s", operation, attempt+1, m.Config.MaxRetries, wait,
			SubString(err.Error(), 0, 300))
		time.Sleep(wait)
	}
}

// nextScroll fetches the next scroll page with retries, on failure the run is marked as incomplete
func (m *Migrator) nextScroll(api ESAPI, scrollTime string, scrollId string) (ScrollAPI, error) {
	var scroll ScrollAPI
	err := m.withRetry("scroll request", func() error {
		var err error
		scroll, err = api.NextScroll(scrollTime, scrollId)
		return err
	})
	if err != nil {
		atomic.StoreInt32(&m.Stats.ReadFailed, 1)
		log.Errorf("scroll request failed, stop reading from source: %s", SubString(err.Error(), 0, 500))
	}
	return scroll, err
}

// bulkErrorReason formats the error of a bulk item, ie: "mapper_parsing_exception: failed to parse field [price]"
func bulkErrorReason(action Action) string {
	if e, ok := action.Error.(map[string]interface{}); ok {
		return fmt.Sprintf("index=%s id=%s status=%d %v: %v", action.Index, action.Id, action.Status, e["type"], e["reason"])
	}
	reason, _ := json.Marshal(action.Error)
	return fmt.Sprintf("index=%s id=%s status=%d %s", action.Index, action.Id, action.Status, SubString(string(reason), 0, 300))
}

// flushBulk sends bulk items (each one is the action line plus the source line, newline terminated)
// to api, retries failed requests and items rejected with 429, and records the results.
// It returns the number of successfully indexed items.
func (m *Migrator) flushBulk(api ESAPI, items [][]byte) int {
	if len(items) == 0 {
		return 0
	}
	atomic.AddInt64(&m.Stats.Sent, int64(len(items)))
	succeeded := 0
	pending := items

	for attempt := 0; len(pending) > 0; attempt++ {
		canRetry := attempt < m.Config.MaxRetries
		buf := bytes.Buffer{}
		for _, item := range pending {
			buf.Write(item)
		}

		response, err := api.Bulk(&buf)
		if err != nil {
			if canRetry && isRetryable(err) {
				wait := backoff(attempt + 1)
				log.Warnf("bulk request failed, retry %d/%d in %s: %s", attempt+1, m.Config.MaxRetries, wait,
					SubString(err.Error(), 0, 300))
				time.Sleep(wait)
				continue
			}
			log.Errorf("bulk request of %d documents failed: %s", len(pending), SubString(err.Error(), 0, 500))
			atomic.AddInt64(&m.Stats.Failed, int64(len(pending)))
			for _, item := range pending {
				m.Stats.saveFailed(item)
			}
			break
		}

		var rejected [][]byte
		for i, item := range pending {
			if i >= len(response.Items) {
				m.Stats.recordFailure("missing in bulk response")
				m.Stats.saveFailed(item)
				continue
			}
			for op, action := range response.Items[i] {
				if action.Status < 300 || (op == "delete" && action.Status == 404) {
					succeeded++
				} else if action.Status == 429 && canRetry {
					rejected = append(rejected, item)
				} else {
					m.Stats.recordFailure(bulkErrorReason(action))
					m.Stats.saveFailed(item)
				}
			}
		}
		pending = rejected
		if len(pending) > 0 {
			wait := backoff(attempt + 1)
			log.Warnf("%d documents rejected by target (429), retry %d/%d in %s", len(pending), attempt+1,
				m.Config.MaxRetries, wait)
			time.Sleep(wait)
		}
	}

	atomic.AddInt64(&m.Stats.Indexed, int64(succeeded))
	return succeeded
}
