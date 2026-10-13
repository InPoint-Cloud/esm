/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bufio"
	"encoding/json"
	log "github.com/InPoint-Cloud/esm/internal/log"
	"github.com/cheggaaa/pb"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

func (m *Migrator) NewFileReadWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {
	defer wg.Done()
	// the workers stop when DocChan is closed, also if the file can not be read
	defer close(m.DocChan)
	log.Debug("start reading file")
	f, err := os.Open(m.Config.DumpInputFile)
	if err != nil {
		log.Error(err)
		atomic.StoreInt32(&m.Stats.ReadFailed, 1)
		return
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		line, err := r.ReadString('\n')
		if err != nil && err != io.EOF {
			log.Error("can not read ", m.Config.DumpInputFile, ": ", err)
			atomic.StoreInt32(&m.Stats.ReadFailed, 1)
			break
		}
		// the last line may have no newline
		if strings.TrimSpace(line) != "" {
			js := Document{}
			if err := DecodeJson(line, &js); err != nil {
				log.Error(err)
			} else {
				m.DocChan <- js
				pb.Increment()
			}
		}
		if err == io.EOF {
			break
		}
	}
	log.Debug("end reading file")
}

// openDumpOutput opens the --output_file, it is truncated with --truncate_output and appended to otherwise
func openDumpOutput(path string, truncate bool) (*os.File, error) {
	flag := os.O_WRONLY | os.O_CREATE | os.O_APPEND
	if truncate {
		flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	return os.OpenFile(path, flag, 0644)
}

// NewFileDumpWorker writes the documents of DocChan to f. After a write error it keeps reading
// DocChan without writing, so the readers do not block, and marks the output as failed.
func (c *Migrator) NewFileDumpWorker(f *os.File, pb *pb.ProgressBar, wg *sync.WaitGroup) {
	defer wg.Done()
	w := bufio.NewWriter(f)
	skipFields := splitFieldList(c.Config.SkipFields)
	var writeErr error

	for docI := range c.DocChan {
		if source, err := removeSourceFields(docI.Source, skipFields); err != nil {
			log.Error(err)
		} else {
			docI.Source = source
		}

		jsr, err := json.Marshal(docI)
		log.Trace(string(jsr))
		if err != nil {
			log.Error(err)
			continue
		}
		jsr = append(jsr, '\n')
		// bufio.Writer keeps the first error, the following writes fail too
		if _, writeErr = w.Write(jsr); writeErr != nil {
			break
		}
		pb.Increment()
	}
	if writeErr == nil {
		writeErr = w.Flush()
	}
	if writeErr != nil {
		log.Error("can not write output file ", c.Config.DumpOutFile, ": ", writeErr)
		atomic.StoreInt32(&c.Stats.WriteFailed, 1)
		// drain the docs, the readers block on a full channel otherwise
		for range c.DocChan {
		}
	}
	log.Debug("file dump finished")
}
