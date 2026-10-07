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
	"github.com/cheggaaa/pb"
	log "github.com/cihub/seelog"
	"io"
	"os"
	"sync"
)

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func (m *Migrator) NewFileReadWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {
	log.Debug("start reading file")
	f, err := os.Open(m.Config.DumpInputFile)
	if err != nil {
		log.Error(err)
		return
	}

	defer f.Close()
	r := bufio.NewReader(f)
	lineCount := 0
	for {
		line, err := r.ReadString('\n')
		if io.EOF == err || nil != err {
			break
		}
		lineCount += 1
		js := Document{}

		err = DecodeJson(line, &js)
		if err != nil {
			log.Error(err)
			continue
		}
		m.DocChan <- js
		pb.Increment()
	}

	defer f.Close()
	log.Debug("end reading file")
	close(m.DocChan)
	wg.Done()
}

func (c *Migrator) NewFileDumpWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {
	var f *os.File
	var err1 error

	if checkFileIsExist(c.Config.DumpOutFile) {
		flag := os.O_WRONLY
		if c.Config.TruncateOutFile {
			flag |= os.O_TRUNC
		} else {
			flag |= os.O_APPEND
		}
		f, err1 = os.OpenFile(c.Config.DumpOutFile, flag, os.ModeAppend)
		if err1 != nil {
			log.Error(err1)
			return
		}

	} else {
		f, err1 = os.Create(c.Config.DumpOutFile)
		if err1 != nil {
			log.Error(err1)
			return
		}
	}

	w := bufio.NewWriter(f)
	skipFields := splitFieldList(c.Config.SkipFields)

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
		w.Write(jsr)
		if err := w.WriteByte('\n'); err != nil {
			log.Error(err)
		}
		pb.Increment()
	}

	w.Flush()
	f.Close()

	wg.Done()
	log.Debug("file dump finished")
}
