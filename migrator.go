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
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/InPoint-Cloud/esm/internal/log"
	"github.com/cheggaaa/pb"
	//"github.com/google/go-cmp/cmp"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BulkOperation uint8

const (
	opIndex BulkOperation = iota
	opDelete
)

func (op BulkOperation) String() string {
	switch op {
	case opIndex:
		return "opIndex"
	case opDelete:
		return "opDelete"
	default:
		return fmt.Sprintf("unknown:%d", op)
	}
}

func (m *Migrator) recoveryIndexSettings(sourceIndexRefreshSettings map[string]interface{}) {
	//update replica and refresh_interval
	for name, settings := range sourceIndexRefreshSettings {
		tempIndexSettings := getEmptyIndexSettings()
		index := tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})
		for key, value := range settings.(map[string]interface{}) {
			// a nil refresh_interval resets it to the default
			if value != nil || key == "refresh_interval" {
				index[key] = value
			}
		}
		if err := m.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings); err != nil {
			log.Errorf("can not restore settings %v of index %s: %v", index, name, err)
			m.Stats.MarkSetupFailed()
		}
		if m.Config.Refresh {
			m.TargetESAPI.Refresh(name)
		}
	}
}

func (m *Migrator) ClusterVersion(host string, auth *Auth, proxy string) (*ClusterVersion, []error) {

	url := host
	resp, body, errs := Get(url, auth, proxy)

	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return nil, errs
	}

	log.Debug(body)

	version := &ClusterVersion{}
	err := json.Unmarshal([]byte(body), version)

	if err != nil {
		log.Error(body, errs)
		return nil, []error{err}
	}

	// e.g. a 401 json error body from a secured cluster
	if len(version.Version.Number) == 0 {
		log.Errorf("can not get elasticsearch version from %s, check the url and the credentials, response: %s", host, body)
		return nil, []error{fmt.Errorf("can not get elasticsearch version from %s", host)}
	}
	return version, nil
}

func (m *Migrator) ParseEsApi(isSource bool, host string, authStr string, apiKey string, proxy string) ESAPI {
	var auth *Auth = nil
	if len(apiKey) > 0 {
		auth = &Auth{ApiKey: apiKey}
	} else if len(authStr) > 0 && strings.Contains(authStr, ":") {
		authArray := strings.SplitN(authStr, ":", 2)
		auth = &Auth{User: authArray[0], Pass: authArray[1]}
	}
	if auth != nil {
		if isSource {
			m.SourceAuth = auth
		} else {
			m.TargetAuth = auth
		}
	}

	esVersion, errs := m.ClusterVersion(host, auth, proxy)
	if errs != nil {
		return nil
	}

	esInfo := "dest"
	if isSource {
		esInfo = "source"
	}

	log.Infof("%s es version: %s", esInfo, esVersion.Version.Number)
	if majorVersion(esVersion) >= 8 {
		log.Debug("es is v8+,", esVersion.Version.Number)
		api := new(ESAPIV8)
		api.Host = host
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
	} else if strings.HasPrefix(esVersion.Version.Number, "7.") {
		log.Debug("es is V7,", esVersion.Version.Number)
		api := new(ESAPIV7)
		api.Host = host
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else if strings.HasPrefix(esVersion.Version.Number, "6.") {
		log.Debug("es is V6,", esVersion.Version.Number)
		api := new(ESAPIV6)
		api.Host = host
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else if strings.HasPrefix(esVersion.Version.Number, "5.") {
		log.Debug("es is V5,", esVersion.Version.Number)
		api := new(ESAPIV5)
		api.Host = host
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else {
		log.Debug("es is not V5,", esVersion.Version.Number)
		api := new(ESAPIV0)
		api.Host = host
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
	}
}

// targetIsTypeless returns true when the target cluster (8.x and above) does not support mapping types
func (m *Migrator) targetIsTypeless() bool {
	return m.TargetESAPI != nil && majorVersion(m.TargetESAPI.ClusterVersion()) >= 8
}

func (m *Migrator) ClusterReady(api ESAPI) (*ClusterHealth, bool) {
	health := api.ClusterHealth()

	if !m.Config.WaitForGreen {
		return health, true
	}

	if health.Status == "red" {
		return health, false
	}

	if !m.Config.WaitForGreen && health.Status == "yellow" {
		return health, true
	}

	if health.Status == "green" {
		return health, true
	}

	return health, false
}

func (m *Migrator) NewBulkWorker(pb *pb.ProgressBar, wg *sync.WaitGroup) {

	log.Debug("start es bulk worker")

	// bulk items of the current request, each one is the action line plus the source line
	var items [][]byte
	itemsSize := 0
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	idleDuration := 5 * time.Second
	idleTimeout := time.NewTimer(idleDuration)
	defer idleTimeout.Stop()

	taskTimeOutDuration := 5 * time.Minute
	taskTimeout := time.NewTimer(taskTimeOutDuration)
	defer taskTimeout.Stop()

	haveTypeField := !m.targetIsTypeless()
	// 7.x accepts documents without a type, older versions require one
	targetNeedsType := haveTypeField && majorVersion(m.TargetESAPI.ClusterVersion()) < 7
	skipFields := splitFieldList(m.Config.SkipFields)

	flush := func() {
		log.Trace("clean buffer, and execute bulk insert")
		pb.Add(m.flushBulk(m.TargetESAPI, items))
		items = nil
		itemsSize = 0
	}

READ_DOCS:
	for {
		idleTimeout.Reset(idleDuration)
		taskTimeout.Reset(taskTimeOutDuration)
		select {
		case src, open := <-m.DocChan:
			// if channel is closed flush and gtfo
			if !open {
				goto WORKER_DONE
			}

			// system indices can not be written to on 8.x+
			if !haveTypeField && strings.HasPrefix(src.Index, ".") && !m.Config.CopyAllIndexes && m.Config.TargetIndexName == "" {
				log.Debugf("skip document %s of system index %s", src.Id, src.Index)
				continue
			}

			doc := Document{
				Index:   src.Index,
				Id:      src.Id,
				Routing: src.Routing,
			}
			if m.Config.TargetIndexName != "" {
				doc.Index = m.Config.TargetIndexName
			}
			if haveTypeField {
				doc.Type = src.Type
				if m.Config.OverrideTypeName != "" {
					doc.Type = m.Config.OverrideTypeName
				}
			}
			if m.Config.RegenerateID {
				doc.Id = ""
			}

			// sanity check
			if len(doc.Index) == 0 || len(doc.Type) == 0 && targetNeedsType {
				log.Errorf("failed decoding document: %+v", doc)
				continue
			}

			// encode the doc and and the _source field for a bulk request
			post := map[string]Document{
				"index": doc,
			}
			if err := docEnc.Encode(post); err != nil {
				log.Error(err)
				docBuf.Reset()
				continue
			}
			source, err := removeSourceFields(src.Source, skipFields)
			if err != nil {
				log.Error(err)
				docBuf.Reset()
				continue
			}
			docBuf.Write(source)
			docBuf.WriteByte('\n')
			item := make([]byte, docBuf.Len())
			copy(item, docBuf.Bytes())
			docBuf.Reset()
			items = append(items, item)
			itemsSize += len(item)

			// if we approach the es bulk size limit, flush to es
			if itemsSize > (m.Config.BulkSizeInMB * 1024 * 1024) {
				flush()
				if m.Config.SleepSecondsAfterEachBulk > 0 {
					time.Sleep(time.Duration(m.Config.SleepSecondsAfterEachBulk) * time.Second)
				}
			}

		case <-idleTimeout.C:
			log.Debug("5s no message input")
			flush()
		case <-taskTimeout.C:
			log.Warn("5m no message input, close worker")
			goto WORKER_DONE
		}
		goto READ_DOCS
	}
WORKER_DONE:
	flush()
	log.Trace("bulk insert")
	wg.Done()
}

func (m *Migrator) bulkRecords(bulkOp BulkOperation, dstEsApi ESAPI, targetIndex string, targetType string, diffDocMaps map[string]json.RawMessage) error {
	var items [][]byte
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	haveTypeField := majorVersion(dstEsApi.ClusterVersion()) < 8

	for docId, docData := range diffDocMaps {
		log.Debugf("now will bulk %s docId=%s, docData=%+v", bulkOp, docId, docData)
		var strOperation string
		doc := Document{
			Index: targetIndex,
			Id:    docId,
		}
		if haveTypeField {
			doc.Type = targetType
		}
		switch bulkOp {
		case opIndex:
			strOperation = "index"
		case opDelete:
			strOperation = "delete"
			//do nothing
		}

		// encode the doc and and the _source field for a bulk request
		post := map[string]Document{
			strOperation: doc,
		}
		if err := docEnc.Encode(post); err != nil {
			return err
		}
		if bulkOp == opIndex {
			_, _ = docBuf.Write(docData)
			if docData[len(docData)-1] != byte('\n') {
				_, _ = docBuf.Write([]byte{'\n'})
			}
		}
		item := make([]byte, docBuf.Len())
		copy(item, docBuf.Bytes())
		docBuf.Reset()
		items = append(items, item)
	}

	m.flushBulk(dstEsApi, items)
	return nil
}

func showDocs(message string, docs map[string]json.RawMessage) {
	count := 0
	for k := range docs {
		count++
		if count > 50 {
			break
		}
		log.Infof("%s %s", message, k)
	}
}

func (m *Migrator) SyncBetweenIndex(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	// _id => value
	srcDocMaps := make(map[string]json.RawMessage)
	dstDocMaps := make(map[string]json.RawMessage)
	diffDocMaps := make(map[string]json.RawMessage)
	newDocMaps := make(map[string]json.RawMessage)

	srcRecordIndex := 0
	dstRecordIndex := 0
	var err error
	srcType := ""
	dstType := ""
	var srcScroll ScrollAPI = nil
	var dstScroll ScrollAPI = nil
	lastSrcId := ""
	lastDestId := ""
	needScrollSrc := true
	needScrollDest := true

	addCount := 0
	updateCount := 0
	deleteCount := 0

	//TODO: 进度计算,分为 [ scroll src/dst + index ] => delete 几个部分
	srcBar := pb.New(1).Prefix("Progress")
	//srcBar := pb.New(1).Prefix("Source")
	//dstBar := pb.New(100).Prefix("Dest")
	//pool, err := pb.StartPool(srcBar, dstBar)

	for {
		if srcScroll == nil {
			srcScroll, err = srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)
			if err != nil {
				log.Errorf("can not scroll for source index: %s, reason:%s", cfg.SourceIndexNames, err.Error())
				atomic.StoreInt32(&m.Stats.ReadFailed, 1)
				return
			}
			log.Infof("src total count=%d", srcScroll.GetHitsTotal())
			srcBar.Total = int64(srcScroll.GetHitsTotal())
			srcBar.Start()
		} else if needScrollSrc {
			start := time.Now()
			log.Debugf("source index: %s next scroll, source id: %s", cfg.SourceIndexNames, srcScroll.GetScrollId())
			srcScroll, err = m.nextScroll(srcEsApi, cfg.ScrollTime, srcScroll.GetScrollId())
			if err != nil {
				return
			}
			if cfg.Dry {
				elapsed := time.Since(start)
				fmt.Printf("src scroll : %s\n", elapsed)
			}
		}

		if dstScroll == nil {
			dstScroll, err = dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)
			if err != nil {
				if strings.Contains(err.Error(), "indices.id_field_data.enabled") {
					log.Errorf("can not scroll dest index %s sorted by %s, please enable the cluster setting "+
						"indices.id_field_data.enabled on the target, reason:%s", cfg.TargetIndexName, cfg.SortField, err.Error())
					atomic.StoreInt32(&m.Stats.ReadFailed, 1)
					return
				}
				log.Errorf("can not scroll for dest index: %s, , reason:%s", cfg.TargetIndexName, err.Error())
				atomic.StoreInt32(&m.Stats.ReadFailed, 1)
				return
			} else {
				//有 dest index,
				//dstBar.Total = int64(dstScroll.GetHitsTotal()) // pb.New(dstScroll.GetHitsTotal()).Prefix("Dest")
			}
			//dstBar.Start()
			log.Infof("dst total count=%d", dstScroll.GetHitsTotal())
		} else if needScrollDest {
			start := time.Now()
			log.Debugf("source index: %s next scroll, source id: %s", cfg.TargetIndexName, dstScroll.GetScrollId())
			dstScroll, err = m.nextScroll(dstEsApi, cfg.ScrollTime, dstScroll.GetScrollId())
			if err != nil {
				return
			}
			if cfg.Dry {
				elapsed := time.Since(start)
				fmt.Printf("dest scroll : %s\n", elapsed)
			}
		}

		//从目标 index 中查询,并放入 destMap, 如果没有则是空
		if needScrollDest {
			start := time.Now()
			for idx, dstDoc := range dstScroll.GetDocs() {
				destId := dstDoc.Id
				if dstDoc.Type != "" {
					dstType = dstDoc.Type
				}
				dstSource := dstDoc.Source
				lastDestId = destId
				log.Debugf("dst [%d]: dstId=%s", dstRecordIndex+idx, destId)

				if srcSource, found := srcDocMaps[destId]; found {
					delete(srcDocMaps, destId)

					//如果从 src 的 map 中找到匹配地项
					if !cfg.IgnoreContentCompare && !bytes.Equal(srcSource, dstSource) {
						//不相等, 则需要更新
						diffDocMaps[destId] = srcSource
						if cfg.Dry {
							//diff := cmp.Diff(srcSource, dstSource)
							//log.Infof("%s diff: %s", destId, diff)
						}

					} else {
						//完全相等, 则不需要处理
					}
				} else {
					// 没有从 src 的 map 中找到匹配地项, 先放入 dstDocMaps 中等待后续对比
					dstDocMaps[destId] = dstSource
				}
				//dstBar.Increment()
			}
			dstRecordIndex += len(dstScroll.GetDocs())
			if cfg.Dry {
				elapsed := time.Since(start)
				fmt.Printf("dest compare : %s\n", elapsed)
			}
		}

		//先将 src 的当前批次查出并放入 map
		if needScrollSrc {
			start := time.Now()
			for idx, srcDoc := range srcScroll.GetDocs() {
				srcId := srcDoc.Id
				srcSource := srcDoc.Source
				if srcDoc.Type != "" {
					srcType = srcDoc.Type
				}
				lastSrcId = srcId
				log.Debugf("src [%d]: srcId=%s", srcRecordIndex+idx, srcId)

				if len(lastDestId) == 0 {
					//没有 destId, 表示 目标 index 中没有数据, 直接全部更新
					newDocMaps[srcId] = srcSource
				} else if dstSource, ok := dstDocMaps[srcId]; ok { //能从 dstDocMaps 中找到相同ID的数据
					if !cfg.IgnoreContentCompare && !bytes.Equal(srcSource, dstSource) {
						//不完全相同,需要更新,否则忽略
						diffDocMaps[srcId] = srcSource
					}
					//从 dst 中删除相同的
					delete(dstDocMaps, srcId)
				} else {
					//找不到相同的 id, 可能是 dst 还没找到, 或者 dst 中不存在
					if srcId < lastDestId {
						//dest 已经超过当前的 srcId, 表示 dst 中不存在
						newDocMaps[srcId] = srcSource
					} else {
						// dest 可能还没有遍历到, 先放入 srcDocMaps 中等待后续对比
						srcDocMaps[srcId] = srcSource
					}
				}
				srcBar.Increment()
			}
			srcRecordIndex += len(srcScroll.GetDocs())
			if cfg.Dry {
				elapsed := time.Since(start)
				fmt.Printf("src compare : %s\n", elapsed)
			}
		}

		if len(diffDocMaps) > 0 {
			updateCount += len(diffDocMaps)
			log.Debugf("now will bulk update %d records", len(diffDocMaps))
			if !cfg.Dry {
				_ = m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, diffDocMaps)
			} else {
				showDocs("diff", diffDocMaps)
			}
			diffDocMaps = make(map[string]json.RawMessage)
		}
		if len(newDocMaps) > 0 {
			addCount += len(newDocMaps)
			log.Debugf("now will bulk index %d records", len(diffDocMaps))
			if !cfg.Dry {
				_ = m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, newDocMaps)
			} else {
				showDocs("new", newDocMaps)
			}
			newDocMaps = make(map[string]json.RawMessage)
		}

		if len(srcDocMaps) > 0 && lastSrcId < lastDestId {
			// dst 已经中已经没有更多的记录, 可以直接将所有的 src 都同步到 dst 中了,避免其中保存太多
			addCount += len(srcDocMaps)
			if !cfg.Dry {
				_ = m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, srcDocMaps)
			} else {
				showDocs("insert", srcDocMaps)
			}
			srcDocMaps = make(map[string]json.RawMessage)
		}

		if len(dstDocMaps) > 0 && lastSrcId > lastDestId {
			//dstDocMaps 中还有记录,而且当前已经检测过所有的 src 记录, 说明这些 dst 记录是多余的,需要删除
			deleteCount += len(dstDocMaps)
			if !cfg.Dry && cfg.EnableDelete {
				_ = m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, dstType, dstDocMaps)
			}
			if cfg.Dry {
				showDocs("delete", dstDocMaps)
			}
			dstDocMaps = make(map[string]json.RawMessage)
		}

		if lastSrcId == lastDestId {
			needScrollSrc = true
			needScrollDest = true
		} else if len(lastDestId) == 0 || lastSrcId < lastDestId {
			//len(lastDestId) == 0 表示dest数据为空
			//上一次要求遍历 dest,但遍历出空
			needScrollSrc = true
			needScrollDest = false
		} else if lastSrcId > lastDestId {
			//上一次要求遍历 src, 但遍历出空
			needScrollSrc = false
			needScrollDest = true
		}
		if needScrollSrc && len(srcScroll.GetDocs()) == 0 {
			needScrollSrc = false
		}
		if needScrollDest && len(dstScroll.GetDocs()) == 0 {
			needScrollDest = false
		}

		//如果 src 和 dst 都遍历完毕, 才退出
		log.Debugf("lastSrcId=%s, lastDestId=%s, "+
			"needScrollSrc=%t, len(srcScroll.GetDocs()=%d, "+
			"needScrollDest=%t, len(dstScroll.GetDocs())=%d",
			lastSrcId, lastDestId,
			needScrollSrc, len(srcScroll.GetDocs()),
			needScrollDest, len(dstScroll.GetDocs()))

		if (!needScrollSrc || len(srcScroll.GetDocs()) == 0) &&
			(!needScrollDest || len(dstScroll.GetDocs()) == 0) {
			log.Debugf("can not find more, will quit, and index %d, delete %d", len(srcDocMaps), len(dstDocMaps))

			if len(srcDocMaps) > 0 {
				addCount += len(srcDocMaps)
				if !cfg.Dry {
					_ = m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, srcDocMaps)
				} else {
					showDocs("insert", srcDocMaps)
				}
			}
			if len(dstDocMaps) > 0 {
				//最后在 dst 中还有遗留的,表示 dst 中多的.需要删除
				deleteCount += len(dstDocMaps)
				if !cfg.Dry && cfg.EnableDelete {
					_ = m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, dstType, dstDocMaps)
				}
				if cfg.Dry {
					showDocs("delete", dstDocMaps)
				}
			}
			break
		}

		//目标不存在 或 src 还没有查询到和 dest 一样的地方
		if cfg.SleepSecondsAfterEachBulk > 0 {
			time.Sleep(time.Duration(cfg.SleepSecondsAfterEachBulk) * time.Second)
		}
	}
	srcEsApi.DeleteScroll(srcScroll.GetScrollId())
	dstEsApi.DeleteScroll(dstScroll.GetScrollId())

	srcBar.FinishPrint("Source End")
	//dstBar.FinishPrint("Dest End")
	//pool.Stop()

	log.Infof("sync %s(%d) to %s(%d), add=%d, update=%d, delete=%d",
		cfg.SourceIndexNames, srcRecordIndex, cfg.TargetIndexName, dstRecordIndex,
		addCount, updateCount, deleteCount)

	//log.Infof("diffDocMaps=%+v", diffDocMaps)
}

func (m *Migrator) DiffCounts(srcEsApi ESAPI, dstEsApi ESAPI) {
	srcIndices, err := srcEsApi.GetIndices("")
	if err != nil {
		fmt.Printf("failed to get source indices: %v", err)
		return
	}
	dstIndices, err := dstEsApi.GetIndices("")
	if err != nil {
		fmt.Printf("failed to get destination indices: %v", err)
		return
	}
	var same []string
	var diff []string
	var miss []string
	for srcIndex, srcInfo := range *srcIndices {
		if destInfo, ok := (*dstIndices)[srcIndex]; ok {
			if srcInfo.DocsCount == destInfo.DocsCount {
				same = append(same, srcIndex)
			} else {
				s := fmt.Sprintf("index %s : source=%d, destination=%d", srcIndex, srcInfo.DocsCount, destInfo.DocsCount)
				diff = append(diff, s)
			}
		} else {
			miss = append(miss, srcIndex)
		}
	}
	fmt.Printf("==========equals===========\n")
	for _, idx := range same {
		fmt.Println(idx)
	}
	fmt.Printf("----------diff-------------\n")
	for _, idx := range diff {
		fmt.Println(idx)
	}
	fmt.Printf("----not in destination-----\n")
	for _, idx := range miss {
		fmt.Println(idx)
	}
}
