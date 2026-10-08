package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/InPoint-Cloud/esm/internal/log"
	"github.com/cheggaaa/pb"
	"github.com/mattn/go-isatty"
)

// runMigration copies the documents from the source (es or dump file) to the output (es or dump file)
func (m *Migrator) runMigration() int {
	c := m.Config

	if len(c.FailedOutputFile) > 0 {
		if err := m.Stats.OpenFailedOutput(c.FailedOutputFile); err != nil {
			log.Error("can not open failed output file: ", err)
			return 1
		}
		defer m.Stats.CloseFailedOutput()
	}
	defer m.restoreIndexSettings()

	// output at least once
	if c.RepeatOutputTimes < 1 {
		c.RepeatOutputTimes = 1
	} else {
		log.Info("source data will repeat send to target: ", c.RepeatOutputTimes, " times, the document id will be regenerated.")
	}

	showBar := isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
	for i := 0; i < c.RepeatOutputTimes; i++ {
		if c.RepeatOutputTimes > 1 {
			log.Info("repeat round: ", i+1)
		}
		if err := m.migrateRound(showBar); err != nil {
			log.Error(err)
			return 1
		}
	}

	log.Info("data migration finished.")
	m.restoreIndexSettings()
	return m.exitCode()
}

// migrateRound reads all documents once and writes them to the output
func (m *Migrator) migrateRound(showBar bool) error {
	c := m.Config

	// enough of a buffer to hold all the search results across all workers
	m.DocChan = make(chan Document, c.BufferCount)
	fetchBar := pb.New(1).Prefix("Scroll")
	outputBar := pb.New(1).Prefix("Output ")
	wg := sync.WaitGroup{}

	// start reading, the docs are buffered in DocChan while the target is prepared
	if len(c.SourceEs) > 0 {
		if err := m.startScrollReaders(&wg, fetchBar, outputBar, showBar); err != nil {
			return err
		}
	} else if len(c.DumpInputFile) > 0 {
		lineCount, err := countLines(c.DumpInputFile)
		if err != nil {
			return err
		}
		log.Trace("file line,", lineCount)
		fetchBar = pb.New(lineCount).Prefix("Read")
		outputBar = pb.New(lineCount).Prefix("Output ")
		wg.Add(1)
		go m.NewFileReadWorker(fetchBar, &wg)
	}

	var pool *pb.Pool
	if showBar {
		var err error
		if pool, err = pb.StartPool(fetchBar, outputBar); err != nil {
			log.Warn("can not show progress bars: ", err)
			showBar = false
		}
	}
	defer func() {
		if showBar {
			outputBar.Finish()
			pool.Stop()
		}
	}()

	if len(c.TargetEs) > 0 {
		if err := m.prepareTarget(); err != nil {
			return err
		}
	}
	if c.OnlyMeta {
		return nil
	}

	log.Info("start data migration..")
	if len(c.TargetEs) > 0 {
		log.Debug("start es bulk workers")
		outputBar.Prefix("Bulk")
		wg.Add(c.Workers)
		for i := 0; i < c.Workers; i++ {
			go m.NewBulkWorker(outputBar, &wg)
		}
	} else if len(c.DumpOutFile) > 0 {
		outputBar.Prefix("Write")
		wg.Add(1)
		go m.NewFileDumpWorker(outputBar, &wg)
	}
	wg.Wait()
	return nil
}

// startScrollReaders starts one reader per scroll slice of the source es, DocChan is closed when all are done
func (m *Migrator) startScrollReaders(wg *sync.WaitGroup, fetchBar, outputBar *pb.ProgressBar, showBar bool) error {
	c := m.Config
	m.SourceESAPI = m.ParseEsApi(true, c.SourceEs, c.SourceEsAuthStr, c.SourceEsApiKey, c.SourceProxy)
	if m.SourceESAPI == nil {
		return fmt.Errorf("can not connect to the source elasticsearch")
	}

	// _doc is the cheapest order, sorting by _id loads all ids into the heap of the source (fielddata),
	// which is disabled by default since 8.0. Versions before 5.x scan without sorting.
	if c.SortField == "" && majorVersion(m.SourceESAPI.ClusterVersion()) >= 5 {
		c.SortField = "_doc"
	}
	if c.ScrollSliceSize < 1 {
		c.ScrollSliceSize = 1
	}
	if c.OnlyMeta {
		c.ScrollSliceSize = 0
	}

	totalSize := 0
	// tracks only the scroll slices, so the doc chan can be closed once all of them are done
	sliceWg := sync.WaitGroup{}
	for slice := 0; slice < c.ScrollSliceSize; slice++ {
		scroll, err := m.SourceESAPI.NewScroll(c.SourceIndexNames, c.ScrollTime, c.DocBufferCount, c.Query,
			c.SortField, slice, c.ScrollSliceSize, c.Fields)
		if err != nil {
			return err
		}
		totalSize += scroll.GetHitsTotal()
		if scroll.GetDocs() == nil {
			continue
		}
		if scroll.GetHitsTotal() == 0 {
			log.Warn("can't find documents from source.")
		}

		wg.Add(1)
		sliceWg.Add(1)
		go func() {
			defer wg.Done()
			defer sliceWg.Done()
			scroll.ProcessScrollResult(m, fetchBar)
			// loop scrolling until done
			for !scroll.Next(m, fetchBar) {
			}
		}()
	}

	// all slices finished, close doc chan so the workers can drain and exit
	go func() {
		sliceWg.Wait()
		if showBar {
			fetchBar.Finish()
		}
		log.Debug("closing doc chan")
		close(m.DocChan)
	}()

	if totalSize > 0 {
		fetchBar.Total = int64(totalSize)
		outputBar.Total = int64(totalSize)
	}
	return nil
}

func countLines(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	lineCount := 0
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadString('\n')
		if err != nil && err != io.EOF {
			return lineCount, err
		}
		if strings.TrimSpace(line) != "" {
			lineCount++
		}
		if err == io.EOF {
			return lineCount, nil
		}
	}
}

// prepareTarget connects to the target es, waits for the clusters and copies index settings and mappings
func (m *Migrator) prepareTarget() error {
	c := m.Config
	m.TargetESAPI = m.ParseEsApi(false, c.TargetEs, c.TargetEsAuthStr, c.TargetEsApiKey, c.TargetProxy)
	if m.TargetESAPI == nil {
		return fmt.Errorf("can not connect to the target elasticsearch")
	}

	// mappings from 7.x+ are typeless and can be copied across major versions,
	// mappings of older versions may need to be checked manually
	if c.CopyIndexMappings && m.SourceESAPI != nil {
		srcMajor := majorVersion(m.SourceESAPI.ClusterVersion())
		dstMajor := majorVersion(m.TargetESAPI.ClusterVersion())
		if srcMajor != dstMajor && (srcMajor < 7 || dstMajor < 7) {
			log.Warn(m.SourceESAPI.ClusterVersion().Version, "=>", m.TargetESAPI.ClusterVersion().Version,
				",cross-big-version mapping migration, please confirm manually !!")
		}
	}

	m.waitForClusters()

	// index settings can only be copied from a source es, not from a dump file
	if len(c.SourceEs) == 0 {
		return nil
	}

	indexNames, indexCount, sourceIndexMappings, err := m.SourceESAPI.GetIndexMappings(c.CopyAllIndexes, c.SourceIndexNames)
	if err != nil {
		return err
	}
	log.Debugf("indexCount: %d", indexCount)
	if indexCount == 0 {
		return fmt.Errorf("index not exists,%s", c.SourceIndexNames)
	}
	//override indexnames to be copy
	c.SourceIndexNames = indexNames

	if c.CopyIndexSettings || c.CopyIndexMappings || c.ShardsCount > 0 {
		return m.copyIndexSetup(indexCount, sourceIndexMappings)
	}
	return nil
}

// clusterWaitInterval is the time between two cluster health checks, tests make it shorter
var clusterWaitInterval = 3 * time.Second

// waitForClusters blocks until source and target are ready, see ClusterReady
func (m *Migrator) waitForClusters() {
	c := m.Config
	clusters := []struct {
		url string
		api ESAPI
	}{{c.SourceEs, m.SourceESAPI}, {c.TargetEs, m.TargetESAPI}}

	for _, cluster := range clusters {
		if len(cluster.url) == 0 {
			continue
		}
		for {
			status, ready := m.ClusterReady(cluster.api)
			if ready {
				break
			}
			log.Infof("%s at %s is %s, delaying migration ", status.Name, cluster.url, status.Status)
			time.Sleep(clusterWaitInterval)
		}
	}
}

// copyIndexSetup creates (or updates) the target indexes with the settings and mappings of the source indexes.
// The refresh_interval and number_of_replicas of the target are disabled for the migration and restored afterwards.
func (m *Migrator) copyIndexSetup(indexCount int, sourceIndexMappings *Indexes) error {
	c := m.Config
	log.Info("start settings/mappings migration..")

	sourceIndexSettings, err := m.SourceESAPI.GetIndexSettings(c.SourceIndexNames)
	log.Debug("source index settings:", sourceIndexSettings)
	if err != nil {
		return err
	}

	targetIndexSettings, err := m.TargetESAPI.GetIndexSettings(c.TargetIndexName)
	if err != nil {
		//ignore target es settings error
		log.Debug(err)
	}
	log.Debug("target IndexSettings", targetIndexSettings)

	sourceIndexRefreshSettings := map[string]interface{}{}
	// registered before the indexes are created, so they are restored even if a later step fails
	m.settingsToRestore = append(m.settingsToRestore, sourceIndexRefreshSettings)

	//if there is only one index and we specify the dest indexname
	if c.SourceIndexNames != c.TargetIndexName && (len(c.TargetIndexName) > 0) && indexCount == 1 {
		log.Debugf("only one index,so we can rewrite indexname, src:%v, dest:%v ,indexCount:%d", c.SourceIndexNames, c.TargetIndexName, indexCount)
		(*sourceIndexSettings)[c.TargetIndexName] = (*sourceIndexSettings)[c.SourceIndexNames]
		delete(*sourceIndexSettings, c.SourceIndexNames)
		log.Debug(sourceIndexSettings)
		if c.CopyIndexMappings {
			(*sourceIndexMappings)[c.TargetIndexName] = (*sourceIndexMappings)[c.SourceIndexNames]
			delete(*sourceIndexMappings, c.SourceIndexNames)
		}
	}

	for name, idx := range *sourceIndexSettings {
		log.Debug("dealing with index,name:", name, ",settings:", idx)
		tempIndexSettings := getEmptyIndexSettings()

		targetIndexExist := false
		//if target index settings is exist and we don't copy settings, we use target settings
		if targetIndexSettings != nil {
			//if target es have this index and we dont copy index settings
			if val, ok := (*targetIndexSettings)[name]; ok {
				targetIndexExist = true
				tempIndexSettings = val.(map[string]interface{})
			}

			if c.RecreateIndex {
				m.TargetESAPI.DeleteIndex(name)
				targetIndexExist = false
			}
		}

		//copy index settings
		if c.CopyIndexSettings {
			tempIndexSettings = ((*sourceIndexSettings)[name]).(map[string]interface{})
			if c.CopyIndexMappings {
				tempIndexSettings["mappings"] = (*sourceIndexMappings)[name].(map[string]interface{})["mappings"]
			}
		}
		//check map elements
		if _, ok := tempIndexSettings["settings"]; !ok {
			tempIndexSettings["settings"] = map[string]interface{}{}
		}
		if _, ok := tempIndexSettings["settings"].(map[string]interface{})["index"]; !ok {
			tempIndexSettings["settings"].(map[string]interface{})["index"] = map[string]interface{}{}
		}

		//remember the settings changed during the migration, they are restored afterwards
		sourceIndex := ((*sourceIndexSettings)[name].(map[string]interface{}))["settings"].(map[string]interface{})["index"].(map[string]interface{})
		sourceIndexRefreshSettings[name] = map[string]interface{}{
			"refresh_interval":   sourceIndex["refresh_interval"],
			"number_of_replicas": sourceIndex["number_of_replicas"],
		}

		//set refresh_interval
		mapIndex := tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})
		mapIndex["refresh_interval"] = -1
		mapIndex["number_of_replicas"] = 0
		//remove routing allocation
		if _, ok := mapIndex["routing"]; ok && !c.RemainMappingRoutingAllocation {
			delete(mapIndex, "routing")
		}

		if targetIndexExist {
			//the number of shards of an existing index can not be changed
			delete(mapIndex, "number_of_shards")
			if c.ShardsCount > 0 {
				log.Warnf("index %s already exists, ignore --shards", name)
			}
			log.Debug("update index with settings,", name, tempIndexSettings)
			if err := m.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings); err != nil {
				log.Error(err)
				m.Stats.MarkSetupFailed()
			}
			continue
		}

		//override shard settings, otherwise keep the shards of the source index
		if c.ShardsCount > 0 {
			mapIndex["number_of_shards"] = c.ShardsCount
		} else if !c.CopyIndexSettings {
			delete(mapIndex, "number_of_shards")
		}
		log.Debug("create index with settings,", name, tempIndexSettings)
		if err := m.TargetESAPI.CreateIndex(name, tempIndexSettings); err != nil {
			//nothing to restore, the index does not exist
			delete(sourceIndexRefreshSettings, name)
			return err
		}
	}

	if c.CopyIndexMappings && !c.CopyIndexSettings {
		//the mappings were already renamed to the dest indexname together with the settings
		for name, mapping := range *sourceIndexMappings {
			err := m.TargetESAPI.UpdateIndexMapping(name, mapping.(map[string]interface{})["mappings"].(map[string]interface{}))
			if err != nil {
				log.Error(err)
				m.Stats.MarkSetupFailed()
			}
		}
	}

	log.Info("settings/mappings migration finished.")
	return nil
}

func (m *Migrator) restoreIndexSettings() {
	for _, settings := range m.settingsToRestore {
		m.recoveryIndexSettings(settings)
	}
	m.settingsToRestore = nil
}

// exitCode prints the summary and returns 1 if the migration failed or is incomplete
func (m *Migrator) exitCode() int {
	c := m.Config
	if c.OnlyMeta {
		return 0
	}
	if len(c.TargetEs) == 0 {
		if atomic.LoadInt32(&m.Stats.ReadFailed) > 0 {
			log.Error("reading from source failed, the output file is incomplete")
			return 1
		}
		return 0
	}
	code := 0
	m.Stats.PrintSummary()
	if m.Stats.HasFailures() {
		code = 1
	}
	if len(c.SourceEs) > 0 && !c.SkipCountCheck {
		if !m.CheckCounts() {
			log.Error("document counts of source and target differ")
			code = 1
		}
	}
	return code
}
