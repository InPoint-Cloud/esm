package main

import (
	"bufio"
	"fmt"
	log "github.com/InPoint-Cloud/esm/internal/log"
	"github.com/cheggaaa/pb"
	goflags "github.com/jessevdk/go-flags"
	"github.com/mattn/go-isatty"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run does the migration with the given command line arguments and returns the exit code
func run(args []string) int {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var err error
	c := &Config{}
	migrator := Migrator{}
	migrator.Config = c

	// parse args
	_, err = goflags.ParseArgs(c, args)
	if err != nil {
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

	if len(c.SourceEs) == 0 && len(c.DumpInputFile) == 0 {
		log.Error("no input, type --help for more details")
		return 1
	}
	if len(c.TargetEs) == 0 && len(c.DumpOutFile) == 0 {
		log.Error("no output, type --help for more details")
		return 1
	}

	if c.SourceEs == c.TargetEs && c.SourceIndexNames == c.TargetIndexName {
		log.Error("migration output is the same as the output")
		return 1
	}

	var showBar bool = false
	if isatty.IsTerminal(os.Stdout.Fd()) {
		showBar = true
	} else if isatty.IsCygwinTerminal(os.Stdout.Fd()) {
		showBar = true
	} else {
		showBar = false
	}

	if c.Sync {
		//sync 功能时,只支持一个 index:
		if len(c.SourceIndexNames) == 0 {
			log.Error("migration sync only support source 1 index to 1 target index")
			return 1
		}
		if len(c.TargetIndexName) == 0 {
			c.TargetIndexName = c.SourceIndexNames
		}
		migrator.SourceESAPI = migrator.ParseEsApi(true, c.SourceEs, c.SourceEsAuthStr, c.SourceEsApiKey, c.SourceProxy, c.Compress)
		if migrator.SourceESAPI == nil {
			log.Error("can not parse source es api")
			return 1
		}
		migrator.TargetESAPI = migrator.ParseEsApi(false, c.TargetEs, c.TargetEsAuthStr, c.TargetEsApiKey, c.TargetProxy, false)
		if migrator.TargetESAPI == nil {
			log.Error("can not parse target es api")
			return 1
		}
		migrator.SyncBetweenIndex(migrator.SourceESAPI, migrator.TargetESAPI, c)
		migrator.Stats.PrintSummary()
		if migrator.Stats.HasFailures() {
			return 1
		}
		return 0
	}

	if c.DiffCounts {
		migrator.SourceESAPI = migrator.ParseEsApi(true, c.SourceEs, c.SourceEsAuthStr, c.SourceEsApiKey, c.SourceProxy, c.Compress)
		if migrator.SourceESAPI == nil {
			log.Error("can not parse source es api")
			return 1
		}
		migrator.TargetESAPI = migrator.ParseEsApi(false, c.TargetEs, c.TargetEsAuthStr, c.TargetEsApiKey, c.TargetProxy, false)
		if migrator.TargetESAPI == nil {
			log.Error("can not parse target es api")
			return 1
		}
		migrator.DiffCounts(migrator.SourceESAPI, migrator.TargetESAPI)
		return 0
	}

	if len(c.FailedOutputFile) > 0 {
		if err := migrator.Stats.OpenFailedOutput(c.FailedOutputFile); err != nil {
			log.Error("can not open failed output file: ", err)
			return 1
		}
		defer migrator.Stats.CloseFailedOutput()
	}

	// settings changed during the migration (refresh_interval, number_of_replicas), restored when finished
	var indexSettingsToRestore []map[string]interface{}
	restoreIndexSettings := func() {
		for _, settings := range indexSettingsToRestore {
			migrator.recoveryIndexSettings(settings)
		}
		indexSettingsToRestore = nil
	}
	defer restoreIndexSettings()

	//至少输出一次
	if c.RepeatOutputTimes < 1 {
		c.RepeatOutputTimes = 1
	} else {
		log.Info("source data will repeat send to target: ", c.RepeatOutputTimes, " times, the document id will be regenerated.")
	}

	if c.RepeatOutputTimes > 0 {

		for i := 0; i < c.RepeatOutputTimes; i++ {

			if c.RepeatOutputTimes > 1 {
				log.Info("repeat round: ", i+1)
			}

			// enough of a buffer to hold all the search results across all workers
			migrator.DocChan = make(chan Document, c.BufferCount)

			//var srcESVersion *ClusterVersion
			// create a progressbar
			var outputBar *pb.ProgressBar = pb.New(1).Prefix("Output ")

			var fetchBar = pb.New(1).Prefix("Scroll")

			wg := sync.WaitGroup{}

			//dealing with input
			if len(c.SourceEs) > 0 {
				//dealing with basic auth

				migrator.SourceESAPI = migrator.ParseEsApi(true, c.SourceEs, c.SourceEsAuthStr, c.SourceEsApiKey,
					migrator.Config.SourceProxy, c.Compress)
				if migrator.SourceESAPI == nil {
					log.Error("can not parse source es api")
					return 1
				}

				// sorting on _id needs fielddata, which is disabled by default since 8.0
				if c.SortField == "_id" && majorVersion(migrator.SourceESAPI.ClusterVersion()) >= 8 {
					log.Info("source es is 8.x+, sorting by _doc instead of _id")
					c.SortField = "_doc"
				}

				if c.ScrollSliceSize < 1 {
					c.ScrollSliceSize = 1
				}
				// do read data
				if c.OnlyMeta {
					c.ScrollSliceSize = 0
				}

				totalSize := 0
				// tracks only the scroll slices, so the doc chan can be closed once all of them are done
				sliceWg := sync.WaitGroup{}
				for slice := 0; slice < c.ScrollSliceSize; slice++ {
					scroll, err := migrator.SourceESAPI.NewScroll(c.SourceIndexNames, c.ScrollTime, c.DocBufferCount, c.Query,
						c.SortField, slice, c.ScrollSliceSize, c.Fields)
					if err != nil {
						log.Error(err)
						return 1
					}

					totalSize += scroll.GetHitsTotal()

					if scroll.GetDocs() != nil {

						if scroll.GetHitsTotal() == 0 {
							log.Warn("can't find documents from source.")
							//return
						}

						wg.Add(1)
						sliceWg.Add(1)
						go func() {
							defer wg.Done()
							defer sliceWg.Done()
							//process input
							// start scroll
							scroll.ProcessScrollResult(&migrator, fetchBar)

							// loop scrolling until done
							for !scroll.Next(&migrator, fetchBar) {
							}
						}()
					}
				}

				// all slices finished, close doc chan so the bulk workers can drain and exit
				go func() {
					sliceWg.Wait()
					if showBar {
						fetchBar.Finish()
					}
					log.Debug("closing doc chan")
					close(migrator.DocChan)
				}()

				if totalSize > 0 {
					fetchBar.Total = int64(totalSize)
					outputBar.Total = int64(totalSize)
				}

			} else if len(c.DumpInputFile) > 0 {
				//read file stream
				wg.Add(1)
				f, err := os.Open(c.DumpInputFile)
				if err != nil {
					log.Error(err)
					return 1
				}
				//get file lines
				lineCount := 0
				defer f.Close()
				r := bufio.NewReader(f)
				for {
					_, err := r.ReadString('\n')
					if io.EOF == err || nil != err {
						break
					}
					lineCount += 1
				}
				log.Trace("file line,", lineCount)

				fetchBar := pb.New(lineCount).Prefix("Read")
				outputBar = pb.New(lineCount).Prefix("Output ")

				f.Close()

				go migrator.NewFileReadWorker(fetchBar, &wg)

			}

			var pool *pb.Pool
			if showBar {

				// start pool
				pool, err = pb.StartPool(fetchBar, outputBar)
				if err != nil {
					log.Warn("can not show progress bars: ", err)
					showBar = false
				}
			}

			//dealing with output
			if len(c.TargetEs) > 0 {
				//get target es api
				migrator.TargetESAPI = migrator.ParseEsApi(false, c.TargetEs, c.TargetEsAuthStr, c.TargetEsApiKey,
					migrator.Config.TargetProxy, false)
				if migrator.TargetESAPI == nil {
					log.Error("can not parse target es api")
					return 1
				}

				log.Debug("start process with mappings")
				// mappings from 7.x+ are typeless and can be copied across major versions,
				// mappings of older versions may need to be checked manually
				if c.CopyIndexMappings && migrator.SourceESAPI != nil {
					srcMajor := majorVersion(migrator.SourceESAPI.ClusterVersion())
					dstMajor := majorVersion(migrator.TargetESAPI.ClusterVersion())
					if srcMajor != dstMajor && (srcMajor < 7 || dstMajor < 7) {
						log.Warn(migrator.SourceESAPI.ClusterVersion().Version, "=>",
							migrator.TargetESAPI.ClusterVersion().Version,
							",cross-big-version mapping migration, please confirm manually !!")
						//return
					}
				}
				// wait for cluster state to be okay before moving
				idleDuration := 3 * time.Second
				timer := time.NewTimer(idleDuration)
				defer timer.Stop()
				for {
					timer.Reset(idleDuration)

					if len(c.SourceEs) > 0 {
						if status, ready := migrator.ClusterReady(migrator.SourceESAPI); !ready {
							log.Infof("%s at %s is %s, delaying migration ", status.Name, c.SourceEs, status.Status)
							<-timer.C
							continue
						}
					}

					if len(c.TargetEs) > 0 {
						if status, ready := migrator.ClusterReady(migrator.TargetESAPI); !ready {
							log.Infof("%s at %s is %s, delaying migration ", status.Name, c.TargetEs, status.Status)
							<-timer.C
							continue
						}
					}
					break
				}

				if len(c.SourceEs) > 0 {
					// get all indexes from source
					indexNames, indexCount, sourceIndexMappings, err := migrator.SourceESAPI.GetIndexMappings(c.CopyAllIndexes, c.SourceIndexNames)

					if err != nil {
						log.Error(err)
						return 1
					}

					sourceIndexRefreshSettings := map[string]interface{}{}
					// registered before the indexes are created, so they are restored even if a later step fails
					indexSettingsToRestore = append(indexSettingsToRestore, sourceIndexRefreshSettings)

					log.Debugf("indexCount: %d", indexCount)

					if indexCount > 0 {
						//override indexnames to be copy
						c.SourceIndexNames = indexNames
						// copy index settings if user asked
						if c.CopyIndexSettings || c.ShardsCount > 0 {
							log.Info("start settings/mappings migration..")
							//get source index settings
							var sourceIndexSettings *Indexes
							sourceIndexSettings, err := migrator.SourceESAPI.GetIndexSettings(c.SourceIndexNames)
							log.Debug("source index settings:", sourceIndexSettings)
							if err != nil {
								log.Error(err)
								return 1
							}

							//get target index settings
							targetIndexSettings, err := migrator.TargetESAPI.GetIndexSettings(c.TargetIndexName)
							if err != nil {
								//ignore target es settings error
								log.Debug(err)
							}
							log.Debug("target IndexSettings", targetIndexSettings)

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

							// dealing with indices settings
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
										migrator.TargetESAPI.DeleteIndex(name)
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
								mapSettings := tempIndexSettings["settings"].(map[string]interface{})
								mapIndex := mapSettings["index"].(map[string]interface{})
								mapIndex["refresh_interval"] = -1
								mapIndex["number_of_replicas"] = 0
								//remove routing allocation
								if _, ok := mapIndex["routing"]; ok && !c.RemainMappingRoutingAllocation {
									delete(mapIndex, "routing")
								}
								//copy indexsettings and mappings
								if targetIndexExist {
									//the number of shards of an existing index can not be changed
									delete(mapIndex, "number_of_shards")
									if c.ShardsCount > 0 {
										log.Warnf("index %s already exists, ignore --shards", name)
									}
									log.Debug("update index with settings,", name, tempIndexSettings)
									err := migrator.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings)
									if err != nil {
										log.Error(err)
										migrator.Stats.MarkSetupFailed()
									}
								} else {

									//override shard settings, otherwise keep the shards of the source index
									if c.ShardsCount > 0 {
										mapIndex["number_of_shards"] = c.ShardsCount
									} else if !c.CopyIndexSettings {
										delete(mapIndex, "number_of_shards")
									}

									log.Debug("create index with settings,", name, tempIndexSettings)
									err := migrator.TargetESAPI.CreateIndex(name, tempIndexSettings)
									if err != nil {
										log.Error(err)
										//nothing to restore, the index does not exist
										delete(sourceIndexRefreshSettings, name)
										return 1
									}

								}

							}

							if c.CopyIndexMappings && !c.CopyIndexSettings {
								//the mappings were already renamed to the dest indexname together with the settings
								for name, mapping := range *sourceIndexMappings {
									err := migrator.TargetESAPI.UpdateIndexMapping(name, mapping.(map[string]interface{})["mappings"].(map[string]interface{}))
									if err != nil {
										log.Error(err)
										migrator.Stats.MarkSetupFailed()
									}
								}
							}

							log.Info("settings/mappings migration finished.")
						}

					} else {
						log.Error("index not exists,", c.SourceIndexNames)
						return 1
					}

				} else if len(c.DumpInputFile) > 0 {
					//check shard settings
					//TODO support shard config
				}

			}
			if c.OnlyMeta {
				goto FIN
			}
			log.Info("start data migration..")

			//start es bulk thread
			if len(c.TargetEs) > 0 {
				log.Debug("start es bulk workers")
				outputBar.Prefix("Bulk")
				wg.Add(c.Workers)
				for i := 0; i < c.Workers; i++ {
					go migrator.NewBulkWorker(outputBar, &wg)
				}
			} else if len(c.DumpOutFile) > 0 {
				// start file write
				outputBar.Prefix("Write")
				wg.Add(1)
				go migrator.NewFileDumpWorker(outputBar, &wg)
			}

			wg.Wait()
		FIN:
			if showBar {

				outputBar.Finish()
				// close pool
				pool.Stop()

			}
		}

	}

	log.Info("data migration finished.")
	restoreIndexSettings()

	if c.OnlyMeta {
		return 0
	}
	if len(c.TargetEs) == 0 {
		if atomic.LoadInt32(&migrator.Stats.ReadFailed) > 0 {
			log.Error("reading from source failed, the output file is incomplete")
			return 1
		}
		return 0
	}
	exitCode := 0
	migrator.Stats.PrintSummary()
	if migrator.Stats.HasFailures() {
		exitCode = 1
	}
	if len(c.SourceEs) > 0 && !c.SkipCountCheck {
		if !migrator.CheckCounts() {
			log.Error("document counts of source and target differ")
			exitCode = 1
		}
	}
	return exitCode
}
