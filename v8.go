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
	"strconv"
	"strings"
)

// ESAPIV8 is used for elasticsearch 8.x and 9.x, which are typeless and
// reject a number of settings and mapping parameters still accepted by 7.x
type ESAPIV8 struct {
	ESAPIV7
}

// majorVersion returns the major version of a cluster, 0 if unknown
func majorVersion(version *ClusterVersion) int {
	if version == nil {
		return 0
	}
	major, err := strconv.Atoi(strings.SplitN(version.Version.Number, ".", 2)[0])
	if err != nil {
		return 0
	}
	return major
}

func (s *ESAPIV8) NextScroll(scrollTime string, scrollId string) (ScrollAPI, error) {
	param := make(map[string]string)
	param["scroll"] = scrollTime
	param["scroll_id"] = scrollId
	data, _ := json.Marshal(param)
	reqData := bytes.NewBuffer(data)
	url := fmt.Sprintf("%s/_search/scroll", s.Host)
	body, err := Request(s.Compress, "POST", url, s.Auth, reqData, s.HttpProxy)

	if err != nil {
		//log.Error(errs)
		return nil, err
	}
	// decode elasticsearch scroll response
	scroll := &ScrollV7{}
	err = DecodeJson(body, &scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, nil
}

func (s *ESAPIV8) DeleteScroll(scrollId string) error {
	if len(scrollId) == 0 {
		return nil
	}
	data, _ := json.Marshal(map[string]string{"scroll_id": scrollId})
	url := fmt.Sprintf("%s/_search/scroll", s.Host)
	_, err := Request(false, "DELETE", url, s.Auth, bytes.NewBuffer(data), s.HttpProxy)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (s *ESAPIV8) CreateIndex(name string, settings map[string]interface{}) error {
	sanitizeIndexSettingsV8(settings)
	if mappings, ok := settings["mappings"].(map[string]interface{}); ok {
		sanitizeMappingsV8(mappings)
	}
	return s.ESAPIV7.CreateIndex(name, settings)
}

func (s *ESAPIV8) UpdateIndexSettings(name string, settings map[string]interface{}) error {
	sanitizeIndexSettingsV8(settings)
	return s.ESAPIV7.UpdateIndexSettings(name, settings)
}

func (s *ESAPIV8) UpdateIndexMapping(indexName string, mappings map[string]interface{}) error {
	// unlike v7, dynamic_templates are kept
	sanitizeMappingsV8(mappings)

	log.Debug("start update mapping: ", indexName, ", ", mappings)

	url := fmt.Sprintf("%s/%s/_mapping", s.Host, indexName)

	body := bytes.Buffer{}
	enc := json.NewEncoder(&body)
	enc.Encode(mappings)
	res, err := Request(s.Compress, "PUT", url, s.Auth, &body, s.HttpProxy)
	if err != nil {
		log.Error(url)
		log.Error(body.String())
		log.Error(err, res)
		return err
	}
	return nil
}

// index settings that are removed in 8.x/9.x, or private/internal and rejected on index creation
var removedIndexSettingsV8 = [][]string{
	{"mapper", "dynamic"},
	{"max_adjacency_matrix_filters"},
	{"force_memory_term_dictionary"},
	{"soft_deletes", "enabled"},
	{"translog", "retention"},
	{"frozen"},
	{"search", "throttled"},
	{"verified_before_close"},
	{"resize"},
	{"shrink"},
	{"routing", "allocation", "initial_recovery"},
}

// deleteSettingPath deletes a nested key, in both nested and flattened ("a.b") form,
// and drops parent objects left empty
func deleteSettingPath(m map[string]interface{}, path []string) {
	delete(m, strings.Join(path, "."))
	if len(path) == 1 {
		delete(m, path[0])
		return
	}
	child, ok := m[path[0]].(map[string]interface{})
	if !ok {
		return
	}
	deleteSettingPath(child, path[1:])
	if len(child) == 0 {
		delete(m, path[0])
	}
}

func sanitizeIndexSettingsV8(settings map[string]interface{}) {
	set, ok := settings["settings"].(map[string]interface{})
	if !ok {
		return
	}
	index, ok := set["index"].(map[string]interface{})
	if !ok {
		return
	}

	for _, path := range removedIndexSettingsV8 {
		deleteSettingPath(index, path)
	}

	analysis, ok := index["analysis"].(map[string]interface{})
	if !ok {
		return
	}
	renamed := map[string]string{"nGram": "ngram", "edgeNGram": "edge_ngram"}
	for _, section := range []string{"filter", "tokenizer"} {
		items, ok := analysis[section].(map[string]interface{})
		if !ok {
			continue
		}
		for name, item := range items {
			def, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if t, ok := def["type"].(string); ok {
				if newType, found := renamed[t]; found {
					log.Infof("analysis %s [%s]: renaming type %s to %s", section, name, t, newType)
					def["type"] = newType
				}
			}
		}
	}
}

func sanitizeMappingsV8(mappings map[string]interface{}) {
	if fieldNames, ok := mappings["_field_names"].(map[string]interface{}); ok {
		delete(fieldNames, "enabled")
		if len(fieldNames) == 0 {
			delete(mappings, "_field_names")
		}
	}

	removeBoost(mappings)

	if templates, ok := mappings["dynamic_templates"].([]interface{}); ok {
		for _, template := range templates {
			named, ok := template.(map[string]interface{})
			if !ok {
				continue
			}
			for _, t := range named {
				if def, ok := t.(map[string]interface{}); ok {
					if mapping, ok := def["mapping"].(map[string]interface{}); ok {
						delete(mapping, "boost")
					}
				}
			}
		}
	}
}

// removeBoost removes the mapping parameter "boost" (rejected since 8.0) from
// every field in properties, including object fields and multi-fields
func removeBoost(mapping map[string]interface{}) {
	for _, key := range []string{"properties", "fields"} {
		fields, ok := mapping[key].(map[string]interface{})
		if !ok {
			continue
		}
		for _, field := range fields {
			if def, ok := field.(map[string]interface{}); ok {
				delete(def, "boost")
				removeBoost(def)
			}
		}
	}
}
