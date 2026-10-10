// SPDX-License-Identifier: Apache-2.0
// See LICENSE.md in the repository root for license details.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	log "github.com/InPoint-Cloud/esm/internal/log"
)

const (
	analysePageBytes        = 10 << 20  // aimed size of a scroll page
	analyseBufferBytes      = 1 << 30   // aimed memory of the buffered documents
	analyseLargeDocBytes    = 1 << 20   // documents from this size on are large, fewer workers
	analyseMaxShardBytes    = 50 << 30  // primary shards above this size are too large
	analyseShardBytes       = 30 << 30  // aimed size of a primary shard with --shards
	analyseManyDocs         = 100000    // indexes from this size on profit from sliced scrolls and more workers
	analyseMaxSlices        = 8         // more slices than this rarely help and load the source
	analyseMaxWorkers       = 16        // more bulk workers than this rarely help
	analyseMaxDiffLines     = 30        // differences listed per section, the rest is counted
	defaultMaxContentLength = 100 << 20 // http.max_content_length of elasticsearch
	defaultDocBufferCount   = 10000     // default of -c
	defaultBufferCount      = 1000000   // default of --buffer_count
)

// clusterAnalysis is what --analyse reads from a source or target cluster
type clusterAnalysis struct {
	role    string // source or target
	url     string
	auth    *Auth
	proxy   string
	version string
	major   int

	dataNodes         int
	maxContentLength  int64 // http.max_content_length in bytes
	maxContentDefault bool  // maxContentLength is the default, not read from the cluster
	idFieldData       bool  // sorting on _id is allowed, needed by --sync

	indices map[string]*indexAnalysis
}

type indexAnalysis struct {
	name     string
	closed   bool
	docs     int64
	priBytes int64 // size of the primary shards on disk
	shards   int
	replicas int

	settings map[string]string // flattened settings, ie: index.number_of_shards
	types    []string          // mapping types of 6.x and older
	meta     map[string]string // mapping parameters other than fields, ie: _source, dynamic
	fields   map[string]string // field path => its mapping without sub fields, multi-fields are path.name
	conflict []string          // fields mapped differently in two mapping types
}

// avgDocBytes is the average size of a document on disk, 0 if unknown
func (idx *indexAnalysis) avgDocBytes() int64 {
	if idx == nil || idx.docs == 0 {
		return 0
	}
	return idx.priBytes / idx.docs
}

// runAnalyse reads the settings and mappings of source and target without migrating,
// prints their differences and suggests flags and elasticsearch settings for the migration
func (m *Migrator) runAnalyse() int {
	c := m.Config
	if len(c.SourceEs) == 0 {
		log.Error("--analyse needs a source elasticsearch (-s)")
		return 1
	}
	m.SourceESAPI = m.ParseEsApi(true, c.SourceEs, c.SourceEsAuthStr, c.SourceEsApiKey, c.SourceProxy)
	if m.SourceESAPI == nil {
		log.Error("can not connect to the source elasticsearch")
		return 1
	}
	src := newClusterAnalysis("source", c.SourceEs, m.SourceAuth, c.SourceProxy, m.SourceESAPI.ClusterVersion())
	pattern := c.SourceIndexNames
	wildcard := pattern == "" || pattern == "_all" || strings.ContainsAny(pattern, "*?")
	err := src.loadIndices(pattern, func(name string) bool {
		// like a migration, system indexes are only included with -a or when they are named
		return c.CopyAllIndexes || !wildcard || !strings.HasPrefix(name, ".")
	})
	if err != nil {
		log.Error("can not read the source indexes: ", err)
		return 1
	}
	if len(src.indices) == 0 {
		log.Errorf("no source index matches %s", c.SourceIndexNames)
		return 1
	}

	var dst *clusterAnalysis
	if len(c.TargetEs) > 0 {
		m.TargetESAPI = m.ParseEsApi(false, c.TargetEs, c.TargetEsAuthStr, c.TargetEsApiKey, c.TargetProxy)
		if m.TargetESAPI == nil {
			log.Error("can not connect to the target elasticsearch")
			return 1
		}
		dst = newClusterAnalysis("target", c.TargetEs, m.TargetAuth, c.TargetProxy, m.TargetESAPI.ClusterVersion())
		wanted := map[string]bool{}
		for _, name := range src.names() {
			wanted[targetIndexName(c, name)] = true
		}
		if err := dst.loadIndices("", func(name string) bool { return wanted[name] }); err != nil {
			log.Error("can not read the target indexes: ", err)
			return 1
		}
	}

	// 2 tells scripts that the analysis found problems that make the migration fail
	if writeAnalysis(os.Stdout, c, src, dst).hasErrors() {
		log.Error("the analysis found errors, the migration would fail")
		return 2
	}
	return 0
}

func targetIndexName(c *Config, source string) string {
	if c.TargetIndexName != "" {
		return c.TargetIndexName
	}
	return source
}

func newClusterAnalysis(role, url string, auth *Auth, proxy string, version *ClusterVersion) *clusterAnalysis {
	ca := &clusterAnalysis{role: role, url: strings.TrimRight(url, "/"), auth: auth, proxy: proxy,
		version: version.Version.Number, major: majorVersion(version), indices: map[string]*indexAnalysis{}}
	ca.loadClusterSettings()
	return ca
}

// get decodes the json response of a GET request, it returns the status code
func (ca *clusterAnalysis) get(path string, out interface{}) (int, error) {
	resp, body, errs := Get(ca.url+path, ca.auth, ca.proxy)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if errs != nil {
		return 0, errs[0]
	}
	if resp.StatusCode != 200 {
		if len(body) > 300 {
			body = body[:300]
		}
		return resp.StatusCode, fmt.Errorf("GET %s: %d %s", path, resp.StatusCode, body)
	}
	d := json.NewDecoder(strings.NewReader(body))
	d.UseNumber()
	return resp.StatusCode, d.Decode(out)
}

// loadClusterSettings reads the number of data nodes, http.max_content_length and indices.id_field_data.enabled.
// Older versions do not have all of the apis, the defaults of elasticsearch are assumed then.
func (ca *clusterAnalysis) loadClusterSettings() {
	var health struct {
		DataNodes int `json:"number_of_data_nodes"`
	}
	if _, err := ca.get("/_cluster/health", &health); err != nil {
		log.Debug(err)
	}
	ca.dataNodes = health.DataNodes

	// transient overrides persistent, which overrides the defaults
	cluster := map[string]string{}
	var cs map[string]interface{}
	if _, err := ca.get("/_cluster/settings?include_defaults=true&flat_settings=true", &cs); err != nil {
		log.Debug(err)
	}
	for _, scope := range []string{"defaults", "persistent", "transient"} {
		flattenSettings("", cs[scope], cluster)
	}

	// sorting on _id is disabled by default since 8.0
	ca.idFieldData = ca.major < 8
	if v, ok := cluster["indices.id_field_data.enabled"]; ok {
		ca.idFieldData = v == "true"
	}

	// http.max_content_length is a node setting, the smallest one of all nodes limits the bulk requests
	var nodes struct {
		Nodes map[string]struct {
			Settings map[string]interface{} `json:"settings"`
		} `json:"nodes"`
	}
	if _, err := ca.get("/_nodes/settings", &nodes); err != nil {
		log.Debug(err)
	}
	for _, node := range nodes.Nodes {
		flat := map[string]string{}
		flattenSettings("", node.Settings, flat)
		if size, ok := parseByteSize(flat["http.max_content_length"]); ok && (ca.maxContentLength == 0 || size < ca.maxContentLength) {
			ca.maxContentLength = size
		}
	}
	if ca.maxContentLength == 0 {
		if size, ok := parseByteSize(cluster["http.max_content_length"]); ok {
			ca.maxContentLength = size
		}
	}
	if ca.maxContentLength == 0 {
		ca.maxContentLength, ca.maxContentDefault = defaultMaxContentLength, true
	}
}

// loadIndices reads the indexes matching pattern ("" or _all for all of them) for which keep returns true,
// with their sizes, settings and mappings
func (ca *clusterAnalysis) loadIndices(pattern string, keep func(name string) bool) error {
	path := "/_cat/indices"
	if pattern != "" && pattern != "_all" {
		path += "/" + pattern
	}
	var rows []map[string]interface{}
	status, err := ca.get(path+"?format=json&bytes=b&h=index,status,pri,rep,docs.count,pri.store.size", &rows)
	if status == 404 {
		return nil
	}
	if err != nil {
		// very old versions have no json _cat api, the index names are taken from the settings
		log.Debug(err)
		var settings map[string]interface{}
		if pattern == "" {
			pattern = "_all"
		}
		status, err := ca.get("/"+pattern+"/_settings", &settings)
		if status == 404 {
			return nil
		}
		if err != nil {
			return err
		}
		rows = nil
		for name := range settings {
			rows = append(rows, map[string]interface{}{"index": name})
		}
	}

	for _, row := range rows {
		name := fmt.Sprint(row["index"])
		if !keep(name) {
			continue
		}
		idx := &indexAnalysis{name: name, closed: row["status"] == "close", settings: map[string]string{},
			meta: map[string]string{}, fields: map[string]string{}}
		idx.docs, _ = strconv.ParseInt(fmt.Sprint(row["docs.count"]), 10, 64)
		idx.priBytes, _ = strconv.ParseInt(fmt.Sprint(row["pri.store.size"]), 10, 64)
		idx.shards, _ = strconv.Atoi(fmt.Sprint(row["pri"]))
		idx.replicas, _ = strconv.Atoi(fmt.Sprint(row["rep"]))
		ca.indices[name] = idx
	}

	// in chunks, so the urls do not get too long
	names := ca.names()
	for len(names) > 0 {
		n := min(len(names), 50)
		chunk := strings.Join(names[:n], ",")
		names = names[n:]

		var settings map[string]struct {
			Settings map[string]interface{} `json:"settings"`
		}
		if _, err := ca.get("/"+chunk+"/_settings", &settings); err != nil {
			return err
		}
		for name, s := range settings {
			if idx, ok := ca.indices[name]; ok {
				flattenSettings("", s.Settings, idx.settings)
				// 1.x and 2.x do not report the shards in _cat with format=json
				if idx.shards == 0 {
					idx.shards, _ = strconv.Atoi(idx.settings["index.number_of_shards"])
					idx.replicas, _ = strconv.Atoi(idx.settings["index.number_of_replicas"])
				}
			}
		}

		var mappings map[string]map[string]interface{}
		if _, err := ca.get("/"+chunk+"/_mapping", &mappings); err != nil {
			return err
		}
		for name, m := range mappings {
			if idx, ok := ca.indices[name]; ok {
				mapping, _ := m["mappings"].(map[string]interface{})
				idx.parseMappings(mapping)
			}
		}
	}
	return nil
}

func (ca *clusterAnalysis) names() []string {
	var names []string
	for name := range ca.indices {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// flattenSettings adds the settings to out with dotted keys, nested and flat settings give the same keys
func flattenSettings(prefix string, v interface{}, out map[string]string) {
	switch t := v.(type) {
	case nil:
	case map[string]interface{}:
		for k, child := range t {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			flattenSettings(key, child, out)
		}
	case string:
		out[prefix] = t
	default:
		b, _ := json.Marshal(t)
		out[prefix] = string(b)
	}
}

// parseByteSize parses an elasticsearch size, ie: 100mb, 1gb or 104857600
func parseByteSize(s string) (int64, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return 0, false
	}
	units := []struct {
		suffix string
		factor float64
	}{{"pb", 1 << 50}, {"tb", 1 << 40}, {"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10}, {"b", 1}}
	factor := 1.0
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			s, factor = strings.TrimSuffix(s, u.suffix), u.factor
			break
		}
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return int64(n * factor), true
}

// formatBytes formats a size like elasticsearch, ie: 1.5mb
func formatBytes(n int64) string {
	units := []string{"b", "kb", "mb", "gb", "tb", "pb"}
	v, i := float64(n), 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 || v >= 100 {
		return fmt.Sprintf("%.0f%s", v, units[i])
	}
	return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64) + units[i]
}

// mapping parameters on the top level of a typeless mapping, everything else is a mapping type
var mappingParameters = map[string]bool{"properties": true, "dynamic": true, "dynamic_templates": true,
	"date_detection": true, "numeric_detection": true, "dynamic_date_formats": true, "runtime": true,
	"_source": true, "_routing": true, "_meta": true, "_field_names": true, "_all": true, "_size": true,
	"_data_stream_timestamp": true, "subobjects": true, "enabled": true}

// parseMappings reads the fields of a mapping, typeless (7.x+) or with mapping types (6.x and older)
func (idx *indexAnalysis) parseMappings(mapping map[string]interface{}) {
	typed := len(mapping) > 0
	for key, v := range mapping {
		if _, isMap := v.(map[string]interface{}); mappingParameters[key] || !isMap {
			typed = false
			break
		}
	}
	if !typed {
		idx.parseMapping(mapping)
		return
	}
	for typ := range mapping {
		idx.types = append(idx.types, typ)
	}
	sort.Strings(idx.types)
	for _, typ := range idx.types {
		idx.parseMapping(mapping[typ].(map[string]interface{}))
	}
}

func (idx *indexAnalysis) parseMapping(mapping map[string]interface{}) {
	for key, v := range mapping {
		if key == "properties" {
			if props, ok := v.(map[string]interface{}); ok {
				idx.parseFields("", props)
			}
			continue
		}
		idx.meta[key] = canonicalJSON(v)
	}
}

func (idx *indexAnalysis) parseFields(prefix string, props map[string]interface{}) {
	for name, v := range props {
		def, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		path := prefix + name
		own := map[string]interface{}{}
		for k, p := range def {
			if k != "properties" && k != "fields" {
				own[k] = p
			}
		}
		if _, ok := own["type"]; !ok {
			own["type"] = "object"
		}
		mapping := canonicalJSON(own)
		if existing, ok := idx.fields[path]; ok && existing != mapping {
			idx.conflict = append(idx.conflict, path)
		}
		idx.fields[path] = mapping
		for _, sub := range []string{"properties", "fields"} {
			if children, ok := def[sub].(map[string]interface{}); ok {
				idx.parseFields(path+".", children)
			}
		}
	}
}

func canonicalJSON(v interface{}) string {
	b, _ := json.Marshal(v) // map keys are sorted
	return string(b)
}

// fieldType returns the type of a field mapping
func fieldType(mapping string) string {
	var def struct {
		Type string `json:"type"`
	}
	json.Unmarshal([]byte(mapping), &def)
	return def.Type
}

// settings that are set by elasticsearch or only change during a migration, they are not compared
var ignoredSettings = []string{"index.uuid", "index.creation_date", "index.provided_name", "index.version.",
	"index.history.uuid", "index.routing.allocation.initial_recovery", "index.resize.", "index.shrink.",
	"index.verified_before_close", "index.frozen", "index.search.throttled"}

func ignoredSetting(key string) bool {
	for _, prefix := range ignoredSettings {
		if key == prefix || strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// isRemovedSettingV8 returns true for a setting that ESM removes when it creates an index on 8.x+
func isRemovedSettingV8(key string) bool {
	for _, path := range removedIndexSettingsV8 {
		prefix := "index." + strings.Join(path, ".")
		if key == prefix || strings.HasPrefix(key, prefix+".") {
			return true
		}
	}
	return false
}

// diffLine is one difference between source and target
type diffLine struct {
	key, source, target string
}

func diffMaps(source, target map[string]string, ignore func(string) bool) []diffLine {
	keys := map[string]bool{}
	for k := range source {
		keys[k] = true
	}
	for k := range target {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		if ignore == nil || !ignore(k) {
			sorted = append(sorted, k)
		}
	}
	sort.Strings(sorted)

	var lines []diffLine
	for _, k := range sorted {
		s, inSource := source[k]
		t, inTarget := target[k]
		if inSource && inTarget && s == t {
			continue
		}
		if !inSource {
			s = "-"
		}
		if !inTarget {
			t = "-"
		}
		lines = append(lines, diffLine{k, s, t})
	}
	return lines
}

// finding is a problem or a hint of the analysis
type finding struct {
	level string // ERROR: the migration fails, WARN: it may fail or lose data, INFO: good to know
	text  string
}

// suggestion is a flag of esm, or an elasticsearch setting, with the reason for it
type suggestion struct {
	flag   string
	reason string
}

// analysis is the result of comparing the source with the target
type analysis struct {
	cluster  []finding
	indices  map[string][]finding // source index => findings
	flags    []suggestion
	settings []suggestion // elasticsearch settings to change
}

func (a *analysis) add(index, level, format string, args ...interface{}) {
	f := finding{level, fmt.Sprintf(format, args...)}
	if index == "" {
		a.cluster = append(a.cluster, f)
		return
	}
	a.indices[index] = append(a.indices[index], f)
}

// analyse compares source and target (nil without -d) and suggests flags and settings
func analyse(c *Config, src, dst *clusterAnalysis) *analysis {
	a := &analysis{indices: map[string][]finding{}}

	if dst != nil {
		if src.major > dst.major {
			a.add("", "WARN", "the target (%s) is older than the source (%s), settings and mappings may not be accepted", dst.version, src.version)
		}
		if src.major != dst.major && (src.major < 7 || dst.major < 7) && c.CopyIndexMappings {
			a.add("", "WARN", "mappings of %d.x are copied to %d.x, check them manually: esm only converts mappings from 7.x on", src.major, dst.major)
		}
		if c.TargetIndexName != "" && len(src.indices) > 1 {
			a.add("", "WARN", "%d source indexes are merged into %s, documents with the same id overwrite each other", len(src.indices), c.TargetIndexName)
		}
	}

	var maxAvg, totalDocs int64
	maxShards := 0
	for _, name := range src.names() {
		idx := src.indices[name]
		if idx.closed {
			a.add(name, "ERROR", "the index is closed, open it to migrate it")
		}
		maxAvg = max(maxAvg, idx.avgDocBytes())
		totalDocs += idx.docs
		maxShards = max(maxShards, idx.shards)

		if idx.meta["_source"] != "" && strings.Contains(idx.meta["_source"], `"enabled":false`) {
			a.add(name, "ERROR", "_source is disabled, esm can not read the documents")
		}
		for _, f := range idx.conflict {
			a.add(name, "WARN", "field %s is mapped differently in the mapping types, only one mapping is copied", f)
		}

		var target *indexAnalysis
		tname := targetIndexName(c, name)
		if dst != nil {
			target = dst.indices[tname]
			analyseCompatibility(a, c, name, idx, target, dst)
		}
		if dst != nil && target == nil && !copySetupFlags(c) {
			a.add(name, "INFO", "target index %s does not exist, it is created by the first bulk request with default settings and dynamic mappings", tname)
		}
		if target != nil {
			analyseExistingTarget(a, c, name, idx, target)
		}
	}

	suggestFlags(a, c, src, dst, maxAvg, totalDocs, maxShards)
	return a
}

// analyseCompatibility finds settings and mappings that the target version rejects
func analyseCompatibility(a *analysis, c *Config, name string, idx, target *indexAnalysis, dst *clusterAnalysis) {
	copyMappings := c.CopyIndexMappings && target == nil

	if len(idx.types) > 1 && dst.major >= 6 {
		a.add(name, "WARN", "the index has %d mapping types (%s), %d.x supports one: documents with the same id in different types overwrite each other",
			len(idx.types), strings.Join(idx.types, ", "), dst.major)
	}
	if len(idx.types) > 0 && dst.major == 6 && c.OverrideTypeName == "" {
		a.add(name, "INFO", "documents keep their mapping type %s, -u _doc writes them with the 7.x type name", strings.Join(idx.types, ","))
	}

	var strings5, boosts []string
	for path, mapping := range idx.fields {
		if dst.major >= 5 && fieldType(mapping) == "string" {
			strings5 = append(strings5, path)
		}
		if dst.major >= 8 && strings.Contains(mapping, `"boost":`) {
			boosts = append(boosts, path)
		}
	}
	sort.Strings(strings5)
	sort.Strings(boosts)
	if len(strings5) > 0 && (copyMappings || target != nil) {
		level := "WARN"
		if copyMappings {
			level = "ERROR"
		}
		a.add(name, level, "%s the type string, removed in 5.x (ie: %s), create the target mapping with text/keyword manually",
			plural(len(strings5), "field")+map[bool]string{true: " uses", false: " use"}[len(strings5) == 1], strings.Join(firstN(strings5, 3), ", "))
	}
	if _, ok := idx.meta["_all"]; ok && dst.major >= 7 && copyMappings {
		a.add(name, "ERROR", "the mapping uses _all, removed in 7.x, create the target mapping manually")
	}
	if len(boosts) > 0 && copyMappings {
		a.add(name, "INFO", "boost is removed from %s, %d.x rejects it", plural(len(boosts), "field"), dst.major)
	}
	if _, ok := idx.meta["_field_names"]; ok && dst.major >= 8 && copyMappings {
		a.add(name, "INFO", "_field_names is removed from the mapping, %d.x rejects it", dst.major)
	}
	if dst.major >= 8 && c.CopyIndexSettings && target == nil {
		var removed []string
		for key := range idx.settings {
			if isRemovedSettingV8(key) {
				removed = append(removed, key)
			}
		}
		if len(removed) > 0 {
			sort.Strings(removed)
			a.add(name, "INFO", "settings rejected by %d.x are removed: %s", dst.major, strings.Join(removed, ", "))
		}
	}
}

// analyseExistingTarget compares the source index with the existing target index
func analyseExistingTarget(a *analysis, c *Config, name string, idx, target *indexAnalysis) {
	if target.closed {
		a.add(name, "ERROR", "the target index %s is closed", target.name)
	}
	if target.docs > 0 && !c.RecreateIndex && !c.Sync {
		a.add(name, "WARN", "the target index %s has %d documents, they are kept and documents with the same id are overwritten (-f recreates the index)",
			target.name, target.docs)
	}
	if c.RecreateIndex && !copySetupFlags(c) {
		a.add(name, "WARN", "-f only deletes the target index together with --copy_settings, --copy_mappings or --shards")
	}

	strict := strings.Contains(target.meta["dynamic"], "strict")
	for path, mapping := range idx.fields {
		tm, ok := target.fields[path]
		switch {
		case !ok && strict:
			a.add(name, "ERROR", "field %s is missing in the target mapping, which is dynamic: strict", path)
		case ok && fieldType(mapping) != fieldType(tm) && fieldType(mapping) != "string":
			a.add(name, "WARN", "field %s is %s in the source and %s in the target, documents may be rejected or indexed differently",
				path, fieldType(mapping), fieldType(tm))
		}
	}

	if !copySetupFlags(c) {
		if target.settings["index.refresh_interval"] != "-1" {
			a.add(name, "INFO", "refresh_interval and number_of_replicas of the existing target are only set to -1/0 during the migration "+
				"with --copy_settings, --copy_mappings or --shards, set them manually for a faster migration")
		}
	} else if idx.shards > 0 && target.shards > 0 && idx.shards != target.shards && c.ShardsCount == 0 {
		a.add(name, "INFO", "the target has %s and the source %d, the shards of an existing index can not be changed (-f recreates it)",
			plural(target.shards, "primary shard"), idx.shards)
	}
}

func copySetupFlags(c *Config) bool {
	return c.CopyIndexSettings || c.CopyIndexMappings || c.ShardsCount > 0
}

func firstN(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// suggestFlags derives the esm flags and elasticsearch settings from the size of the documents and the clusters
func suggestFlags(a *analysis, c *Config, src, dst *clusterAnalysis, avg, totalDocs int64, maxShards int) {
	flag := func(value, format string, args ...interface{}) {
		a.flags = append(a.flags, suggestion{value, fmt.Sprintf(format, args...)})
	}
	setting := func(value, format string, args ...interface{}) {
		a.settings = append(a.settings, suggestion{value, fmt.Sprintf(format, args...)})
	}

	if avg > 0 {
		// a scroll page of about 10MB, the source holds it in its heap
		count := int(clampInt64(analysePageBytes/avg, 1, defaultDocBufferCount))
		if count != c.DocBufferCount {
			flag(fmt.Sprintf("-c %d", count), "documents are about %s on average, a scroll page of about %s", formatBytes(avg), formatBytes(int64(count)*avg))
		}
		// esm holds up to --buffer_count documents in memory
		buffer := int(clampInt64(analyseBufferBytes/avg, int64(count)*2, defaultBufferCount))
		if buffer != c.BufferCount {
			flag(fmt.Sprintf("--buffer_count=%d", buffer), "keeps the memory of esm at about %s", formatBytes(int64(buffer)*avg))
		}
	}

	if dst != nil {
		maxContent := dst.maxContentLength
		source := "of the target"
		if dst.maxContentDefault {
			source = "(the default, not readable from the target)"
		}
		if int64(c.BulkSizeInMB)<<20 >= maxContent {
			flag(fmt.Sprintf("-b %d", max(1, maxContent>>21)), "bulk requests must be smaller than http.max_content_length %s %s", formatBytes(maxContent), source)
		}
		if avg > 0 && avg*2 > maxContent {
			// elasticsearch does not accept more than 2gb
			size := min(int64(math.Ceil(float64(avg*4)/float64(1<<20)))<<20, 2047<<20)
			setting(fmt.Sprintf("target elasticsearch.yml: http.max_content_length: %s", formatBytes(size)),
				"documents are about %s on average, a bulk request with a larger one is rejected above %s", formatBytes(avg), formatBytes(maxContent))
		}
	}

	if totalDocs >= analyseManyDocs {
		workers := 2
		if dst != nil && dst.dataNodes > 0 {
			workers = dst.dataNodes * 2
		}
		if avg >= analyseLargeDocBytes {
			workers = 2
		}
		workers = min(workers, analyseMaxWorkers)
		if workers != c.Workers {
			nodes := ""
			if dst != nil && dst.dataNodes > 0 {
				nodes = fmt.Sprintf(", the target has %d data nodes", dst.dataNodes)
			}
			flag(fmt.Sprintf("-w %d", workers), "%d documents%s", totalDocs, nodes)
		}

		slices := min(maxShards, analyseMaxSlices)
		if src.major >= 5 && slices > 1 && slices != c.ScrollSliceSize {
			flag(fmt.Sprintf("--sliced_scroll_size=%d", slices), "reads the source in parallel, up to one slice per primary shard (%d)", maxShards)
		}
	}

	if dst != nil {
		var missing []string
		var largeShards []string
		for _, name := range src.names() {
			idx := src.indices[name]
			if _, ok := dst.indices[targetIndexName(c, name)]; ok {
				continue
			}
			missing = append(missing, name)
			if idx.shards > 0 && idx.priBytes/int64(idx.shards) > analyseMaxShardBytes {
				largeShards = append(largeShards, name)
				if c.ShardsCount == 0 {
					flag(fmt.Sprintf("--shards=%d", int(math.Ceil(float64(idx.priBytes)/analyseShardBytes))),
						"the primary shards of %s are %s each, keeps the target shards at about %s", name,
						formatBytes(idx.priBytes/int64(idx.shards)), formatBytes(analyseShardBytes))
				}
			}
		}
		if len(missing) > 0 && !c.CopyIndexSettings && !c.CopyIndexMappings {
			flag("--copy_settings --copy_mappings", "the target index of %s does not exist yet, creates it like the source", strings.Join(firstN(missing, 3), ", "))
		}

		for _, cl := range []*clusterAnalysis{src, dst} {
			if cl.major >= 8 && !cl.idFieldData {
				setting(fmt.Sprintf(`%s: PUT _cluster/settings {"persistent":{"indices.id_field_data.enabled":true}}`, cl.role),
					"only for --sync, it sorts by _id, which %d.x disables by default", cl.major)
			}
		}
	}
}

func clampInt64(v, lo, hi int64) int64 {
	return max(lo, min(v, hi))
}

// hasErrors returns true if a finding is an ERROR
func (a *analysis) hasErrors() bool {
	for _, f := range a.cluster {
		if f.level == "ERROR" {
			return true
		}
	}
	for _, findings := range a.indices {
		for _, f := range findings {
			if f.level == "ERROR" {
				return true
			}
		}
	}
	return false
}

// writeAnalysis prints the analysis and returns it
func writeAnalysis(w io.Writer, c *Config, src, dst *clusterAnalysis) *analysis {
	a := analyse(c, src, dst)

	fmt.Fprintln(w, "clusters:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if dst != nil {
		fmt.Fprintln(tw, "\tSOURCE\tTARGET")
		fmt.Fprintf(tw, "  url\t%s\t%s\n", src.url, dst.url)
		fmt.Fprintf(tw, "  version\t%s\t%s\n", src.version, dst.version)
		fmt.Fprintf(tw, "  data nodes\t%d\t%d\n", src.dataNodes, dst.dataNodes)
		fmt.Fprintf(tw, "  http.max_content_length\t%s\t%s\n", maxContentString(src), maxContentString(dst))
		fmt.Fprintf(tw, "  indices.id_field_data.enabled\t%t\t%t\n", src.idFieldData, dst.idFieldData)
	} else {
		fmt.Fprintln(tw, "\tSOURCE")
		fmt.Fprintf(tw, "  url\t%s\n", src.url)
		fmt.Fprintf(tw, "  version\t%s\n", src.version)
		fmt.Fprintf(tw, "  data nodes\t%d\n", src.dataNodes)
		fmt.Fprintf(tw, "  http.max_content_length\t%s\n", maxContentString(src))
		fmt.Fprintf(tw, "  indices.id_field_data.enabled\t%t\n", src.idFieldData)
	}
	tw.Flush()
	writeFindings(w, a.cluster)

	for _, name := range src.names() {
		idx := src.indices[name]
		tname := targetIndexName(c, name)
		fmt.Fprintf(w, "\nindex %s", name)
		if dst != nil {
			fmt.Fprintf(w, " => %s", tname)
		}
		fmt.Fprintln(w)
		fmt.Fprintf(w, "  source: %s\n", indexSummary(idx))
		var target *indexAnalysis
		if dst != nil {
			target = dst.indices[tname]
			if target == nil {
				fmt.Fprintln(w, "  target: does not exist")
			} else {
				fmt.Fprintf(w, "  target: %s\n", indexSummary(target))
			}
		}
		if target != nil {
			writeDiff(w, "settings", "SETTING", diffMaps(idx.settings, target.settings, ignoredSetting))
			sourceMapping, targetMapping := map[string]string{}, map[string]string{}
			for k, v := range idx.meta {
				sourceMapping[k] = v
			}
			for k, v := range idx.fields {
				sourceMapping[k] = v
			}
			for k, v := range target.meta {
				targetMapping[k] = v
			}
			for k, v := range target.fields {
				targetMapping[k] = v
			}
			writeDiff(w, "mappings", "FIELD", diffMaps(sourceMapping, targetMapping, nil))
		}
		writeFindings(w, a.indices[name])
	}

	fmt.Fprintln(w, "\nsuggestions:")
	if len(a.flags) == 0 && len(a.settings) == 0 {
		fmt.Fprintln(w, "  none, the defaults fit")
	}
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, s := range a.flags {
		fmt.Fprintf(tw, "  %s\t%s\n", s.flag, s.reason)
	}
	tw.Flush()
	for _, s := range a.settings {
		fmt.Fprintf(w, "  %s\n      %s\n", s.flag, s.reason)
	}

	cmd := []string{"esm", "-s", src.url}
	if dst != nil {
		cmd = append(cmd, "-d", dst.url)
	}
	cmd = append(cmd, "-x", shellQuote(c.SourceIndexNames))
	if c.TargetIndexName != "" {
		cmd = append(cmd, "-y", shellQuote(c.TargetIndexName))
	}
	for _, s := range a.flags {
		cmd = append(cmd, s.flag)
	}
	if dst != nil {
		fmt.Fprintf(w, "\nsuggested command (add the auth and TLS flags):\n  %s\n", strings.Join(cmd, " "))
	}
	return a
}

func maxContentString(ca *clusterAnalysis) string {
	if ca.maxContentDefault {
		return formatBytes(ca.maxContentLength) + " (default)"
	}
	return formatBytes(ca.maxContentLength)
}

func indexSummary(idx *indexAnalysis) string {
	s := fmt.Sprintf("%s, %s, %s, %s", plural(idx.docs, "document"), plural(idx.shards, "primary shard"),
		plural(idx.replicas, "replica"), formatBytes(idx.priBytes))
	if avg := idx.avgDocBytes(); avg > 0 {
		s += fmt.Sprintf(" (about %s per document)", formatBytes(avg))
	}
	if len(idx.types) > 0 {
		s += ", mapping types: " + strings.Join(idx.types, ",")
	}
	if idx.closed {
		s += ", closed"
	}
	return s
}

func writeDiff(w io.Writer, title, column string, lines []diffLine) {
	if len(lines) == 0 {
		fmt.Fprintf(w, "  %s: equal\n", title)
		return
	}
	fmt.Fprintf(w, "  %s: %s\n", title, plural(len(lines), "difference"))
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "    %s\tSOURCE\tTARGET\n", column)
	for i, l := range lines {
		if i == analyseMaxDiffLines {
			fmt.Fprintf(tw, "    ... %d more\t\t\n", len(lines)-i)
			break
		}
		fmt.Fprintf(tw, "    %s\t%s\t%s\n", l.key, truncate(l.source, 60), truncate(l.target, 60))
	}
	tw.Flush()
}

func writeFindings(w io.Writer, findings []finding) {
	for _, f := range findings {
		fmt.Fprintf(w, "  %-5s %s\n", f.level, f.text)
	}
}

func plural[T int | int64](n T, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}

// shellQuote quotes an argument of the suggested command if the shell would expand it
func shellQuote(s string) string {
	if strings.ContainsAny(s, "*?[]{} \t\"'$&|;<>()") {
		return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
	}
	return s
}

func truncate(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n-3]) + "..."
	}
	return s
}
