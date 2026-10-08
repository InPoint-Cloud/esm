package main

import (
	"fmt"
	"strings"
	"testing"
)

// analyseOutput runs esm with --analyse and returns the exit code and stdout
func analyseOutput(t *testing.T, args ...string) (int, string) {
	t.Helper()
	code := 0
	out := captureStdout(t, func() {
		code = run(append(args, "--analyse"))
	})
	return code, out
}

func wantContains(t *testing.T, out string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("output does not contain %q:\n%s", w, out)
		}
	}
}

func TestAnalyse(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("logs", 200)
	src.addIndex("other", 10)
	src.addIndex(".kibana", 1)
	dst := newFakeES("8.11.0")
	dst.dataNodes = 3
	logs := dst.addIndex("logs", 5)
	logs.settings["number_of_shards"] = "1"
	logs.mappings = map[string]interface{}{"properties": map[string]interface{}{"n": map[string]interface{}{"type": "keyword"}}}

	code, out := analyseOutput(t, "-s", src.start(t), "-d", dst.start(t), "-x", "*")
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	wantContains(t, out,
		"7.10.2", "8.11.0",
		"index logs => logs", "index other => other",
		"target: does not exist",
		"index.number_of_shards", // 3 in the source, 1 in the target
		"field n is long in the source and keyword in the target",
		"the target index logs has 5 documents",
		"--copy_settings --copy_mappings",
		`target: PUT _cluster/settings {"persistent":{"indices.id_field_data.enabled":true}}`,
		"-x '*' --copy_settings --copy_mappings",
	)
	if strings.Contains(out, ".kibana") {
		t.Errorf("system index analysed without -a:\n%s", out)
	}

	// nothing is migrated
	if n := len(dst.docs("logs")); n != 5 {
		t.Errorf("target logs has %d documents, want 5", n)
	}
	if dst.settings("other") != nil {
		t.Error("target index other was created")
	}
}

func TestAnalyseSourceOnly(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("logs", 20)

	code, out := analyseOutput(t, "-s", src.start(t), "-x", "logs")
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	wantContains(t, out, "index logs\n", "source: 20 documents, 3 primary shards, 1 replica,", "suggestions:\n  none")
	if strings.Contains(out, "suggested command") {
		t.Errorf("command suggested without a target:\n%s", out)
	}
}

func TestAnalyseErrors(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("logs", 20)
	url := src.start(t)

	if code, _ := analyseOutput(t, "-s", url, "-x", "missing"); code != 1 {
		t.Errorf("missing source index: exit code %d, want 1", code)
	}
	if code, _ := analyseOutput(t, "-d", url, "-x", "logs"); code != 1 {
		t.Errorf("no source: exit code %d, want 1", code)
	}
	if code, _ := analyseOutput(t, "-s", url, "-d", "http://127.0.0.1:1", "-x", "logs"); code != 1 {
		t.Errorf("unreachable target: exit code %d, want 1", code)
	}
}

func TestAnalyseOldSource(t *testing.T) {
	src := newFakeES("2.4.6")
	idx := src.addIndex("old", 10)
	idx.mappings = map[string]interface{}{
		"a": map[string]interface{}{"properties": map[string]interface{}{"title": map[string]interface{}{"type": "string", "boost": 2}}},
		"b": map[string]interface{}{"properties": map[string]interface{}{"title": map[string]interface{}{"type": "string", "index": "not_analyzed"}}},
	}
	dst := newFakeES("8.11.0")

	code, out := analyseOutput(t, "-s", src.start(t), "-d", dst.start(t), "-x", "old", "-y", "new", "--copy_mappings")
	if code != 2 {
		t.Fatalf("exit code %d, want 2 for the string fields", code)
	}
	wantContains(t, out,
		"mapping types: a,b",
		"ERROR 1 field uses the type string",
		"the index has 2 mapping types (a, b)",
		"field title is mapped differently in the mapping types",
		"mappings of 2.x are copied to 8.x, check them manually",
	)
}

func TestAnalyseStrictTarget(t *testing.T) {
	src := newFakeES("8.11.0")
	src.addIndex("logs", 5)
	src.indices["logs"].mappings["properties"].(map[string]interface{})["extra"] = map[string]interface{}{"type": "keyword"}
	src.clusterSettings = map[string]interface{}{"indices": map[string]interface{}{"id_field_data": map[string]interface{}{"enabled": "true"}}}
	dst := newFakeES("9.1.0")
	idx := dst.addIndex("logs", 0)
	idx.mappings["dynamic"] = "strict"
	idx.settings["number_of_shards"] = "1"
	dst.maxContentLength = "2mb"

	code, out := analyseOutput(t, "-s", src.start(t), "-d", dst.start(t), "-x", "logs", "--copy_settings")
	if code != 2 {
		t.Fatalf("exit code %d, want 2 for the missing field", code)
	}
	wantContains(t, out,
		"ERROR field extra is missing in the target mapping, which is dynamic: strict",
		"suggested command (add the auth and TLS flags):",
		"-b 1", "http.max_content_length 2mb of the target",
		"the target has 1 primary shard and the source 3",
	)
	if strings.Contains(out, "source: PUT _cluster/settings") {
		t.Errorf("id_field_data suggested for the source, it is enabled:\n%s", out)
	}
}

// analysedIndex returns an index with docs documents of avg bytes each
func analysedIndex(name string, docs, avg int64, shards int) *indexAnalysis {
	return &indexAnalysis{name: name, docs: docs, priBytes: docs * avg, shards: shards, replicas: 1,
		settings: map[string]string{}, meta: map[string]string{}, fields: map[string]string{}}
}

func analysedCluster(role, version string, dataNodes int, indices ...*indexAnalysis) *clusterAnalysis {
	ca := &clusterAnalysis{role: role, url: "http://" + role, version: version, dataNodes: dataNodes,
		maxContentLength: defaultMaxContentLength, indices: map[string]*indexAnalysis{}}
	fmt.Sscanf(version, "%d.", &ca.major)
	ca.idFieldData = ca.major < 8
	for _, idx := range indices {
		ca.indices[idx.name] = idx
	}
	return ca
}

func defaultConfig() *Config {
	return &Config{SourceIndexNames: "_all", DocBufferCount: 10000, BufferCount: 1000000, Workers: 1, BulkSizeInMB: 5, ScrollSliceSize: 1}
}

func flagsOf(a *analysis) string {
	var flags []string
	for _, s := range a.flags {
		flags = append(flags, s.flag)
	}
	return strings.Join(flags, " ")
}

func TestSuggestFlags(t *testing.T) {
	tests := []struct {
		name          string
		docs, avg     int64
		shards, nodes int
		want          string
		setting       bool // http.max_content_length suggested
	}{
		{"small index", 1000, 500, 1, 1, "", false},
		{"many small documents", 2000000, 1 << 10, 5, 3, "-w 6 --sliced_scroll_size=5", false},
		{"many shards", 2000000, 1 << 10, 30, 20, "-w 16 --sliced_scroll_size=8", false},
		{"20kb documents", 1000, 20 << 10, 1, 1, "-c 512 --buffer_count=52428", false},
		{"20mb documents", 200000, 20 << 20, 3, 3, "-c 1 --buffer_count=51 -w 2 --sliced_scroll_size=3", false},
		{"60mb documents", 1000, 60 << 20, 1, 1, "-c 1 --buffer_count=17", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := analysedCluster("source", "7.17.0", 1, analysedIndex("i", tt.docs, tt.avg, tt.shards))
			dst := analysedCluster("target", "7.17.0", tt.nodes, analysedIndex("i", 0, 0, tt.shards))
			a := analyse(defaultConfig(), src, dst)
			if got := flagsOf(a); got != tt.want {
				t.Errorf("flags %q, want %q", got, tt.want)
			}
			if got := len(a.settings) > 0; got != tt.setting {
				t.Errorf("settings %v, want max_content_length suggested: %t", a.settings, tt.setting)
			}
		})
	}
}

func TestSuggestShards(t *testing.T) {
	// 300GB in 2 primary shards, the target index does not exist
	src := analysedCluster("source", "7.17.0", 1, analysedIndex("big", 3<<20, 100<<10, 2))
	dst := analysedCluster("target", "8.11.0", 1)
	a := analyse(defaultConfig(), src, dst)
	wantContains(t, flagsOf(a), "--shards=10", "--copy_settings --copy_mappings")
	if len(a.settings) != 1 || !strings.HasPrefix(a.settings[0].flag, "target: PUT _cluster/settings") {
		t.Errorf("settings %v, want id_field_data of the target", a.settings)
	}
}

func TestParseMappings(t *testing.T) {
	typeless := &indexAnalysis{meta: map[string]string{}, fields: map[string]string{}}
	typeless.parseMappings(map[string]interface{}{
		"dynamic": "strict",
		"properties": map[string]interface{}{
			"title": map[string]interface{}{"type": "text", "fields": map[string]interface{}{"raw": map[string]interface{}{"type": "keyword"}}},
			"user":  map[string]interface{}{"properties": map[string]interface{}{"name": map[string]interface{}{"type": "keyword"}}},
		},
	})
	if len(typeless.types) != 0 {
		t.Errorf("types %v, want none", typeless.types)
	}
	want := map[string]string{"title": "text", "title.raw": "keyword", "user": "object", "user.name": "keyword"}
	for path, typ := range want {
		if got := fieldType(typeless.fields[path]); got != typ {
			t.Errorf("field %s: type %q, want %q", path, got, typ)
		}
	}
	if typeless.meta["dynamic"] != `"strict"` {
		t.Errorf("meta %v", typeless.meta)
	}

	typed := &indexAnalysis{meta: map[string]string{}, fields: map[string]string{}}
	typed.parseMappings(map[string]interface{}{
		"doc": map[string]interface{}{"_all": map[string]interface{}{"enabled": false}, "properties": map[string]interface{}{"n": map[string]interface{}{"type": "long"}}},
	})
	if strings.Join(typed.types, ",") != "doc" || fieldType(typed.fields["n"]) != "long" || typed.meta["_all"] == "" {
		t.Errorf("typed mapping: types %v, fields %v, meta %v", typed.types, typed.fields, typed.meta)
	}
}

func TestDiffMaps(t *testing.T) {
	lines := diffMaps(
		map[string]string{"index.number_of_shards": "3", "index.uuid": "a", "index.codec": "best_compression", "same": "1"},
		map[string]string{"index.number_of_shards": "1", "index.uuid": "b", "index.refresh_interval": "1s", "same": "1"},
		ignoredSetting)
	got := fmt.Sprint(lines)
	want := "[{index.codec best_compression -} {index.number_of_shards 3 1} {index.refresh_interval - 1s}]"
	if got != want {
		t.Errorf("diff %s, want %s", got, want)
	}
}

func TestByteSizes(t *testing.T) {
	for in, want := range map[string]int64{"100mb": 100 << 20, "1gb": 1 << 30, "512": 512, "1.5kb": 1536, "2MB": 2 << 20} {
		if got, ok := parseByteSize(in); !ok || got != want {
			t.Errorf("parseByteSize(%q) = %d, %t, want %d", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "abc", "-1mb"} {
		if _, ok := parseByteSize(in); ok {
			t.Errorf("parseByteSize(%q) is valid", in)
		}
	}
	for in, want := range map[int64]string{0: "0b", 1023: "1023b", 1536: "1.5kb", 100 << 20: "100mb", 20 << 30: "20gb", 2 << 20: "2mb"} {
		if got := formatBytes(in); got != want {
			t.Errorf("formatBytes(%d) = %s, want %s", in, got, want)
		}
	}
}

func TestIsRemovedSettingV8(t *testing.T) {
	for key, want := range map[string]bool{"index.mapper.dynamic": true, "index.translog.retention.size": true,
		"index.frozen": true, "index.number_of_shards": false, "index.mapper": false} {
		if got := isRemovedSettingV8(key); got != want {
			t.Errorf("isRemovedSettingV8(%s) = %t, want %t", key, got, want)
		}
	}
}
