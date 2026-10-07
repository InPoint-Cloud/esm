package main

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

func decode(t *testing.T, s string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSanitizeIndexSettingsV8(t *testing.T) {
	settings := decode(t, `{"settings":{"index":{
		"number_of_shards":"1",
		"mapper":{"dynamic":"false"},
		"soft_deletes.enabled":"true",
		"translog":{"retention":{"size":"1gb"},"durability":"async"},
		"routing":{"allocation":{"initial_recovery":{"_id":"x"},"include":{"zone":"a"}}},
		"frozen":"true",
		"analysis":{
			"filter":{"grams":{"type":"nGram"},"edge":{"type":"edgeNGram"},"other":{"type":"lowercase"},"bad":"x"},
			"tokenizer":{"tok":{"type":"edgeNGram"}}
		}}}}`)
	sanitizeIndexSettingsV8(settings)

	want := decode(t, `{"settings":{"index":{
		"number_of_shards":"1",
		"translog":{"durability":"async"},
		"routing":{"allocation":{"include":{"zone":"a"}}},
		"analysis":{
			"filter":{"grams":{"type":"ngram"},"edge":{"type":"edge_ngram"},"other":{"type":"lowercase"},"bad":"x"},
			"tokenizer":{"tok":{"type":"edge_ngram"}}
		}}}}`)
	if !reflect.DeepEqual(settings, want) {
		got, _ := json.Marshal(settings)
		t.Errorf("got %s", got)
	}

	// settings without an index object are left alone
	for _, s := range []string{`{}`, `{"settings":{}}`, `{"settings":{"index":{"number_of_shards":"1"}}}`} {
		m := decode(t, s)
		sanitizeIndexSettingsV8(m)
		if !reflect.DeepEqual(m, decode(t, s)) {
			t.Errorf("%s changed", s)
		}
	}
}

func TestSanitizeMappingsV8(t *testing.T) {
	mappings := decode(t, `{
		"_field_names":{"enabled":false},
		"properties":{
			"title":{"type":"text","boost":2,"fields":{"raw":{"type":"keyword","boost":3}}},
			"user":{"properties":{"name":{"type":"text","boost":1.5}}}
		},
		"dynamic_templates":[{"strings":{"match":"*","mapping":{"type":"keyword","boost":2}}},"bad"]
	}`)
	sanitizeMappingsV8(mappings)

	want := decode(t, `{
		"properties":{
			"title":{"type":"text","fields":{"raw":{"type":"keyword"}}},
			"user":{"properties":{"name":{"type":"text"}}}
		},
		"dynamic_templates":[{"strings":{"match":"*","mapping":{"type":"keyword"}}},"bad"]
	}`)
	if !reflect.DeepEqual(mappings, want) {
		got, _ := json.Marshal(mappings)
		t.Errorf("got %s", got)
	}

	// other _field_names options are kept
	m := decode(t, `{"_field_names":{"enabled":false,"other":1}}`)
	sanitizeMappingsV8(m)
	if !reflect.DeepEqual(m, decode(t, `{"_field_names":{"other":1}}`)) {
		t.Errorf("got %v", m)
	}
}

func TestMajorVersion(t *testing.T) {
	tests := map[string]int{"7.10.2": 7, "8.0.0-rc1": 8, "10.1": 10, "": 0, "x.y": 0}
	for number, want := range tests {
		v := &ClusterVersion{}
		v.Version.Number = number
		if got := majorVersion(v); got != want {
			t.Errorf("majorVersion(%q) = %d, want %d", number, got, want)
		}
	}
	if got := majorVersion(nil); got != 0 {
		t.Errorf("majorVersion(nil) = %d", got)
	}
}

func TestToInt(t *testing.T) {
	tests := map[string]int{"12": 12, "12.7": 12, "0": 0}
	for in, want := range tests {
		if got, err := ToInt(in); err != nil || got != want {
			t.Errorf("ToInt(%q) = %d, %v", in, got, err)
		}
	}
	if _, err := ToInt("x"); err == nil {
		t.Error("want an error for x")
	}
}

func TestBulkErrorReason(t *testing.T) {
	structured := Action{Index: "i", Id: "1", Status: 400, Error: map[string]interface{}{"type": "mapper_parsing_exception", "reason": "bad"}}
	if got := bulkErrorReason(structured); got != "index=i id=1 status=400 mapper_parsing_exception: bad" {
		t.Errorf("got %q", got)
	}
	plain := Action{Index: "i", Id: "2", Status: 500, Error: "boom"}
	if got := bulkErrorReason(plain); got != `index=i id=2 status=500 "boom"` {
		t.Errorf("got %q", got)
	}
}

func TestSaveFailed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.json")
	var s MigrationStats
	s.saveFailed([]byte("ignored, no file open\n{}\n"))
	if err := s.OpenFailedOutput(path); err != nil {
		t.Fatal(err)
	}
	s.saveFailed([]byte(`{"index":{"_index":"i","_id":"1"}}` + "\n" + `{"n":1}` + "\n"))
	s.saveFailed([]byte(`{"delete":{"_index":"i","_id":"2"}}` + "\n"))
	s.saveFailed([]byte("not json\n{}\n"))
	s.CloseFailedOutput()

	lines := readLines(t, path)
	if len(lines) != 1 || lines[0] != `{"_index":"i","_id":"1","_source":{"n":1}}` {
		t.Errorf("failed output = %q", lines)
	}
	if s.failedErrors != 1 {
		t.Errorf("failedErrors = %d, want 1 for the invalid item", s.failedErrors)
	}
	if s.failedSaved != 1 {
		t.Errorf("failedSaved = %d", s.failedSaved)
	}
}

func TestBulkOperationString(t *testing.T) {
	for op, want := range map[BulkOperation]string{opIndex: "opIndex", opDelete: "opDelete", 7: "unknown:7"} {
		if got := op.String(); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
