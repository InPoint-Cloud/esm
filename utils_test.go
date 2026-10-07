package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestSplitFieldList(t *testing.T) {
	tests := map[string][]string{
		"":             nil,
		"a":            {"a"},
		"a,b":          {"a", "b"},
		" a , ,b, ":    {"a", "b"},
		",,":           nil,
		"col1,col2,c3": {"col1", "col2", "c3"},
	}
	for in, want := range tests {
		if got := splitFieldList(in); !reflect.DeepEqual(got, want) {
			t.Errorf("splitFieldList(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRemoveSourceFields(t *testing.T) {
	src := json.RawMessage(`{"a":1,"b":{"c":2},"d":"x"}`)

	got, err := removeSourceFields(src, []string{"b", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"a":1,"d":"x"}` {
		t.Errorf("got %s", got)
	}

	// no fields, the source is returned untouched
	if got, _ := removeSourceFields(src, nil); string(got) != string(src) {
		t.Errorf("got %s, want unchanged source", got)
	}

	if _, err := removeSourceFields(json.RawMessage(`not json`), []string{"a"}); err == nil {
		t.Error("want an error for an invalid source")
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{&HTTPStatusError{Code: 429}, true},
		{&HTTPStatusError{Code: 503}, true},
		{&HTTPStatusError{Code: 400}, false},
		{&HTTPStatusError{Code: 404}, false},
		{errors.New("connection reset"), true},
	}
	for _, tt := range tests {
		if got := isRetryable(tt.err); got != tt.want {
			t.Errorf("isRetryable(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestBackoff(t *testing.T) {
	tests := map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 10: 30 * time.Second, 100: 30 * time.Second}
	for attempt, want := range tests {
		if got := backoff(attempt); got != want {
			t.Errorf("backoff(%d) = %s, want %s", attempt, got, want)
		}
	}
}

func TestDocumentRouting(t *testing.T) {
	var hit Document
	if err := json.Unmarshal([]byte(`{"_id":"1","_routing":"r1"}`), &hit); err != nil {
		t.Fatal(err)
	}
	if hit.Routing != "r1" {
		t.Errorf("routing of a search hit = %q, want r1", hit.Routing)
	}

	var dumped Document
	if err := json.Unmarshal([]byte(`{"_id":"1","routing":"r2"}`), &dumped); err != nil {
		t.Fatal(err)
	}
	if dumped.Routing != "r2" {
		t.Errorf("routing of a dumped doc = %q, want r2", dumped.Routing)
	}
}

func TestGetSendsAuth(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"denied"}`))
	}))
	defer srv.Close()

	resp, body, errs := Get(srv.URL, &Auth{User: "elastic", Pass: "secret"}, "")
	if errs != nil {
		t.Fatal(errs)
	}
	if got != "Basic ZWxhc3RpYzpzZWNyZXQ=" {
		t.Errorf("basic auth header = %q", got)
	}
	// the body is returned for error responses too
	if resp.StatusCode != http.StatusUnauthorized || body != `{"error":"denied"}` {
		t.Errorf("got %d %q", resp.StatusCode, body)
	}

	Get(srv.URL, &Auth{ApiKey: "a2V5"}, "")
	if got != "ApiKey a2V5" {
		t.Errorf("api key header = %q", got)
	}
}
