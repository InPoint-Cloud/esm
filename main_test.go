package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// mockES is a minimal elasticsearch serving one index of docs through (sliced) scrolls
// and recording the bulk requests it receives
type mockES struct {
	version     string // cluster version, ie: 7.10.2
	docs        int    // number of documents in the index
	scrollError int    // if set, scroll requests after the first page answer with this status code

	mu   sync.Mutex
	bulk []string // source lines of all bulk requests
}

func (m *mockES) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(srv.Close)
	return srv.URL
}

func (m *mockES) major() int {
	var major int
	fmt.Sscanf(m.version, "%d.", &major)
	return major
}

func (m *mockES) hitsTotal(n int) interface{} {
	if m.major() >= 7 {
		return map[string]interface{}{"value": n, "relation": "eq"}
	}
	return n
}

func (m *mockES) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	path := r.URL.Path
	reply := func(o interface{}) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(o)
	}

	switch {
	case path == "/":
		reply(map[string]interface{}{"version": map[string]string{"number": m.version}})
	case strings.HasPrefix(path, "/_search/scroll"):
		if m.scrollError != 0 {
			w.WriteHeader(m.scrollError)
			w.Write([]byte("{}"))
			return
		}
		reply(map[string]interface{}{"_scroll_id": "done", "hits": map[string]interface{}{"total": m.hitsTotal(0), "hits": []interface{}{}}})
	case strings.HasSuffix(path, "/_search"):
		var q struct {
			Slice *struct{ Id, Max int } `json:"slice"`
		}
		json.Unmarshal(body, &q)
		slice, max := 0, 1
		if q.Slice != nil {
			slice, max = q.Slice.Id, q.Slice.Max
		}
		var hits []interface{}
		for i := slice; i < m.docs; i += max {
			hits = append(hits, map[string]interface{}{
				"_index": "src", "_type": "_doc", "_id": fmt.Sprint(i),
				"_source": map[string]interface{}{"n": i, "secret": "x", "keep": true},
			})
		}
		reply(map[string]interface{}{"_scroll_id": fmt.Sprint("s", slice), "hits": map[string]interface{}{"total": m.hitsTotal(len(hits)), "hits": hits}})
	case strings.HasSuffix(path, "/_settings"):
		reply(map[string]interface{}{"src": map[string]interface{}{"settings": map[string]interface{}{"index": map[string]string{"number_of_shards": "1", "number_of_replicas": "0"}}}})
	case strings.HasSuffix(path, "/_mapping"):
		reply(map[string]interface{}{"src": map[string]interface{}{"mappings": map[string]interface{}{"properties": map[string]interface{}{}}}})
	case strings.HasSuffix(path, "/_count"):
		reply(map[string]int{"count": m.docs})
	case strings.HasSuffix(path, "/_bulk"):
		lines := strings.Split(strings.TrimSpace(string(body)), "\n")
		var items []interface{}
		m.mu.Lock()
		for i := 1; i < len(lines); i += 2 {
			m.bulk = append(m.bulk, lines[i])
			items = append(items, map[string]interface{}{"index": map[string]int{"status": 201}})
		}
		m.mu.Unlock()
		reply(map[string]interface{}{"errors": false, "items": items})
	default:
		reply(map[string]interface{}{})
	}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		lines = append(lines, s.Text())
	}
	return lines
}

// checkDocs verifies that each line is a document with a _source, all docs are there and the skipped field is gone
func checkDocs(t *testing.T, lines []string, want int, skipped string) {
	t.Helper()
	if len(lines) != want {
		t.Fatalf("got %d documents, want %d", len(lines), want)
	}
	for _, l := range lines {
		if !strings.Contains(l, `"n":`) {
			t.Fatalf("not a document: %q", l)
		}
		if strings.Contains(l, `"`+skipped+`"`) {
			t.Fatalf("skipped field %q still present: %q", skipped, l)
		}
	}
}

func TestDumpToFile(t *testing.T) {
	es := &mockES{version: "7.10.2", docs: 1000}
	out := filepath.Join(t.TempDir(), "dump.json")

	if code := run([]string{"-s", es.start(t), "-x", "src", "--sliced_scroll_size=4", "-o", out, "--skip=secret"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	checkDocs(t, readLines(t, out), 1000, "secret")
}

func TestReimportDumpFile(t *testing.T) {
	es := &mockES{version: "7.10.2", docs: 100}
	dir := t.TempDir()
	dump, out := filepath.Join(dir, "dump.json"), filepath.Join(dir, "out.json")

	if code := run([]string{"-s", es.start(t), "-x", "src", "-o", dump}); code != 0 {
		t.Fatalf("dump exit code %d", code)
	}
	if code := run([]string{"-i", dump, "-o", out, "--skip=keep"}); code != 0 {
		t.Fatalf("reimport exit code %d", code)
	}
	checkDocs(t, readLines(t, out), 100, "keep")
}

func TestMigrateToES(t *testing.T) {
	src := &mockES{version: "7.10.2", docs: 1000}
	dst := &mockES{version: "8.11.0", docs: 1000}

	code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst",
		"--sliced_scroll_size=4", "-w", "4", "--skip=secret"})
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	dst.mu.Lock()
	defer dst.mu.Unlock()
	checkDocs(t, dst.bulk, 1000, "secret")
}

func TestScrollErrorFailsDump(t *testing.T) {
	for _, version := range []string{"6.8.23", "7.10.2"} {
		t.Run(version, func(t *testing.T) {
			es := &mockES{version: version, docs: 10, scrollError: http.StatusNotFound}
			out := filepath.Join(t.TempDir(), "dump.json")

			if code := run([]string{"-s", es.start(t), "-x", "src", "-o", out, "--max_retries=1"}); code != 1 {
				t.Fatalf("exit code %d, want 1 for an incomplete dump", code)
			}
		})
	}
}

func TestVersionFlag(t *testing.T) {
	if code := run([]string{"--version"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
}
