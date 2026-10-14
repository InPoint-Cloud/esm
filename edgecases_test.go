package main

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fastRetries shortens the waits between retries and health checks for the duration of a test
func fastRetries(t *testing.T) {
	t.Helper()
	retry, wait := retryBaseDelay, clusterWaitInterval
	retryBaseDelay, clusterWaitInterval = time.Millisecond, time.Millisecond
	t.Cleanup(func() {
		retryBaseDelay, clusterWaitInterval = retry, wait
	})
}

func TestOldVersions(t *testing.T) {
	for _, version := range []string{"1.7.6", "2.4.6"} {
		t.Run(version, func(t *testing.T) {
			src := newFakeES(version)
			src.addIndex("src", 100)
			dst := newFakeES(version)

			code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "-c", "30", "--copy_mappings"})
			if code != 0 {
				t.Fatalf("exit code %d", code)
			}
			checkDocs(t, values(dst.docs("dst")), 100, "")
			if !strings.Contains(fmt.Sprint(dst.mappings("dst")), "long") {
				t.Errorf("mappings not copied: %v", dst.mappings("dst"))
			}
		})
	}
}

func TestIndexPatterns(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("logs-1", 10)
	src.addIndex("logs-2", 20)
	src.addIndex("other", 5)

	t.Run("wildcard", func(t *testing.T) {
		dst := newFakeES("8.11.0")
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "logs-*", "--copy_settings"}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		if n1, n2, n3 := len(dst.docs("logs-1")), len(dst.docs("logs-2")), len(dst.docs("other")); n1 != 10 || n2 != 20 || n3 != 0 {
			t.Errorf("got %d, %d and %d documents, want 10, 20 and 0", n1, n2, n3)
		}
	})

	t.Run("all", func(t *testing.T) {
		dst := newFakeES("8.11.0")
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "_all", "--copy_mappings"}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		if n := len(dst.docs("other")); n != 5 {
			t.Errorf("index other has %d documents, want 5", n)
		}
	})

	t.Run("merge into one index", func(t *testing.T) {
		dst := newFakeES("8.11.0")
		// both indexes have docs with the same ids, they overwrite each other and the count check fails
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "logs-1,logs-2", "-y", "logs"}); code != 1 {
			t.Fatalf("exit code %d, want 1 for the count mismatch", code)
		}
		if n := len(dst.docs("logs")); n != 20 {
			t.Errorf("index logs has %d documents, want 20", n)
		}

		// with new ids all of them are kept
		dst = newFakeES("8.11.0")
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "logs-1,logs-2", "-y", "logs", "--regenerate_id"}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		if n := len(dst.docs("logs")); n != 30 {
			t.Errorf("index logs has %d documents, want 30", n)
		}
	})
}

func TestAuthentication(t *testing.T) {
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("elastic:secret"))

	t.Run("basic auth", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 10)
		src.auth = basic
		dst := newFakeES("8.11.0")
		dst.auth = basic
		code := run([]string{"-s", src.start(t), "-m", "elastic:secret", "-d", dst.start(t), "-n", "elastic:secret", "-x", "src", "-y", "dst"})
		if code != 0 {
			t.Fatalf("exit code %d", code)
		}
		checkDocs(t, values(dst.docs("dst")), 10, "")
	})

	t.Run("api key", func(t *testing.T) {
		src := newFakeES("8.11.0")
		src.addIndex("src", 10)
		src.auth = "ApiKey c3JjOmtleQ=="
		dst := newFakeES("9.1.0")
		dst.auth = "ApiKey ZHN0OmtleQ=="
		code := run([]string{"-s", src.start(t), "--source_api_key", "c3JjOmtleQ==", "-d", dst.start(t),
			"--dest_api_key", "ZHN0OmtleQ==", "-x", "src", "-y", "dst", "--copy_settings", "--copy_mappings"})
		if code != 0 {
			t.Fatalf("exit code %d", code)
		}
		checkDocs(t, values(dst.docs("dst")), 10, "")
	})

	t.Run("wrong password", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 10)
		src.auth = basic
		code := run([]string{"-s", src.start(t), "-m", "elastic:wrong", "-x", "src", "-o", filepath.Join(t.TempDir(), "x")})
		if code != 1 {
			t.Fatalf("exit code %d, want 1", code)
		}
	})
}

// forwardProxy is an http proxy counting the requests it forwards
func forwardProxy(t *testing.T) (string, *int64) {
	t.Helper()
	var count int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&count, 1)
		r.RequestURI = ""
		resp, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		buf := make([]byte, 32*1024)
		for {
			n, err := resp.Body.Read(buf)
			w.Write(buf[:n])
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &count
}

func TestProxies(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("8.11.0")
	srcProxy, srcCount := forwardProxy(t)
	dstProxy, dstCount := forwardProxy(t)

	code := run([]string{"-s", src.start(t), "--source_proxy", srcProxy, "-d", dst.start(t), "--dest_proxy", dstProxy, "-x", "src", "-y", "dst"})
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	checkDocs(t, values(dst.docs("dst")), 10, "")
	if atomic.LoadInt64(srcCount) == 0 || atomic.LoadInt64(dstCount) == 0 {
		t.Errorf("requests through the source proxy: %d, the target proxy: %d", *srcCount, *dstCount)
	}
}

func TestRetries(t *testing.T) {
	fastRetries(t)

	t.Run("scroll", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 20)
		src.scrollError, src.scrollFails = 503, 2
		out := filepath.Join(t.TempDir(), "dump.json")
		if code := run([]string{"-s", src.start(t), "-x", "src", "-c", "5", "-o", out}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		checkDocs(t, readLines(t, out), 20, "")
	})

	t.Run("bulk request", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 20)
		dst := newFakeES("7.10.2")
		dst.bulkStatus = []int{503, 502}
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst"}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		checkDocs(t, values(dst.docs("dst")), 20, "")
	})

	t.Run("rejected documents", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 20)
		dst := newFakeES("7.10.2")
		dst.itemStatus = []int{0, 429, 0, 429}
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst"}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		checkDocs(t, values(dst.docs("dst")), 20, "")
	})

	t.Run("bulk request retries exhausted", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 20)
		dst := newFakeES("7.10.2")
		dst.bulkStatus = []int{503, 503, 503}
		failed := filepath.Join(t.TempDir(), "failed.json")
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--max_retries=2", "--failed_output", failed}); code != 1 {
			t.Fatalf("exit code %d, want 1", code)
		}
		if lines := readLines(t, failed); len(lines) != 20 {
			t.Errorf("failed output has %d lines, want 20", len(lines))
		}
	})

	t.Run("bad request is not retried", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 20)
		dst := newFakeES("7.10.2")
		dst.bulkStatus = []int{400}
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst"}); code != 1 {
			t.Fatalf("exit code %d, want 1", code)
		}
		if n := len(dst.docs("dst")); n != 0 {
			t.Errorf("%d documents indexed after a failed bulk request", n)
		}
	})
}

func TestManyFailedDocuments(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 50)
	dst := newFakeES("7.10.2")
	dst.rejectBulk = 400
	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst"}); code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
}

func TestWaitForGreen(t *testing.T) {
	fastRetries(t)
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	src.health, src.healthCheck = "yellow", 3
	dst := newFakeES("7.10.2")
	dst.addIndex("dst", 0)
	dst.health, dst.healthCheck = "red", 2

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--green"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if src.healthCheck != 0 || dst.healthCheck != 0 {
		t.Error("did not wait for green clusters")
	}
	checkDocs(t, values(dst.docs("dst")), 10, "")
}

func TestOnlyMeta(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("8.11.0")

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--copy_settings", "--copy_mappings", "--only_meta"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if dst.settings("dst") == nil {
		t.Fatal("index not created")
	}
	if n := len(dst.docs("dst")); n != 0 {
		t.Errorf("%d documents copied with --only_meta", n)
	}
}

func TestRefreshAndAnalysisSettings(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 10).settings["analysis"] = map[string]interface{}{
		"filter": map[string]interface{}{"grams": map[string]interface{}{"type": "nGram"}},
	}
	dst := newFakeES("8.11.0")
	dst.addIndex("dst", 0)

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--copy_settings", "--refresh"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	// analysis settings are updated on the closed index, nGram was renamed in 8.x
	if got := fmt.Sprint(dst.settings("dst")["analysis"]); !strings.Contains(got, "ngram") {
		t.Errorf("analysis settings = %s", got)
	}
}

func TestDumpOptions(t *testing.T) {
	es := newFakeES("7.10.2")
	es.addIndex("src", 10)
	url := es.start(t)
	out := filepath.Join(t.TempDir(), "dump.json")

	if code := run([]string{"-s", url, "-x", "src", "-o", out, "--fields", "n"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if code := run([]string{"-s", url, "-x", "src", "-o", out, "--truncate_output", "-q", "n:1"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	// the fake ignores queries, the second dump replaced the first one
	checkDocs(t, readLines(t, out), 10, "")

	if code := run([]string{"-s", url, "-x", "src", "-o", out}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	checkDocs(t, readLines(t, out), 20, "")
}

func TestUnwritableOutput(t *testing.T) {
	es := newFakeES("7.10.2")
	es.addIndex("src", 10)
	url := es.start(t)
	dir := t.TempDir()

	if code := run([]string{"-i", filepath.Join(dir, "missing.json"), "-o", filepath.Join(dir, "out.json")}); code != 1 {
		t.Errorf("missing input file: exit code %d, want 1", code)
	}
	if code := run([]string{"-s", url, "-x", "src", "-d", url, "-y", "dst", "--failed_output", filepath.Join(dir, "no", "such", "dir")}); code != 1 {
		t.Errorf("unwritable failed output: exit code %d, want 1", code)
	}
	if code := run([]string{"-s", url, "-x", "src", "-o", filepath.Join(dir, "out.json"), "--log_file", filepath.Join(dir, "no", "such", "dir")}); code != 1 {
		t.Errorf("unwritable log file: exit code %d, want 1", code)
	}
}

func TestDiffCountsUnreachable(t *testing.T) {
	src := newFakeES("7.10.2")
	if code := run([]string{"-s", src.start(t), "-d", "http://127.0.0.1:1", "--diff_counts"}); code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
	if code := run([]string{"-s", "http://127.0.0.1:1", "-d", src.start(t), "--sync", "-x", "idx"}); code != 1 {
		t.Fatalf("sync exit code %d, want 1", code)
	}
}

func TestScrollSort(t *testing.T) {
	tests := []struct {
		name    string
		version string
		args    []string
		want    string
	}{
		{"migration sorts by _doc", "7.10.2", nil, "_doc"},
		{"8.x too", "8.11.0", nil, "_doc"},
		{"no sort before 5.x", "2.4.6", nil, ""},
		{"explicit sort", "7.10.2", []string{"--sort", "n"}, "n"},
		{"sync sorts by _id", "7.10.2", []string{"--sync", "-y", "src"}, "_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := newFakeES(tt.version)
			src.addIndex("src", 5)
			dst := newFakeES(tt.version)
			dst.addIndex("src", 5)
			args := append([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-q", "n:*"}, tt.args...)
			if code := run(args); code != 0 {
				t.Fatalf("exit code %d", code)
			}
			src.mu.Lock()
			defer src.mu.Unlock()
			if len(src.sorts) == 0 || src.sorts[0] != tt.want {
				t.Errorf("scroll sorted by %q, want %q", src.sorts, tt.want)
			}
		})
	}
}

func TestInputFileFormatting(t *testing.T) {
	dir := t.TempDir()
	in, out := filepath.Join(dir, "in.json"), filepath.Join(dir, "out.json")
	// a blank line, an invalid line and no newline at the end
	content := `{"_index":"i","_id":"1","_source":{"n":1}}` + "\n\n" + "not json\n" + `{"_index":"i","_id":"2","_source":{"n":2}}`
	if err := os.WriteFile(in, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"-i", in, "-o", out}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	checkDocs(t, readLines(t, out), 2, "")
	if n, err := countLines(in); err != nil || n != 3 {
		t.Errorf("countLines = %d, %v, want 3", n, err)
	}
}

func TestBulkTooLarge(t *testing.T) {
	fastRetries(t)

	t.Run("rejected request is split", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 20)
		dst := newFakeES("7.10.2")
		dst.maxBulkBytes = 300
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst"}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		checkDocs(t, values(dst.docs("dst")), 20, "")
	})

	t.Run("sync splits rejected request", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("idx", 30)
		dst := newFakeES("7.10.2")
		dst.addIndex("idx", 0)
		dst.maxBulkBytes = 300
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "idx", "--sync", "-c", "50"}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		if got := len(dst.docs("idx")); got != 30 {
			t.Errorf("target has %d documents, want 30", got)
		}
	})

	t.Run("sync uses bulk size", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("idx", 0)
		big := strings.Repeat("x", 100*1024)
		src.mu.Lock()
		for i := 0; i < 30; i++ {
			src.putDoc("idx", fmt.Sprintf("%04d", i), "_doc", fmt.Sprintf(`{"n":%d,"text":"%s"}`, i, big))
		}
		src.mu.Unlock()
		dst := newFakeES("7.10.2")
		dst.addIndex("idx", 0)
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "idx", "--sync", "-c", "50", "-b", "1"}); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		if got := len(dst.docs("idx")); got != 30 {
			t.Errorf("target has %d documents, want 30", got)
		}
		if dst.largestBulk > 1024*1024 {
			t.Errorf("largest bulk request has %d bytes, want at most 1mb", dst.largestBulk)
		}
		if dst.bulkRequests < 3 {
			t.Errorf("%d bulk requests, want at least 3", dst.bulkRequests)
		}
	})

	t.Run("single document too large", func(t *testing.T) {
		src := newFakeES("7.10.2")
		src.addIndex("src", 20)
		dst := newFakeES("7.10.2")
		dst.maxBulkBytes = 10
		failed := filepath.Join(t.TempDir(), "failed.json")
		if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--max_retries=1", "--failed_output", failed}); code != 1 {
			t.Fatalf("exit code %d, want 1", code)
		}
		if lines := readLines(t, failed); len(lines) != 20 {
			t.Errorf("failed output has %d lines, want 20", len(lines))
		}
	})
}

func TestDocumentOverIndexingPressureLimit(t *testing.T) {
	fastRetries(t)
	src := newFakeES("8.15.0")
	src.addIndex("idx", 20)
	big := fmt.Sprintf(`{"n":20,"text":"%s"}`, strings.Repeat("x", 200*1024))
	src.mu.Lock()
	src.putDoc("idx", "0020", "_doc", big)
	src.mu.Unlock()
	dst := newFakeES("8.15.0")
	dst.addIndex("idx", 0)
	dst.indexingPressureLimit = 100 * 1024

	var bigRequests int32
	dstURL := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_bulk" && r.ContentLength > int64(len(big)) && r.ContentLength < int64(len(big))+60 {
			atomic.AddInt32(&bigRequests, 1)
		}
		dst.handle(w, r)
	}))
	t.Cleanup(dstURL.Close)

	failed := filepath.Join(t.TempDir(), "failed.json")
	if code := run([]string{"-s", src.start(t), "-d", dstURL.URL, "-x", "idx", "--sync", "-c", "300", "--failed_output", failed}); code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
	if got := len(dst.docs("idx")); got != 20 {
		t.Errorf("target has %d documents, want the 20 small ones", got)
	}
	// the document alone is over the limit, it is not retried
	if n := atomic.LoadInt32(&bigRequests); n != 1 {
		t.Errorf("the large document was sent %d times on its own, want 1", n)
	}
	// the line is too long for readLines
	out, err := os.ReadFile(failed)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSpace(string(out)), "\n"); len(lines) != 1 || !strings.Contains(lines[0], `"_id":"0020"`) {
		t.Errorf("failed output = %d lines, want the large document", len(lines))
	}
}

func TestClassifyRejection(t *testing.T) {
	pressure := func(op string, size, limit int) map[string]interface{} {
		return map[string]interface{}{"type": "es_rejected_execution_exception", "reason": fmt.Sprintf(
			"rejected execution of %s operation [coordinating_and_primary_bytes=0, replica_bytes=0, all_bytes=0, "+
				"%s_operation_bytes=%d, max_coordinating_and_primary_bytes=%d]", op, op, size, limit)}
	}
	for _, tc := range []struct {
		name      string
		status    int
		err       interface{}
		permanent bool
		hint      string
	}{
		{"too large for http", 413, nil, true, "http.max_content_length"},
		{"coordinating over limit", 429, pressure("coordinating", 60, 50), true, "indexing_pressure.memory.limit"},
		{"coordinating busy", 429, pressure("coordinating", 40, 50), false, "indexing_pressure.memory.limit"},
		{"primary over limit", 429, pressure("primary", 60, 50), true, "indexing_pressure.memory.limit"},
		{"replica over limit", 429, map[string]interface{}{"type": "es_rejected_execution_exception",
			"reason": "rejected execution of replica operation [replica_bytes=0, replica_operation_bytes=80, max_replica_bytes=75]"},
			true, "indexing_pressure.memory.limit"},
		{"primary with its coordinating bytes over limit", 429, map[string]interface{}{"type": "es_rejected_execution_exception",
			"reason": "rejected execution of primary operation [coordinating_and_primary_bytes=51748375, replica_bytes=0, " +
				"all_bytes=51748375, primary_operation_bytes=206991772, max_primary_bytes=214748364]"}, true, "indexing_pressure.memory.limit"},
		{"primary busy", 429, map[string]interface{}{"type": "es_rejected_execution_exception",
			"reason": "rejected execution of primary operation [coordinating_and_primary_bytes=200000000, replica_bytes=0, " +
				"all_bytes=200000000, primary_operation_bytes=1000, max_primary_bytes=214748364]"}, false, "indexing_pressure.memory.limit"},
		{"breaker transient", 429, map[string]interface{}{"type": "circuit_breaking_exception", "durability": "TRANSIENT"}, false, "indices.breaker"},
		{"breaker permanent", 429, map[string]interface{}{"type": "circuit_breaking_exception", "durability": "PERMANENT"}, true, "indices.breaker"},
		{"write queue full", 429, map[string]interface{}{"type": "es_rejected_execution_exception",
			"reason": "rejected execution of TimedRunnable on EsThreadPoolExecutor[name = node/write, queue capacity = 10000]"}, false, ""},
		{"mapping error", 400, map[string]interface{}{"type": "mapper_parsing_exception"}, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rej := classifyRejection(tc.status, tc.err, 51748375)
			if rej.permanent != tc.permanent {
				t.Errorf("permanent = %t, want %t", rej.permanent, tc.permanent)
			}
			if tc.hint == "" && rej.hint != "" || !strings.Contains(rej.hint, tc.hint) {
				t.Errorf("hint = %q, want it to name %q", rej.hint, tc.hint)
			}
		})
	}
}

func TestInterruptRestoresSettings(t *testing.T) {
	self, err := os.FindProcess(os.Getpid())
	if err != nil || runtime.GOOS == "windows" {
		t.Skip("can not send SIGINT")
	}
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("8.11.0")

	type exit struct {
		code     int
		settings map[string]interface{}
	}
	exited := make(chan exit, 1)
	orig := exitFunc
	exitFunc = func(code int) {
		// copied before the migration goes on, the test does not exit
		settings := map[string]interface{}{}
		for k, v := range dst.settings("dst") {
			settings[k] = v
		}
		exited <- exit{code, settings}
	}
	t.Cleanup(func() { exitFunc = orig })

	var once sync.Once
	var got *exit
	dstURL := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_bulk" {
			// interrupted while the documents are copied, after the index was created
			once.Do(func() {
				self.Signal(os.Interrupt)
				select {
				case e := <-exited:
					got = &e
				case <-time.After(5 * time.Second):
				}
			})
		}
		dst.handle(w, r)
	}))
	t.Cleanup(dstURL.Close)

	code := run([]string{"-s", src.start(t), "-d", dstURL.URL, "-x", "src", "-y", "dst", "--copy_settings"})
	if got == nil {
		t.Fatalf("not interrupted, exit code %d", code)
	}
	if got.code != 130 {
		t.Errorf("exit code %d, want 130", got.code)
	}
	if fmt.Sprint(got.settings["refresh_interval"]) != "5s" || fmt.Sprint(got.settings["number_of_replicas"]) != "1" {
		t.Errorf("settings not restored on interrupt: %v", got.settings)
	}
}

func TestDumpOutputErrors(t *testing.T) {
	es := newFakeES("7.10.2")
	es.addIndex("src", 10)
	url := es.start(t)
	dir := t.TempDir()

	done := make(chan int)
	go func() {
		done <- run([]string{"-s", url, "-x", "src", "-o", filepath.Join(dir, "no", "such", "dir", "out.json")})
	}()
	select {
	case code := <-done:
		if code != 1 {
			t.Errorf("unwritable output file: exit code %d, want 1", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unwritable output file: esm hangs")
	}

	if _, err := os.Stat("/dev/full"); err == nil {
		if code := run([]string{"-s", url, "-x", "src", "-o", "/dev/full"}); code != 1 {
			t.Errorf("full disk: exit code %d, want 1", code)
		}
	}
}

func TestRepeatTimesWithTruncateOutput(t *testing.T) {
	es := newFakeES("7.10.2")
	es.addIndex("src", 10)
	out := filepath.Join(t.TempDir(), "dump.json")
	os.WriteFile(out, []byte(`{"_index":"old","_id":"1","_source":{}}`+"\n"), 0644)

	if code := run([]string{"-s", es.start(t), "-x", "src", "-o", out, "--truncate_output", "--repeat_times", "3"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if n := len(readLines(t, out)); n != 30 {
		t.Errorf("%d lines, want the 30 documents of all rounds", n)
	}
}

func TestSyncIgnoresSlicedScroll(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("idx", 100)
	dst := newFakeES("7.10.2")
	dst.addIndex("idx", 0)
	for i := 0; i < 100; i += 2 {
		dst.putDoc("idx", fmt.Sprintf("%04d", i), "_doc", `{"n":-1}`)
	}
	dst.putDoc("idx", "9999", "_doc", `{"n":-1}`)

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "idx", "--sync", "--enable_delete",
		"--sliced_scroll_size", "4", "-c", "10"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if got, want := dst.docs("idx"), src.docs("idx"); !reflect.DeepEqual(got, want) {
		t.Errorf("target has %d documents, source %d", len(got), len(want))
	}
}

func TestWaitForRedWithoutGreen(t *testing.T) {
	fastRetries(t)
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	src.health, src.healthCheck = "yellow", 3
	dst := newFakeES("7.10.2")
	dst.addIndex("dst", 0)
	dst.health, dst.healthCheck = "red", 2

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if src.healthCheck != 2 {
		t.Errorf("waited for the yellow source, %d health checks left", src.healthCheck)
	}
	if dst.healthCheck != 0 {
		t.Error("did not wait for the red target")
	}
	checkDocs(t, values(dst.docs("dst")), 10, "")
}

// runWithin fails the test if esm does not finish in time, ie: because it waits for a cluster forever
func runWithin(t *testing.T, args []string) int {
	t.Helper()
	done := make(chan int, 1)
	go func() { done <- run(args) }()
	select {
	case code := <-done:
		return code
	case <-time.After(10 * time.Second):
		t.Fatal("esm is still waiting after 10s")
		return -1
	}
}

func TestRedClusterWithGreenIndexes(t *testing.T) {
	fastRetries(t)
	for _, targetExists := range []bool{false, true} {
		t.Run(fmt.Sprintf("target exists %v", targetExists), func(t *testing.T) {
			src := newFakeES("7.10.2")
			src.addIndex("src", 10)
			src.addIndex("broken", 0)
			src.indexHealth = map[string]string{"broken": "red"}
			dst := newFakeES("7.10.2")
			dst.addIndex("other", 0)
			dst.indexHealth = map[string]string{"other": "red"}
			if targetExists {
				dst.addIndex("dst", 0)
			}

			if code := runWithin(t, []string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--green"}); code != 0 {
				t.Fatalf("exit code %d", code)
			}
			checkDocs(t, values(dst.docs("dst")), 10, "")
			src.mu.Lock()
			defer src.mu.Unlock()
			dst.mu.Lock()
			defer dst.mu.Unlock()
			if want := []string{"/_cluster/health/src"}; !reflect.DeepEqual(src.healthPaths, want) {
				t.Errorf("source health requests %v, want %v", src.healthPaths, want)
			}
			if want := []string{"/_cluster/health/dst"}; !reflect.DeepEqual(dst.healthPaths, want) {
				t.Errorf("target health requests %v, want %v", dst.healthPaths, want)
			}
		})
	}
}

func TestWaitForRedIndex(t *testing.T) {
	fastRetries(t)
	src := newFakeES("7.10.2")
	src.addIndex("a", 5)
	src.addIndex("b", 5)
	dst := newFakeES("7.10.2")
	dst.addIndex("b", 0)
	srcURL, dstURL := src.start(t), dst.start(t)

	// the target index turns green while esm waits for it, the migration must not start before
	dst.mu.Lock()
	dst.indexHealth = map[string]string{"b": "red"}
	dst.mu.Unlock()
	go func() {
		time.Sleep(50 * time.Millisecond)
		dst.mu.Lock()
		dst.indexHealth = nil
		dst.mu.Unlock()
	}()

	// without --dest_index the source names are checked on the target, "a" does not exist there yet
	if code := runWithin(t, []string{"-s", srcURL, "-d", dstURL, "-x", "a,b"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	dst.mu.Lock()
	defer dst.mu.Unlock()
	if len(dst.healthPaths) < 3 {
		t.Errorf("did not wait for the red target index: %v", dst.healthPaths)
	}
	for _, p := range dst.healthPaths {
		if p != "/_cluster/health/a" && p != "/_cluster/health/b" {
			t.Errorf("unexpected health request %s", p)
		}
	}
}

func TestClusterReady(t *testing.T) {
	es := newFakeES("7.10.2")
	es.addIndex("green", 0)
	es.addIndex("yellow", 0)
	es.addIndex("red", 0)
	es.indexHealth = map[string]string{"yellow": "yellow", "red": "red"}
	api := &ESAPIV0{Host: es.start(t)}

	for _, tc := range []struct {
		indexes, status, index string
		green, ready           bool
	}{
		{"green", "green", "green", true, true},
		{"green,yellow", "yellow", "yellow", false, true},
		{"green,yellow", "yellow", "yellow", true, false},
		{"green, red ,yellow", "red", "red", false, false},
		{"missing", "missing", "", true, true},
		{"missing,yellow", "yellow", "yellow", true, false},
		{"gr*", "green", "gr*", true, true},
		{"", "red", "", false, false},
		{"_all", "red", "_all", false, false},
	} {
		m := &Migrator{Config: &Config{WaitForGreen: tc.green}}
		health, ready := m.ClusterReady(api, tc.indexes)
		if health.Status != tc.status || health.Index != tc.index || ready != tc.ready {
			t.Errorf("%q --green=%v: %s %q ready %v, want %s %q ready %v", tc.indexes, tc.green,
				health.Status, health.Index, ready, tc.status, tc.index, tc.ready)
		}
	}
}

func TestRequestTimeout(t *testing.T) {
	fastRetries(t)
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("7.10.2")

	var hung int32
	dstURL := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// the first bulk request hangs, the retry succeeds
		if r.URL.Path == "/_bulk" && atomic.AddInt32(&hung, 1) == 1 {
			time.Sleep(time.Second)
			return
		}
		dst.handle(w, r)
	}))
	t.Cleanup(dstURL.Close)

	start := time.Now()
	if code := run([]string{"-s", src.start(t), "-d", dstURL.URL, "-x", "src", "-y", "dst", "--request_timeout", "300ms"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if d := time.Since(start); d > 900*time.Millisecond {
		t.Errorf("took %s, the hanging request was not timed out", d)
	}
	checkDocs(t, values(dst.docs("dst")), 10, "")
}
