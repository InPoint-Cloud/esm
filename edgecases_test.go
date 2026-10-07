package main

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
