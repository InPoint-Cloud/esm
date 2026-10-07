package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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

// checkDocs verifies that each entry is a document, all of them are there and the skipped field is gone
func checkDocs(t *testing.T, docs []string, want int, skipped string) {
	t.Helper()
	if len(docs) != want {
		t.Fatalf("got %d documents, want %d", len(docs), want)
	}
	for _, d := range docs {
		if !strings.Contains(d, `"n":`) {
			t.Fatalf("not a document: %q", d)
		}
		if skipped != "" && strings.Contains(d, `"`+skipped+`"`) {
			t.Fatalf("skipped field %q still present: %q", skipped, d)
		}
	}
}

func values(m map[string]string) []string {
	var out []string
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// captureStdout returns what fn prints to stdout
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	defer func() {
		os.Stdout = stdout
	}()
	fn()
	os.Stdout = stdout
	w.Close()
	return <-done
}

func TestDumpToFile(t *testing.T) {
	es := newFakeES("7.10.2")
	es.addIndex("src", 1000)
	out := filepath.Join(t.TempDir(), "dump.json")

	if code := run([]string{"-s", es.start(t), "-x", "src", "--sliced_scroll_size=4", "-c", "100", "-o", out, "--skip=secret"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	checkDocs(t, readLines(t, out), 1000, "secret")
}

func TestReimportDumpFile(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 100)
	dst := newFakeES("8.11.0")
	dir := t.TempDir()
	dump, out := filepath.Join(dir, "dump.json"), filepath.Join(dir, "out.json")

	if code := run([]string{"-s", src.start(t), "-x", "src", "-o", dump}); code != 0 {
		t.Fatalf("dump exit code %d", code)
	}
	if code := run([]string{"-i", dump, "-o", out, "--skip=keep"}); code != 0 {
		t.Fatalf("reimport to file exit code %d", code)
	}
	checkDocs(t, readLines(t, out), 100, "keep")

	if code := run([]string{"-i", dump, "-d", dst.start(t), "-y", "dst"}); code != 0 {
		t.Fatalf("reimport to es exit code %d", code)
	}
	checkDocs(t, values(dst.docs("dst")), 100, "")
}

func TestMigrateAcrossVersions(t *testing.T) {
	tests := []struct{ src, dst string }{
		{"5.6.16", "6.8.23"},
		{"6.8.23", "7.17.0"},
		{"6.8.23", "8.11.0"},
		{"7.10.2", "8.11.0"},
		{"8.11.0", "9.1.0"},
		{"8.11.0", "7.17.0"},
	}
	for _, tt := range tests {
		t.Run(tt.src+"_to_"+tt.dst, func(t *testing.T) {
			src := newFakeES(tt.src)
			src.addIndex("src", 500)
			dst := newFakeES(tt.dst)

			code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst",
				"--sliced_scroll_size=3", "-c", "50", "-w", "4", "--skip=secret"})
			if code != 0 {
				t.Fatalf("exit code %d", code)
			}
			checkDocs(t, values(dst.docs("dst")), 500, "secret")
		})
	}
}

func TestTypeOverride(t *testing.T) {
	src := newFakeES("5.6.16")
	src.addIndex("src", 10)
	dst := newFakeES("6.8.23")

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "-u", "_doc"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	dst.mu.Lock()
	defer dst.mu.Unlock()
	for id, d := range dst.indices["dst"].docs {
		if d.typ != "_doc" {
			t.Fatalf("doc %s has type %q, want _doc", id, d.typ)
		}
	}
}

func TestRegenerateID(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 20)
	dst := newFakeES("7.10.2")

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--regenerate_id", "--repeat_times=3"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	docs := dst.docs("dst")
	checkDocs(t, values(docs), 60, "")
	if _, ok := docs["0001"]; ok {
		t.Error("source ids were kept")
	}
}

func TestCopySettingsAndMappings(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("8.11.0")

	code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--copy_settings", "--copy_mappings"})
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	settings := dst.settings("dst")
	for key, want := range map[string]string{"number_of_shards": "3", "number_of_replicas": "1", "refresh_interval": "5s"} {
		if fmt.Sprint(settings[key]) != want {
			t.Errorf("setting %s = %v, want %v (settings %v)", key, settings[key], want, settings)
		}
	}
	for _, key := range []string{"uuid", "creation_date", "provided_name", "version"} {
		if _, ok := settings[key]; ok {
			t.Errorf("internal setting %s was copied", key)
		}
	}
	if !strings.Contains(fmt.Sprint(dst.mappings("dst")), "long") {
		t.Errorf("mappings not copied: %v", dst.mappings("dst"))
	}
	checkDocs(t, values(dst.docs("dst")), 10, "")
}

func TestCopyMappingsOnly(t *testing.T) {
	for _, tt := range []struct{ src, dst string }{{"6.8.23", "6.8.23"}, {"7.10.2", "7.10.2"}, {"7.10.2", "8.11.0"}, {"8.11.0", "8.11.0"}} {
		t.Run(tt.src+"_to_"+tt.dst, func(t *testing.T) {
			src := newFakeES(tt.src)
			src.addIndex("src", 10)
			dst := newFakeES(tt.dst)

			if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--copy_mappings"}); code != 0 {
				t.Fatalf("exit code %d", code)
			}
			if !strings.Contains(fmt.Sprint(dst.mappings("dst")), "long") {
				t.Errorf("mappings not copied: %v", dst.mappings("dst"))
			}
			if s := dst.settings("dst"); fmt.Sprint(s["number_of_shards"]) == "3" {
				t.Errorf("settings were copied without --copy_settings: %v", s)
			}
			checkDocs(t, values(dst.docs("dst")), 10, "")
		})
	}
}

func TestShardsOverride(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("7.10.2")

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--copy_settings", "--shards=5"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if got := fmt.Sprint(dst.settings("dst")["number_of_shards"]); got != "5" {
		t.Errorf("number_of_shards = %s, want 5", got)
	}
}

func TestExistingTargetIndexKeepsShards(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("7.10.2")
	dst.addIndex("dst", 0).settings["number_of_shards"] = "7"

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--copy_settings"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	settings := dst.settings("dst")
	if got := fmt.Sprint(settings["number_of_shards"]); got != "7" {
		t.Errorf("number_of_shards of the existing index changed to %s", got)
	}
	// refresh_interval and replicas are restored to the values of the source after the migration
	if fmt.Sprint(settings["refresh_interval"]) != "5s" || fmt.Sprint(settings["number_of_replicas"]) != "1" {
		t.Errorf("settings not restored: %v", settings)
	}
}

func TestRecreateIndex(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("7.10.2")
	dst.addIndex("dst", 5)
	dst.mu.Lock()
	dst.putDoc("dst", "stale", "_doc", `{"n":-1}`)
	dst.mu.Unlock()

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--copy_settings", "--force"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	docs := dst.docs("dst")
	if _, ok := docs["stale"]; ok {
		t.Error("the target index was not recreated")
	}
	checkDocs(t, values(docs), 10, "")
}

func TestMissingSourceIndex(t *testing.T) {
	src := newFakeES("7.10.2")
	dst := newFakeES("7.10.2")

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "missing"}); code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
}

func TestBulkFailures(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("src", 10)
	dst := newFakeES("7.10.2")
	dst.rejectBulk = 400
	failed := filepath.Join(t.TempDir(), "failed.json")

	if code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "src", "-y", "dst", "--failed_output", failed}); code != 1 {
		t.Fatalf("exit code %d, want 1", code)
	}
	// the failed documents can be re-imported with -i
	if lines := readLines(t, failed); len(lines) != 10 {
		t.Errorf("failed output has %d lines, want one for each of the 10 documents", len(lines))
	}
}

func TestSync(t *testing.T) {
	setup := func(version string) (*fakeES, *fakeES) {
		src := newFakeES(version)
		src.addIndex("idx", 30)
		dst := newFakeES(version)
		dst.addIndex("idx", 30)
		dst.mu.Lock()
		typ := dst.defaultType()
		delete(dst.indices["idx"].docs, "0003")     // missing in target
		dst.putDoc("idx", "0007", typ, `{"n":-7}`)  // changed
		dst.putDoc("idx", "0100", typ, `{"n":100}`) // only in target
		dst.putDoc("idx", "0015a", typ, `{"n":15}`) // only in target, between source ids
		dst.mu.Unlock()
		return src, dst
	}
	args := func(src, dst *fakeES, extra ...string) []string {
		return append([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "idx", "--sync", "-c", "7"}, extra...)
	}

	for _, version := range []string{"5.6.16", "6.8.23", "7.10.2", "8.11.0"} {
		t.Run("with delete "+version, func(t *testing.T) {
			src, dst := setup(version)
			if code := run(args(src, dst, "--enable_delete")); code != 0 {
				t.Fatalf("exit code %d", code)
			}
			want, got := src.docs("idx"), dst.docs("idx")
			if len(got) != len(want) {
				t.Errorf("target has %d documents, want %d", len(got), len(want))
			}
			for id, source := range want {
				if got[id] != source {
					t.Errorf("doc %s = %s, want %s", id, got[id], source)
				}
			}
		})
	}

	t.Run("without delete", func(t *testing.T) {
		src, dst := setup("7.10.2")
		if code := run(args(src, dst)); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		got := dst.docs("idx")
		if _, ok := got["0100"]; !ok {
			t.Error("extra target doc deleted without --enable_delete")
		}
		if got["0003"] == "" || got["0007"] != src.docs("idx")["0007"] {
			t.Error("missing or changed docs not synced")
		}
	})

	t.Run("dry", func(t *testing.T) {
		src, dst := setup("7.10.2")
		before := dst.docs("idx")
		if code := run(args(src, dst, "--dry", "--enable_delete")); code != 0 {
			t.Fatalf("exit code %d", code)
		}
		if after := dst.docs("idx"); fmt.Sprint(after) != fmt.Sprint(before) {
			t.Error("--dry changed the target")
		}
	})

	t.Run("missing source index", func(t *testing.T) {
		src, dst := setup("7.10.2")
		code := run([]string{"-s", src.start(t), "-d", dst.start(t), "-x", "missing", "--sync"})
		if code != 1 {
			t.Fatalf("exit code %d, want 1", code)
		}
	})
}

func TestDiffCounts(t *testing.T) {
	src := newFakeES("7.10.2")
	src.addIndex("same", 5)
	src.addIndex("diff", 5)
	src.addIndex("missing", 5)
	dst := newFakeES("8.11.0")
	dst.addIndex("same", 5)
	dst.addIndex("diff", 3)

	var code int
	out := captureStdout(t, func() {
		code = run([]string{"-s", src.start(t), "-d", dst.start(t), "--diff_counts"})
	})
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	for _, want := range []string{"same", "index diff : source=5, destination=3", "missing"} {
		if !strings.Contains(out, want) {
			t.Errorf("output misses %q:\n%s", want, out)
		}
	}
}

func TestScrollErrorFails(t *testing.T) {
	for _, version := range []string{"5.6.16", "6.8.23", "7.10.2", "8.11.0"} {
		t.Run(version, func(t *testing.T) {
			es := newFakeES(version)
			es.addIndex("src", 10)
			es.scrollError = 404
			url := es.start(t)

			if code := run([]string{"-s", url, "-x", "src", "-c", "5", "-o", filepath.Join(t.TempDir(), "dump.json"), "--max_retries=1"}); code != 1 {
				t.Fatalf("dump exit code %d, want 1 for an incomplete dump", code)
			}
			dst := newFakeES("8.11.0")
			if code := run([]string{"-s", url, "-d", dst.start(t), "-x", "src", "-y", "dst", "-c", "5", "--max_retries=1"}); code != 1 {
				t.Fatalf("migration exit code %d, want 1", code)
			}
		})
	}
}

func TestInvalidArguments(t *testing.T) {
	tests := map[string][]string{
		"no input":       {"-o", "out.json"},
		"no output":      {"-s", "http://127.0.0.1:1"},
		"unknown flag":   {"--nope"},
		"bad log level":  {"-s", "http://127.0.0.1:1", "-o", "x", "--log", "loud"},
		"unreachable es": {"-s", "http://127.0.0.1:1", "-x", "src", "-o", filepath.Join(t.TempDir(), "x")},
	}
	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			if code := run(args); code != 1 {
				t.Fatalf("exit code %d, want 1", code)
			}
		})
	}
}

func TestTLSVerification(t *testing.T) {
	es := newFakeES("7.10.2")
	es.addIndex("src", 10)
	es.tls = true
	url := es.start(t)

	out := filepath.Join(t.TempDir(), "dump.json")
	if code := run([]string{"-s", url, "-x", "src", "-o", out}); code != 1 {
		t.Fatalf("exit code %d, want 1 for a self-signed certificate", code)
	}
	if code := run([]string{"-s", url, "-x", "src", "-o", out, "--insecure"}); code != 0 {
		t.Fatalf("exit code %d with --insecure", code)
	}
	checkDocs(t, readLines(t, out), 10, "")
}

func TestLogFile(t *testing.T) {
	es := newFakeES("7.10.2")
	es.addIndex("src", 10)
	dir := t.TempDir()
	logFile := filepath.Join(dir, "esm.log")

	if code := run([]string{"-s", es.start(t), "-x", "src", "-o", filepath.Join(dir, "dump.json"), "--log_file", logFile}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	b, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "source es version: 7.10.2") {
		t.Errorf("log file misses the info output:\n%s", b)
	}
}

func TestVersionFlag(t *testing.T) {
	var code int
	out := captureStdout(t, func() {
		code = run([]string{"--version"})
	})
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if !strings.HasPrefix(out, "esm dev") {
		t.Errorf("unexpected version output %q", out)
	}
}
