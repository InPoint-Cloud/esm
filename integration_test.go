//go:build integration

// Integration tests against real elasticsearch clusters, started by scripts/integration-test.sh:
//
//	ESM_IT_SOURCE        url of the source cluster, ie: http://127.0.0.1:19200
//	ESM_IT_TARGET        url of the target cluster
//	ESM_IT_SOURCE_AUTH   optional user:password of the source
//	ESM_IT_TARGET_AUTH   optional user:password of the target, a https target is expected to use a self-signed certificate
package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const itDocs = 2000

// itCluster is a real cluster, accessed directly to prepare and verify the tests
type itCluster struct {
	url, auth string
	major     int
}

var itClient = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}

func newITCluster(t *testing.T, urlVar, authVar string) *itCluster {
	t.Helper()
	c := &itCluster{url: os.Getenv(urlVar), auth: os.Getenv(authVar)}
	if c.url == "" {
		t.Skipf("%s is not set, run scripts/integration-test.sh", urlVar)
	}
	var info struct {
		Version struct{ Number string } `json:"version"`
	}
	c.do(t, "GET", "/", "", &info)
	fmt.Sscanf(info.Version.Number, "%d.", &c.major)
	return c
}

// do sends a request and decodes the json response into out, it fails the test on errors
func (c *itCluster) do(t *testing.T, method, path, body string, out interface{}) {
	t.Helper()
	status, resp := c.request(t, method, path, body)
	if status >= 300 {
		t.Fatalf("%s %s: %d %s", method, path, status, resp)
	}
	if out != nil {
		if err := json.Unmarshal(resp, out); err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
	}
}

func (c *itCluster) request(t *testing.T, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, c.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if user, pass, ok := strings.Cut(c.auth, ":"); ok {
		req.SetBasicAuth(user, pass)
	}
	resp, err := itClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (c *itCluster) deleteIndexes(t *testing.T) {
	t.Helper()
	for _, name := range []string{"esm-it-src", "esm-it-dst", "esm-it-reimport"} {
		c.request(t, "DELETE", "/"+name, "")
	}
}

func (c *itCluster) count(t *testing.T, index string) int {
	t.Helper()
	c.do(t, "POST", "/"+index+"/_refresh", "", nil)
	var r struct{ Count int }
	c.do(t, "GET", "/"+index+"/_count", "", &r)
	return r.Count
}

// source returns the _source of a document, or "" if it does not exist
func (c *itCluster) source(t *testing.T, index, id string) string {
	t.Helper()
	status, body := c.request(t, "GET", "/"+index+"/_doc/"+id, "")
	if status == 404 {
		return ""
	}
	var doc struct {
		Source json.RawMessage `json:"_source"`
	}
	if err := json.Unmarshal(body, &doc); err != nil || status != 200 {
		t.Fatalf("get %s/%s: %d %s", index, id, status, body)
	}
	return string(doc.Source)
}

// enableIdSort allows sorting on _id, --sync needs it on 8.x+
func (c *itCluster) enableIdSort(t *testing.T) {
	t.Helper()
	if c.major >= 8 {
		c.do(t, "PUT", "/_cluster/settings", `{"persistent":{"indices.id_field_data.enabled":true}}`, nil)
	}
}

func (c *itCluster) args(prefix string) []string {
	args := []string{"-" + prefix, c.url}
	if c.auth != "" {
		authFlag := map[string]string{"s": "-m", "d": "-n"}[prefix]
		args = append(args, authFlag, c.auth)
	}
	if strings.HasPrefix(c.url, "https") {
		args = append(args, "--insecure")
	}
	return args
}

func TestIntegration(t *testing.T) {
	src := newITCluster(t, "ESM_IT_SOURCE", "ESM_IT_SOURCE_AUTH")
	dst := newITCluster(t, "ESM_IT_TARGET", "ESM_IT_TARGET_AUTH")
	t.Logf("source %d.x, target %d.x", src.major, dst.major)
	src.deleteIndexes(t)
	dst.deleteIndexes(t)
	t.Cleanup(func() {
		src.deleteIndexes(t)
		dst.deleteIndexes(t)
	})

	// source index with a mapping, 2 shards and documents with zero padded ids, so _id sorts like numbers
	src.do(t, "PUT", "/esm-it-src", `{
		"settings": {"index": {"number_of_shards": 2, "number_of_replicas": 0}},
		"mappings": {"properties": {"n": {"type": "long"}, "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}}}}
	}`, nil)
	var bulk strings.Builder
	for i := 0; i < itDocs; i++ {
		fmt.Fprintf(&bulk, `{"index":{"_index":"esm-it-src","_id":"%05d"}}`+"\n", i)
		fmt.Fprintf(&bulk, `{"n":%d,"title":"document %d","secret":"x"}`+"\n", i, i)
	}
	var bulkResp struct{ Errors bool }
	src.do(t, "POST", "/_bulk?refresh=true", bulk.String(), &bulkResp)
	if bulkResp.Errors {
		t.Fatal("seeding the source failed")
	}

	esm := func(t *testing.T, want int, args ...string) {
		t.Helper()
		if code := run(args); code != want {
			t.Fatalf("esm %s: exit code %d, want %d", strings.Join(args, " "), code, want)
		}
	}
	srcArgs, dstArgs := src.args("s"), dst.args("d")

	t.Run("migrate with settings and mappings", func(t *testing.T) {
		args := append(append(append([]string{}, srcArgs...), dstArgs...),
			"-x", "esm-it-src", "-y", "esm-it-dst", "--copy_settings", "--copy_mappings", "-w", "4", "--sliced_scroll_size=2", "-c", "300")
		esm(t, 0, args...)

		if n := dst.count(t, "esm-it-dst"); n != itDocs {
			t.Errorf("target has %d documents, want %d", n, itDocs)
		}
		var settings map[string]struct {
			Settings struct {
				Index map[string]interface{} `json:"index"`
			} `json:"settings"`
		}
		dst.do(t, "GET", "/esm-it-dst/_settings", "", &settings)
		index := settings["esm-it-dst"].Settings.Index
		if index["number_of_shards"] != "2" || index["number_of_replicas"] != "0" {
			t.Errorf("settings not copied: %v", index)
		}
		if index["refresh_interval"] == "-1" {
			t.Error("refresh_interval was not restored after the migration")
		}
		var mapping map[string]interface{}
		dst.do(t, "GET", "/esm-it-dst/_mapping", "", &mapping)
		if m := fmt.Sprint(mapping); !strings.Contains(m, "type:long") || !strings.Contains(m, "raw:map[type:keyword]") {
			t.Errorf("mappings not copied: %s", m)
		}
		if got := dst.source(t, "esm-it-dst", "00042"); got != src.source(t, "esm-it-src", "00042") {
			t.Errorf("document 00042 = %s", got)
		}
	})

	if strings.HasPrefix(dst.url, "https") {
		t.Run("self-signed certificate is rejected", func(t *testing.T) {
			args := append(append([]string{}, srcArgs...), "-d", dst.url, "-n", dst.auth, "-x", "esm-it-src", "-y", "esm-it-tls")
			esm(t, 1, args...)
		})
	}

	t.Run("dump and reimport", func(t *testing.T) {
		dump := filepath.Join(t.TempDir(), "dump.json")
		esm(t, 0, append(append([]string{}, srcArgs...), "-x", "esm-it-src", "-o", dump, "--skip=secret")...)
		esm(t, 0, append(append([]string{}, dstArgs...), "-i", dump, "-y", "esm-it-reimport", "-w", "2")...)

		if n := dst.count(t, "esm-it-reimport"); n != itDocs {
			t.Errorf("target has %d documents, want %d", n, itDocs)
		}
		if got := dst.source(t, "esm-it-reimport", "00007"); strings.Contains(got, "secret") || !strings.Contains(got, `"n":7`) {
			t.Errorf("document 00007 = %s", got)
		}
	})

	t.Run("sync", func(t *testing.T) {
		src.enableIdSort(t)
		dst.enableIdSort(t)
		dst.do(t, "DELETE", "/esm-it-dst/_doc/00005", "", nil)
		dst.do(t, "PUT", "/esm-it-dst/_doc/00007", `{"n":-7}`, nil)
		dst.do(t, "PUT", "/esm-it-dst/_doc/99999", `{"n":99999}`, nil)
		dst.do(t, "POST", "/esm-it-dst/_refresh", "", nil)

		args := append(append(append([]string{}, srcArgs...), dstArgs...),
			"-x", "esm-it-src", "-y", "esm-it-dst", "--sync", "--enable_delete", "-c", "300")
		esm(t, 0, args...)

		if n := dst.count(t, "esm-it-dst"); n != itDocs {
			t.Errorf("target has %d documents, want %d", n, itDocs)
		}
		for _, id := range []string{"00005", "00007"} {
			if got, want := dst.source(t, "esm-it-dst", id), src.source(t, "esm-it-src", id); got != want {
				t.Errorf("document %s = %s, want %s", id, got, want)
			}
		}
		if got := dst.source(t, "esm-it-dst", "99999"); got != "" {
			t.Errorf("extra document not deleted: %s", got)
		}
	})
}
