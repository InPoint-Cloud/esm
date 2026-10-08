package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeES is an in-memory elasticsearch good enough for esm: index settings and mappings,
// (sliced) scrolls sorted by _id, _bulk, _count and _cat/indices
type fakeES struct {
	version     string // cluster version, ie: 7.10.2
	tls         bool   // serve https with a self-signed certificate
	scrollError int    // if set, requests for the next scroll page fail with this status code
	scrollFails int    // number of failing scroll requests before they succeed again, 0 fails all of them
	rejectBulk  int    // if set, every bulk item fails with this status code
	bulkStatus  []int  // status codes of the next whole bulk requests, ie: 503 for an overloaded cluster
	itemStatus  []int  // status codes of the next bulk items, ie: 429 for rejected documents
	auth        string // required Authorization header
	health      string // cluster health status of the next healthChecks requests, then green
	healthCheck int
	sorts       []string // the sort of every new scroll, "" without one
	dataNodes   int      // number_of_data_nodes of the cluster health
	// persistent cluster settings, ie: indices.id_field_data.enabled
	clusterSettings map[string]interface{}
	// http.max_content_length set on the nodes, "" for the default
	maxContentLength string

	mu       sync.Mutex
	indices  map[string]*fakeIndex
	scrolls  map[string][]fakeHit // scroll id => remaining hits
	pageSize map[string]int       // scroll id => page size
	nextID   int
}

type fakeIndex struct {
	settings map[string]interface{} // the "index" object of the settings
	mappings map[string]interface{}
	docs     map[string]fakeDoc
}

type fakeDoc struct {
	typ    string
	source json.RawMessage
}

type fakeHit struct {
	Index  string          `json:"_index"`
	Type   string          `json:"_type,omitempty"`
	Id     string          `json:"_id"`
	Source json.RawMessage `json:"_source"`
}

func newFakeES(version string) *fakeES {
	return &fakeES{version: version, indices: map[string]*fakeIndex{}, scrolls: map[string][]fakeHit{}, pageSize: map[string]int{}}
}

func (f *fakeES) start(t *testing.T) string {
	t.Helper()
	newServer := httptest.NewServer
	if f.tls {
		newServer = httptest.NewTLSServer
	}
	srv := newServer(http.HandlerFunc(f.handle))
	t.Cleanup(srv.Close)
	return srv.URL
}

func (f *fakeES) major() int {
	major, _ := strconv.Atoi(strings.SplitN(f.version, ".", 2)[0])
	return major
}

// defaultType is the mapping type of new documents
func (f *fakeES) defaultType() string {
	if f.major() < 7 {
		return "doc"
	}
	return "_doc"
}

// addIndex creates an index with docs number of documents {"n": i, "secret": "x", "keep": true}
func (f *fakeES) addIndex(name string, docs int) *fakeIndex {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.index(name)
	idx.settings = map[string]interface{}{"number_of_shards": "3", "number_of_replicas": "1", "refresh_interval": "5s",
		"uuid": "abc", "creation_date": "1", "provided_name": name, "version": map[string]interface{}{"created": "1"}}
	idx.mappings = map[string]interface{}{"properties": map[string]interface{}{"n": map[string]interface{}{"type": "long"}}}
	if f.major() < 7 {
		idx.mappings = map[string]interface{}{f.defaultType(): idx.mappings}
	}
	for i := 0; i < docs; i++ {
		f.putDoc(name, fmt.Sprintf("%04d", i), f.defaultType(), fmt.Sprintf(`{"n":%d,"secret":"x","keep":true}`, i))
	}
	return idx
}

// index returns the named index, creating it like elasticsearch does on the first document
func (f *fakeES) index(name string) *fakeIndex {
	idx, ok := f.indices[name]
	if !ok {
		idx = &fakeIndex{settings: map[string]interface{}{}, mappings: map[string]interface{}{}, docs: map[string]fakeDoc{}}
		f.indices[name] = idx
	}
	return idx
}

func (f *fakeES) putDoc(index, id, typ, source string) {
	f.index(index).docs[id] = fakeDoc{typ: typ, source: json.RawMessage(source)}
}

// docs returns the sources of an index by id
func (f *fakeES) docs(index string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	if idx, ok := f.indices[index]; ok {
		for id, d := range idx.docs {
			out[id] = string(d.source)
		}
	}
	return out
}

func (f *fakeES) settings(index string) map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx, ok := f.indices[index]; ok {
		return idx.settings
	}
	return nil
}

func (f *fakeES) mappings(index string) map[string]interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx, ok := f.indices[index]; ok {
		return idx.mappings
	}
	return nil
}

func (f *fakeES) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.auth != "" && r.Header.Get("Authorization") != f.auth {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":{"type":"security_exception","reason":"missing authentication credentials"},"status":401}`))
		return
	}

	reply := func(status int, o interface{}) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(o)
	}
	notFound := func(name string) {
		reply(404, map[string]interface{}{"error": map[string]string{"type": "index_not_found_exception", "index": name}, "status": 404})
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.URL.Path == "/":
		reply(200, map[string]interface{}{"version": map[string]string{"number": f.version}})
	case r.URL.Path == "/_cluster/health":
		health := "green"
		if f.healthCheck > 0 {
			f.healthCheck--
			health = f.health
		}
		reply(200, map[string]interface{}{"cluster_name": "fake", "status": health, "number_of_data_nodes": max(f.dataNodes, 1)})
	case r.URL.Path == "/_cluster/settings":
		if r.Method == http.MethodPut {
			var req struct {
				Persistent map[string]interface{} `json:"persistent"`
			}
			json.Unmarshal(body, &req)
			if f.clusterSettings == nil {
				f.clusterSettings = map[string]interface{}{}
			}
			for k, v := range req.Persistent {
				f.clusterSettings[k] = v
			}
			reply(200, map[string]bool{"acknowledged": true})
			return
		}
		reply(200, map[string]interface{}{"persistent": f.clusterSettings, "transient": map[string]interface{}{},
			"defaults": map[string]interface{}{"http.max_content_length": "100mb"}})
	case r.URL.Path == "/_nodes/settings":
		settings := map[string]interface{}{}
		if f.maxContentLength != "" {
			settings["http"] = map[string]interface{}{"max_content_length": f.maxContentLength}
		}
		reply(200, map[string]interface{}{"nodes": map[string]interface{}{"node1": map[string]interface{}{"settings": settings}}})
	case r.URL.Path == "/_bulk":
		if len(f.bulkStatus) > 0 {
			status := f.bulkStatus[0]
			f.bulkStatus = f.bulkStatus[1:]
			reply(status, map[string]string{"error": "bulk failed"})
			return
		}
		reply(200, f.bulk(body))
	case r.URL.Path == "/_search/scroll":
		if r.Method == http.MethodDelete {
			reply(200, map[string]bool{"succeeded": true})
			return
		}
		if f.scrollError != 0 {
			status := f.scrollError
			if f.scrollFails > 0 {
				if f.scrollFails--; f.scrollFails == 0 {
					f.scrollError = 0
				}
			}
			reply(status, map[string]string{"error": "scroll failed"})
			return
		}
		id := r.URL.Query().Get("scroll_id")
		if id == "" {
			var p struct {
				ScrollId string `json:"scroll_id"`
			}
			json.Unmarshal(body, &p)
			id = p.ScrollId
		}
		reply(200, f.scrollPage(id))
	case parts[0] == "_cat" && len(parts) >= 2 && parts[1] == "indices":
		rows := []map[string]string{}
		for name, idx := range f.indices {
			if len(parts) == 3 && !matchesAny(parts[2], name) {
				continue
			}
			size := 0
			for _, d := range idx.docs {
				size += len(d.source)
			}
			rows = append(rows, map[string]string{"index": name, "health": "green", "status": "open", "docs.count": strconv.Itoa(len(idx.docs)),
				"pri": fmt.Sprint(idx.settings["number_of_shards"]), "rep": fmt.Sprint(idx.settings["number_of_replicas"]),
				"pri.store.size": strconv.Itoa(size)})
		}
		if len(rows) == 0 && len(parts) == 3 {
			notFound(parts[2])
			return
		}
		reply(200, rows)
	case len(parts) == 1:
		name := parts[0]
		switch r.Method {
		case http.MethodPut:
			if _, ok := f.indices[name]; ok {
				reply(400, map[string]interface{}{"error": map[string]string{"type": "resource_already_exists_exception"}})
				return
			}
			var req struct {
				Settings struct {
					Index map[string]interface{} `json:"index"`
				} `json:"settings"`
				Mappings map[string]interface{} `json:"mappings"`
			}
			json.Unmarshal(body, &req)
			idx := f.index(name)
			if req.Settings.Index != nil {
				idx.settings = req.Settings.Index
			}
			if req.Mappings != nil {
				idx.mappings = req.Mappings
			}
			reply(200, map[string]bool{"acknowledged": true})
		case http.MethodDelete:
			delete(f.indices, name)
			reply(200, map[string]bool{"acknowledged": true})
		default:
			reply(405, map[string]string{"error": "method not allowed"})
		}
	case len(parts) >= 2:
		names, action := parts[0], parts[len(parts)-1]
		var existing []string
		for n := range f.indices {
			if matchesAny(names, n) {
				existing = append(existing, n)
			}
		}
		sort.Strings(existing)
		if len(existing) == 0 {
			notFound(names)
			return
		}
		switch action {
		case "_settings":
			if r.Method == http.MethodGet {
				out := map[string]interface{}{}
				for _, n := range existing {
					out[n] = map[string]interface{}{"settings": map[string]interface{}{"index": f.indices[n].settings}}
				}
				reply(200, out)
				return
			}
			var req struct {
				Settings struct {
					Index map[string]interface{} `json:"index"`
				} `json:"settings"`
			}
			json.Unmarshal(body, &req)
			for _, n := range existing {
				for k, v := range req.Settings.Index {
					f.indices[n].settings[k] = v
				}
			}
			reply(200, map[string]bool{"acknowledged": true})
		case "_mapping":
			if r.Method == http.MethodGet {
				out := map[string]interface{}{}
				for _, n := range existing {
					out[n] = map[string]interface{}{"mappings": f.indices[n].mappings}
				}
				reply(200, out)
				return
			}
			var m map[string]interface{}
			json.Unmarshal(body, &m)
			for _, n := range existing {
				f.indices[n].mappings = m
			}
			reply(200, map[string]bool{"acknowledged": true})
		case "_count":
			n := 0
			for _, name := range existing {
				n += len(f.indices[name].docs)
			}
			reply(200, map[string]int{"count": n})
		case "_search":
			reply(200, f.newScroll(existing, r, body))
		default: // _refresh, _open, _close
			reply(200, map[string]bool{"acknowledged": true})
		}
	default:
		reply(404, map[string]string{"error": "unknown request"})
	}
}

// matchesAny returns true if the index name matches one of the comma separated patterns
func matchesAny(patterns, name string) bool {
	for _, pattern := range strings.Split(patterns, ",") {
		if ok, _ := path.Match(pattern, name); ok || pattern == "_all" {
			return true
		}
	}
	return false
}

func (f *fakeES) newScroll(indices []string, r *http.Request, body []byte) interface{} {
	var q struct {
		Slice *struct{ Id, Max int } `json:"slice"`
		Sort  []string               `json:"sort"`
	}
	json.Unmarshal(body, &q)
	f.sorts = append(f.sorts, strings.Join(q.Sort, ","))
	size, _ := strconv.Atoi(r.URL.Query().Get("size"))
	if size <= 0 {
		size = 10
	}

	var hits []fakeHit
	for _, name := range indices {
		for id, d := range f.indices[name].docs {
			hits = append(hits, fakeHit{Index: name, Type: d.typ, Id: id, Source: d.source})
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].Id < hits[j].Id })
	if q.Slice != nil && q.Slice.Max > 1 {
		var sliced []fakeHit
		for i, h := range hits {
			if i%q.Slice.Max == q.Slice.Id {
				sliced = append(sliced, h)
			}
		}
		hits = sliced
	}
	if f.major() >= 8 {
		for i := range hits {
			hits[i].Type = "" // _type is gone from 8.x hits
		}
	}

	f.nextID++
	id := fmt.Sprint("scroll", f.nextID)
	f.scrolls[id] = hits
	f.pageSize[id] = size
	return f.scrollPage(id)
}

// scrollPage returns the next page of a scroll
func (f *fakeES) scrollPage(id string) interface{} {
	hits := f.scrolls[id]
	size := f.pageSize[id]
	total := len(hits)
	if size > len(hits) {
		size = len(hits)
	}
	page := hits[:size]
	f.scrolls[id] = hits[size:]

	var t interface{} = total
	if f.major() >= 7 {
		t = map[string]interface{}{"value": total, "relation": "eq"}
	}
	if page == nil {
		page = []fakeHit{}
	}
	return map[string]interface{}{"_scroll_id": id, "hits": map[string]interface{}{"total": t, "hits": page}}
}

func (f *fakeES) bulk(body []byte) interface{} {
	var items []interface{}
	s := bufio.NewScanner(bytes.NewReader(body))
	s.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	for s.Scan() {
		var action map[string]struct {
			Index string `json:"_index"`
			Type  string `json:"_type"`
			Id    string `json:"_id"`
		}
		if err := json.Unmarshal(s.Bytes(), &action); err != nil {
			continue
		}
		for op, meta := range action {
			status := f.rejectBulk
			if len(f.itemStatus) > 0 {
				status = f.itemStatus[0]
				f.itemStatus = f.itemStatus[1:]
			}
			if status != 0 {
				items = append(items, map[string]interface{}{op: map[string]interface{}{"_index": meta.Index, "_id": meta.Id, "status": status,
					"error": map[string]string{"type": "mapper_parsing_exception", "reason": "rejected"}}})
				if op != "delete" {
					s.Scan()
				}
				continue
			}
			typ := meta.Type
			if f.major() >= 8 && typ != "" {
				items = append(items, map[string]interface{}{op: map[string]interface{}{"status": 400,
					"error": map[string]string{"type": "illegal_argument_exception", "reason": "types are removed"}}})
				if op != "delete" {
					s.Scan()
				}
				continue
			}
			if typ == "" && f.major() < 7 {
				items = append(items, map[string]interface{}{op: map[string]interface{}{"status": 400,
					"error": map[string]string{"type": "action_request_validation_exception", "reason": "type is missing"}}})
				if op != "delete" {
					s.Scan()
				}
				continue
			}
			if typ == "" {
				typ = "_doc"
			}
			switch op {
			case "delete":
				status := 404
				if idx, ok := f.indices[meta.Index]; ok {
					if _, ok := idx.docs[meta.Id]; ok {
						delete(idx.docs, meta.Id)
						status = 200
					}
				}
				items = append(items, map[string]interface{}{op: map[string]interface{}{"_id": meta.Id, "status": status}})
			default:
				s.Scan()
				id := meta.Id
				if id == "" {
					f.nextID++
					id = fmt.Sprint("gen", f.nextID)
				}
				f.putDoc(meta.Index, id, typ, string(append([]byte(nil), s.Bytes()...)))
				items = append(items, map[string]interface{}{op: map[string]interface{}{"_id": id, "status": 201}})
			}
		}
	}
	return map[string]interface{}{"errors": false, "items": items}
}
