package main

import (
	"encoding/json"
	"strings"
)

// SubString returns the runes start to end of prim, end is capped at the length of prim
func SubString(prim string, start int, end int) string {
	runes := []rune(prim)
	if end > len(runes) {
		end = len(runes)
	}
	if start > end {
		start = end
	}
	return string(runes[start:end])
}

// splitFieldList parses a comma separated list of field names, ignoring empty entries
func splitFieldList(s string) []string {
	var fields []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			fields = append(fields, f)
		}
	}
	return fields
}

// removeSourceFields returns the _source without the given top level fields
func removeSourceFields(source json.RawMessage, fields []string) (json.RawMessage, error) {
	if len(fields) == 0 || len(source) == 0 {
		return source, nil
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(source, &doc); err != nil {
		return source, err
	}
	for _, f := range fields {
		delete(doc, f)
	}
	return json.Marshal(doc)
}
