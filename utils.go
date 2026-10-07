package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

func SubString(prim string, start int, end int) string {
	if len(prim) == 0 {
		fmt.Println("primitive str is empty")
	}
	if l := len(prim); l < end {
		end = l
	}

	value := prim
	runes := []rune(value)

	safeSubString := string(runes[start:end])

	return safeSubString
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
