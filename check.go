// SPDX-License-Identifier: Apache-2.0
// See LICENSE.md in the repository root for license details.

package main

import (
	"fmt"
	log "github.com/InPoint-Cloud/esm/internal/log"
	"os"
	"strings"
	"text/tabwriter"
)

// CheckCounts compares the document count of every migrated index between source and target,
// prints a table and returns false if any of them differ
func (m *Migrator) CheckCounts() bool {
	c := m.Config
	typeless := m.targetIsTypeless()

	// target index => source indexes, several source indexes are merged into one with -y
	var targets []string
	sources := map[string][]string{}
	var skipped []string
	for _, name := range strings.Split(c.SourceIndexNames, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if typeless && strings.HasPrefix(name, ".") && !c.CopyAllIndexes && c.TargetIndexName == "" {
			skipped = append(skipped, name)
			continue
		}
		target := name
		if c.TargetIndexName != "" {
			target = c.TargetIndexName
		}
		if _, ok := sources[target]; !ok {
			targets = append(targets, target)
		}
		sources[target] = append(sources[target], name)
	}

	// with regenerated ids every round adds new documents
	factor := 1
	if c.RegenerateID && c.RepeatOutputTimes > 1 {
		factor = c.RepeatOutputTimes
	}

	ok := true
	fmt.Println("\ndocument count check:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SOURCE\tTARGET\tSOURCE COUNT\tTARGET COUNT\tRESULT")
	for _, target := range targets {
		// _count only sees refreshed documents
		if err := m.TargetESAPI.Refresh(target); err != nil {
			log.Warn(err)
		}
		expected := 0
		var srcErr error
		for _, src := range sources[target] {
			count, err := m.SourceESAPI.Count(src, c.Query)
			if err != nil {
				srcErr = err
				break
			}
			expected += count * factor
		}
		actual, dstErr := m.TargetESAPI.Count(target, "")

		srcCount, dstCount, result := fmt.Sprint(expected), fmt.Sprint(actual), "ok"
		if srcErr != nil {
			srcCount, result = "error", "ERROR"
			log.Error("can not count source index ", strings.Join(sources[target], ","), ": ", SubString(srcErr.Error(), 0, 300))
		}
		if dstErr != nil {
			dstCount, result = "error", "ERROR"
			log.Error("can not count target index ", target, ": ", SubString(dstErr.Error(), 0, 300))
		}
		if result == "ok" && expected != actual {
			result = fmt.Sprintf("MISMATCH (%+d)", actual-expected)
		}
		if result != "ok" {
			ok = false
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", strings.Join(sources[target], ","), target, srcCount, dstCount, result)
	}
	for _, name := range skipped {
		fmt.Fprintf(w, "%s\t-\t-\t-\tskipped (system index)\n", name)
	}
	w.Flush()
	return ok
}
