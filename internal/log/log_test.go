package log

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := map[string]slog.Level{"trace": LevelTrace, "DEBUG": slog.LevelDebug, "": slog.LevelInfo, "Info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError}
	for name, want := range tests {
		if got, err := ParseLevel(name); err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v, want %v", name, got, err, want)
		}
	}
	if _, err := ParseLevel("loud"); err == nil {
		t.Error("want an error for an unknown level")
	}
}

func TestOutput(t *testing.T) {
	var buf bytes.Buffer
	Setup(&buf, slog.LevelInfo)
	defer Setup(&bytes.Buffer{}, slog.LevelInfo)

	Debug("hidden")
	Info("index not exists,", "idx")
	Errorf("failed %d times", 3)

	out := buf.String()
	if strings.Contains(out, "hidden") {
		t.Errorf("debug message logged at info level:\n%s", out)
	}
	for _, want := range []string{`level=INFO`, `msg="index not exists,idx"`, `level=ERROR`, `msg="failed 3 times"`, `source=log_test.go:`} {
		if !strings.Contains(out, want) {
			t.Errorf("output misses %s:\n%s", want, out)
		}
	}

	buf.Reset()
	Setup(&buf, LevelTrace)
	Trace("very verbose")
	if !strings.Contains(buf.String(), "level=TRACE") {
		t.Errorf("trace level not named TRACE:\n%s", buf.String())
	}
}
