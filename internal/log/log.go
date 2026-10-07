// Package log is a small leveled logger on top of log/slog, with the
// Debug/Debugf/... helpers used across esm.
package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// LevelTrace is below slog's debug level, for very verbose output
const LevelTrace = slog.LevelDebug - 4

// logger is swapped by Setup while other goroutines may be logging
var logger atomic.Pointer[slog.Logger]

func init() {
	logger.Store(newLogger(os.Stderr, slog.LevelInfo))
}

// ParseLevel converts a level name (trace, debug, info, warn, error) to a slog level
func ParseLevel(name string) (slog.Level, error) {
	switch strings.ToLower(name) {
	case "trace":
		return LevelTrace, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return slog.LevelInfo, fmt.Errorf("unknown log level %q, options: trace, debug, info, warn, error", name)
}

// Setup writes logs of the given level and above to w
func Setup(w io.Writer, level slog.Level) {
	logger.Store(newLogger(w, level))
}

func newLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.LevelKey:
				if a.Value.Any().(slog.Level) == LevelTrace {
					a.Value = slog.StringValue("TRACE")
				}
			case slog.SourceKey:
				// file:line is enough, the full path is noise
				if src, ok := a.Value.Any().(*slog.Source); ok {
					a.Value = slog.StringValue(fmt.Sprintf("%s:%d", shortFile(src.File), src.Line))
				}
			}
			return a
		},
	}))
}

func shortFile(path string) string {
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[i+1:]
	}
	return path
}

func write(level slog.Level, msg string) {
	ctx := context.Background()
	l := logger.Load()
	if !l.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip Callers, write and the exported helper
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	_ = l.Handler().Handle(ctx, r)
}

func Trace(v ...interface{})                 { write(LevelTrace, fmt.Sprint(v...)) }
func Tracef(format string, v ...interface{}) { write(LevelTrace, fmt.Sprintf(format, v...)) }
func Debug(v ...interface{})                 { write(slog.LevelDebug, fmt.Sprint(v...)) }
func Debugf(format string, v ...interface{}) { write(slog.LevelDebug, fmt.Sprintf(format, v...)) }
func Info(v ...interface{})                  { write(slog.LevelInfo, fmt.Sprint(v...)) }
func Infof(format string, v ...interface{})  { write(slog.LevelInfo, fmt.Sprintf(format, v...)) }
func Warn(v ...interface{})                  { write(slog.LevelWarn, fmt.Sprint(v...)) }
func Warnf(format string, v ...interface{})  { write(slog.LevelWarn, fmt.Sprintf(format, v...)) }
func Error(v ...interface{})                 { write(slog.LevelError, fmt.Sprint(v...)) }
func Errorf(format string, v ...interface{}) { write(slog.LevelError, fmt.Sprintf(format, v...)) }
