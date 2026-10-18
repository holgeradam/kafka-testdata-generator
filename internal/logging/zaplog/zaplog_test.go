package zaplog_test

import (
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/logging"
	"github.com/holgeradam/kafka-testdata-generator/internal/logging/zaplog"
)

// TestConsoleTextOnTheWriter proves the adapter writes one human line per
// record: the level, the message with its wording untouched, and the attrs.
func TestConsoleTextOnTheWriter(t *testing.T) {
	var out strings.Builder
	l := zaplog.New(&out, logging.Debug)
	l.Warn("dry-run mode disregards Kafka options")
	l.Error("record failed", "record", 3, "error", "broker unreachable")

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("output = %q, want one line per record", out.String())
	}
	for i, want := range [][]string{
		{"WARN", "dry-run mode disregards Kafka options"},
		{"ERROR", "record failed", "record", "3", "error", "broker unreachable"},
	} {
		for _, w := range want {
			if !strings.Contains(lines[i], w) {
				t.Errorf("line %d = %q, want it to contain %q", i, lines[i], w)
			}
		}
	}
}

// TestLevelFilters proves a record below the level never reaches the writer.
func TestLevelFilters(t *testing.T) {
	var out strings.Builder
	l := zaplog.New(&out, logging.Warn)
	l.Debug("debug")
	l.Info("info")
	l.Warn("warn")
	l.Error("error")

	if got := out.String(); strings.Contains(got, "debug") || strings.Contains(got, "info") ||
		!strings.Contains(got, "warn") || !strings.Contains(got, "error") {
		t.Errorf("output = %q, want only warn and error", got)
	}
}
