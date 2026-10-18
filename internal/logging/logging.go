// Package logging is the seam for the tool's diagnostics: warnings, the cause
// of each failed record, the interrupt notice (ADR-0012). It is the Logger
// interface and nothing else - no global logger, no library types - so the
// Pipeline, the Run plan and the Wire formats depend on a few lines of ours
// and tests substitute a Recorder. One adapter writes console text for the
// process edge (internal/logging/zaplog).
//
// Diagnostics are not product output: the Dry run Key and Headers lines and
// the Stats line are what the tool produces and stay unaffected by the level.
package logging

import "fmt"

// Level orders diagnostics by importance; a Logger drops those below its own.
type Level int

const (
	Debug Level = iota
	Info
	Warn
	Error
)

var levelNames = [...]string{Debug: "debug", Info: "info", Warn: "warn", Error: "error"}

// String is the level's name as -log-level spells it.
func (l Level) String() string { return levelNames[l] }

// ParseLevel reads a -log-level value.
func ParseLevel(name string) (Level, error) {
	for l, n := range levelNames {
		if n == name {
			return Level(l), nil
		}
	}
	return 0, fmt.Errorf("unknown log level %q: want debug, info, warn or error", name)
}

// Logger records diagnostics. attrs are alternating keys and values, as in
// Info("record failed", "record", 3, "error", err).
type Logger interface {
	Debug(msg string, attrs ...any)
	Info(msg string, attrs ...any)
	Warn(msg string, attrs ...any)
	Error(msg string, attrs ...any)
}

// Diagnostic is a log record as data. The Run plan and the Wire formats carry
// theirs this way, since building a plan does no I/O; the process edge emits
// them once it has a Logger.
type Diagnostic struct {
	Level Level
	Msg   string
	Attrs []any
}

// Emit hands the diagnostic to l at its level.
func (d Diagnostic) Emit(l Logger) {
	switch d.Level {
	case Debug:
		l.Debug(d.Msg, d.Attrs...)
	case Info:
		l.Info(d.Msg, d.Attrs...)
	case Warn:
		l.Warn(d.Msg, d.Attrs...)
	default:
		l.Error(d.Msg, d.Attrs...)
	}
}

type nop struct{}

func (nop) Debug(string, ...any) {}
func (nop) Info(string, ...any)  {}
func (nop) Warn(string, ...any)  {}
func (nop) Error(string, ...any) {}

// Nop returns a Logger that discards everything, for callers given none.
func Nop() Logger { return nop{} }
