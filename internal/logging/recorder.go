package logging

import "sync"

// Entry is one record a Recorder kept.
type Entry = Diagnostic

// Recorder is a Logger that keeps what it is given, for tests to assert on.
type Recorder struct {
	mu      sync.Mutex
	entries []Entry
}

func (r *Recorder) record(l Level, msg string, attrs []any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, Entry{Level: l, Msg: msg, Attrs: attrs})
}

func (r *Recorder) Debug(msg string, attrs ...any) { r.record(Debug, msg, attrs) }
func (r *Recorder) Info(msg string, attrs ...any)  { r.record(Info, msg, attrs) }
func (r *Recorder) Warn(msg string, attrs ...any)  { r.record(Warn, msg, attrs) }
func (r *Recorder) Error(msg string, attrs ...any) { r.record(Error, msg, attrs) }

// Entries returns the records kept so far, in order.
func (r *Recorder) Entries() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Entry(nil), r.entries...)
}
