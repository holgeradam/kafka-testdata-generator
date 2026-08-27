package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// StdoutSink writes Payloads as NDJSON lines to an out writer (Dry run). When a
// run configured a Key, its value is echoed as "Key: <value>" to a separate err
// writer (stderr) ahead of the payload line. See ADR-0003.
type StdoutSink struct {
	out  io.Writer
	errs io.Writer
}

// NewStdoutSink returns a sink writing Payload NDJSON to out and Key echoes to
// errs. Passing nil errs disables the Key echo.
func NewStdoutSink(out io.Writer, errs io.Writer) *StdoutSink {
	return &StdoutSink{out: out, errs: errs}
}

// Send writes the Payload as one NDJSON line to stdout and, when a Key is
// present, echoes it to stderr.
func (s *StdoutSink) Send(_ context.Context, o Outgoing) error {
	if len(o.Key) > 0 && s.errs != nil {
		var v any
		if err := json.Unmarshal(o.Key, &v); err == nil {
			fmt.Fprintf(s.errs, "Key: %v\n", v)
		}
	}
	_, err := fmt.Fprintln(s.out, string(o.Payload))
	return err
}

// Close is a no-op for a writer-backed sink.
func (s *StdoutSink) Close() error { return nil }
