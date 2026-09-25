package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// StdoutSink writes Payloads as NDJSON lines to an out writer (Dry run). When a
// run configured a Key, its raw bytes are echoed as "Key: <bytes>" to a separate
// err writer (stderr) ahead of the payload line, and a record's Headers follow
// as "Headers: {...}". See ADR-0003.
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
// present, echoes its raw bytes to stderr, then the Headers when there are
// any.
func (s *StdoutSink) Send(_ context.Context, o Outgoing) error {
	if len(o.Key) > 0 && s.errs != nil {
		fmt.Fprintf(s.errs, "Key: %s\n", o.Key)
	}
	if len(o.Headers) > 0 && s.errs != nil {
		fmt.Fprintf(s.errs, "Headers: %s\n", headersJSON(o.Headers))
	}
	_, err := fmt.Fprintln(s.out, string(o.Payload))
	return err
}

// Close is a no-op for a writer-backed sink.
func (s *StdoutSink) Close() error { return nil }

// headersJSON shows Headers as a JSON object of each header's text, in header
// order, a null header as null.
func headersJSON(headers []Header) []byte {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, h := range headers {
		if i > 0 {
			b.WriteByte(',')
		}
		name, _ := json.Marshal(h.Name)
		b.Write(name)
		b.WriteByte(':')
		if h.Value == nil {
			b.WriteString("null")
			continue
		}
		value, _ := json.Marshal(string(h.Value))
		b.Write(value)
	}
	b.WriteByte('}')
	return b.Bytes()
}
