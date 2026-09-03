package pipeline

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestStdoutSinkWritesNDJSON(t *testing.T) {
	var out, errBuf bytes.Buffer
	s := NewStdoutSink(&out, &errBuf)

	if err := s.Send(context.Background(), Outgoing{Payload: []byte(`{"orderId":"abc"}`)}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Send(context.Background(), Outgoing{Payload: []byte(`{"orderId":"def"}`)}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), out.String())
	}
	if lines[0] != `{"orderId":"abc"}` || lines[1] != `{"orderId":"def"}` {
		t.Errorf("unexpected stdout: %q", out.String())
	}
	if errBuf.Len() != 0 {
		t.Errorf("expected no stderr output without a key, got %q", errBuf.String())
	}
}

func TestStdoutSinkEchoesKeyToStderr(t *testing.T) {
	var out, errBuf bytes.Buffer
	s := NewStdoutSink(&out, &errBuf)

	o := Outgoing{Key: []byte(`cust-1`), Payload: []byte(`{"id":"cust-1"}`)}
	if err := s.Send(context.Background(), o); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(errBuf.String(), "Key: cust-1") {
		t.Errorf("expected raw Key echo in stderr, got %q", errBuf.String())
	}
	if !strings.Contains(out.String(), `{"id":"cust-1"}`) {
		t.Errorf("expected payload in stdout, got %q", out.String())
	}
}
