package pipeline

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
)

func testNow() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
}

type fakeSink struct {
	mu       sync.Mutex
	recorded []Outgoing
	err      error
}

func (f *fakeSink) Send(_ context.Context, o Outgoing) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.recorded = append(f.recorded, o)
	return nil
}

func (f *fakeSink) Close() error { return nil }

func (f *fakeSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.recorded)
}

// blockingSink is a Sink whose Send blocks until the context is done, then
// returns the context error - faithfully reproducing the produce path, where
// franz-go's ProduceSync observes an in-flight cancellation and returns the
// context error (producer.go: rctx.Err()). Its released behavior lets a test
// gate cancellation on the moment the first send is in flight.
type blockingSink struct {
	blocked chan struct{}
}

func newBlockingSink() *blockingSink {
	return &blockingSink{blocked: make(chan struct{})}
}

func (b *blockingSink) Send(ctx context.Context, _ Outgoing) error {
	close(b.blocked)
	<-ctx.Done()
	return ctx.Err()
}

func (b *blockingSink) Close() error { return nil }

// schemaFor builds a minimal object schema with the given key field.
func schemaFor(keyField string) map[string]any {
	if keyField == "" {
		return map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "string"}}}
	}
	return map[string]any{"type": "object", "properties": map[string]any{keyField: map[string]any{"type": "string"}}}
}

func TestRunProducesCountPayloads(t *testing.T) {
	gen := generator.New(1, testNow())
	sink := &fakeSink{}
	p := New(Config{
		Generator: gen,
		Schema:    schemaFor(""),
		Count:     3,
	}, sink)

	stats, _ := p.Run(context.Background())

	if stats.Total != 3 {
		t.Errorf("expected Total 3, got %d", stats.Total)
	}
	if stats.Acked != 3 {
		t.Errorf("expected Acked 3, got %d", stats.Acked)
	}
	if stats.Failed != 0 {
		t.Errorf("expected Failed 0, got %d", stats.Failed)
	}
	if len(sink.recorded) != 3 {
		t.Errorf("expected 3 sink calls, got %d", len(sink.recorded))
	}
	for _, o := range sink.recorded {
		if len(o.Payload) == 0 {
			t.Error("expected non-empty payload bytes")
		}
	}
}

func TestRunStopsAtCount(t *testing.T) {
	gen := generator.New(1, testNow())
	sink := &fakeSink{}
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 2}, sink)

	stats, _ := p.Run(context.Background())

	if stats.Total != 2 || stats.Acked != 2 || stats.Failed != 0 {
		t.Errorf("unexpected stats: %+v", stats)
	}
	if sink.count() != 2 {
		t.Errorf("expected 2 sink calls, got %d", sink.count())
	}
}

func TestRunCancellationMidRun(t *testing.T) {
	gen := generator.New(1, testNow())
	sink := &fakeSink{}
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 100000}, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Stats, 1)
	go func() {
		s, _ := p.Run(ctx)
		done <- s
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case stats := <-done:
		if stats.Total == 0 {
			t.Error("expected some payloads before cancellation")
		}
		if stats.Acked != stats.Total {
			t.Errorf("expected acked == total on clean cancel, got %d vs %d", stats.Acked, stats.Total)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// TestRunCancellationInterruptsBlockedSend proves the produce-path teardown: a
// cancellation while a Sink.Send is blocked in-flight (as KafkaSink.Send is,
// via ProduceSync(ctx, ...)) must be observed by the blocked send and must not
// hang the pipeline. This is the #4 gap - TestRunCancellationMidRun covers
// cancellation only between sends, not during one. The blocked send returns the
// context error (matching the real produce path), so the interrupted payload is
// counted as failed, not acked.
func TestRunCancellationInterruptsBlockedSend(t *testing.T) {
	gen := generator.New(1, testNow())
	sink := newBlockingSink()
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 100000}, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Stats, 1)
	go func() {
		s, _ := p.Run(ctx)
		done <- s
	}()

	// Wait until the first Send is blocked in-flight, then cancel.
	select {
	case <-sink.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("Send never blocked")
	}
	cancel()

	// The pipeline must observe the cancelled send and shut down cleanly. The
	// in-flight payload is a failure (context-cancelled), not an ack - the same
	// accounting a real Kafka produce hit by cancellation produces.
	select {
	case stats := <-done:
		if stats.Total < 1 {
			t.Errorf("expected at least one payload to be in flight before cancellation, got %+v", stats)
		}
		if stats.Failed != 1 {
			t.Errorf("expected the interrupted send to count as 1 failure, got %+v", stats)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel interrupting a blocked send")
	}
}

func TestRunCountsSendFailures(t *testing.T) {
	gen := generator.New(1, testNow())
	sink := &fakeSink{err: errors.New("boom")}
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 3}, sink)

	stats, _ := p.Run(context.Background())

	if stats.Total != 3 || stats.Acked != 0 || stats.Failed != 3 {
		t.Errorf("expected Total 3, Acked 0, Failed 3, got %+v", stats)
	}
}

func TestRunCountsMissingKey(t *testing.T) {
	gen := generator.New(1, testNow())
	sink := &fakeSink{}
	// Schema generates an object WITHOUT the configured key field, so every
	// payload is missing that key and should be skipped as failed.
	p := New(Config{Generator: gen, Schema: schemaFor("id"), Count: 3, KeyField: "nope"}, sink)

	stats, _ := p.Run(context.Background())

	if stats.Total != 3 || stats.Acked != 0 || stats.Failed != 3 {
		t.Errorf("expected Total 3, Acked 0, Failed 3, got %+v", stats)
	}
	if sink.count() != 0 {
		t.Errorf("expected no sink calls when key is missing, got %d", sink.count())
	}
}

func TestRunWarnsOnMissingKey(t *testing.T) {
	gen := generator.New(1, testNow())
	sink := &fakeSink{}
	var warn bytes.Buffer
	p := New(Config{
		Generator: gen,
		Schema:    schemaFor("id"),
		Count:     2,
		KeyField:  "nope",
		Warn:      &warn,
	}, sink)

	stats, _ := p.Run(context.Background())

	if stats.Failed != 2 {
		t.Errorf("expected 2 failed, got %d", stats.Failed)
	}
	if !strings.Contains(warn.String(), `field "nope" not found`) {
		t.Errorf("expected missing-key warning in Warn writer, got %q", warn.String())
	}
}

func TestRunAbortsOnGenerationError(t *testing.T) {
	sink := &fakeSink{}
	p := New(Config{
		Generator: generator.New(1, testNow()),
		Schema:    map[string]any{"type": "widget"},
		Count:     3,
	}, sink)

	stats, err := p.Run(context.Background())
	var ue *generator.UnsupportedSchemaError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *generator.UnsupportedSchemaError, got %v", err)
	}
	if ue.Keyword != "type" {
		t.Errorf("keyword = %q, want type", ue.Keyword)
	}
	if ue.Path != generator.RootField {
		t.Errorf("path = %q, want %q", ue.Path, generator.RootField)
	}
	if stats.Total != 0 || stats.Acked != 0 || stats.Failed != 0 {
		t.Errorf("expected zero stats on abort, got %+v", stats)
	}
	if sink.count() != 0 {
		t.Errorf("expected no sink calls on generation failure, got %d", sink.count())
	}
}

func TestRunAttachesKeyWhenPresent(t *testing.T) {
	gen := generator.New(1, testNow())
	sink := &fakeSink{}
	// Schema REQUIRES "id", which doubles as the key field.
	keyField := "id"
	schema := map[string]any{
		"type":     "object",
		"required": []any{"id"},
		"properties": map[string]any{
			"id": map[string]any{"type": "string"},
		},
	}
	p := New(Config{Generator: gen, Schema: schema, Count: 2, KeyField: keyField}, sink)

	stats, _ := p.Run(context.Background())

	if stats.Failed != 0 || stats.Acked != 2 {
		t.Errorf("expected all acked, got %+v", stats)
	}
	if sink.count() != 2 {
		t.Fatalf("expected 2 sink calls, got %d", sink.count())
	}
	for _, o := range sink.recorded {
		if len(o.Key) == 0 {
			t.Error("expected non-empty key bytes")
		}
	}
}
