package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// fakeGenerator is a controlled ValueGenerator: it returns a fixed Payload
// (and optional error) for every Value call, so pipeline tests exercise Pipeline
// mechanics without loading a schema or driving an RNG. Tests that need real
// generation semantics (key-binding synthesis, schema-error aborts) keep using
// *generator.Generator through the same interface.
type fakeGenerator struct {
	payload any
	err     error
}

func (f *fakeGenerator) Value(_ map[string]any) (any, error) {
	return f.payload, f.err
}

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
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := &fakeSink{}
	p := New(Config{
		Generator: gen,
		Schema:    schemaFor(""),
		Count:     3,
		Encoder:   JsonEncoder{},
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
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := &fakeSink{}
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 2, Encoder: JsonEncoder{}}, sink)

	stats, _ := p.Run(context.Background())

	if stats.Total != 2 || stats.Acked != 2 || stats.Failed != 0 {
		t.Errorf("unexpected stats: %+v", stats)
	}
	if sink.count() != 2 {
		t.Errorf("expected 2 sink calls, got %d", sink.count())
	}
}

func TestRunCancellationMidRun(t *testing.T) {
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := &fakeSink{}
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 100000, Encoder: JsonEncoder{}}, sink)

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
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := newBlockingSink()
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 100000, Encoder: JsonEncoder{}}, sink)

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
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := &fakeSink{err: errors.New("boom")}
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 3, Encoder: JsonEncoder{}}, sink)

	stats, _ := p.Run(context.Background())

	if stats.Total != 3 || stats.Acked != 0 || stats.Failed != 3 {
		t.Errorf("expected Total 3, Acked 0, Failed 3, got %+v", stats)
	}
}

func TestRunAbortsOnGenerationError(t *testing.T) {
	sink := &fakeSink{}
	gen := &fakeGenerator{err: &generator.UnsupportedSchemaError{Keyword: "type", Path: generator.RootPath}}
	p := New(Config{
		Generator: gen,
		Schema:    map[string]any{"type": "widget"},
		Count:     3,
		Encoder:   JsonEncoder{},
	}, sink)

	stats, err := p.Run(context.Background())
	var ue *generator.UnsupportedSchemaError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *generator.UnsupportedSchemaError, got %v", err)
	}
	if ue.Keyword != "type" {
		t.Errorf("keyword = %q, want type", ue.Keyword)
	}
	if ue.Path != generator.RootPath {
		t.Errorf("path = %q, want %q", ue.Path, generator.RootPath)
	}
	if stats.Total != 0 || stats.Acked != 0 || stats.Failed != 0 {
		t.Errorf("expected zero stats on abort, got %+v", stats)
	}
	if sink.count() != 0 {
		t.Errorf("expected no sink calls on generation failure, got %d", sink.count())
	}
}

func TestRunNullKeyInfoMessage(t *testing.T) {
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := &fakeSink{}
	var warn bytes.Buffer
	p := New(Config{
		Generator: gen,
		Schema:    schemaFor(""),
		Count:     1,
		Encoder:   JsonEncoder{},
		Warn:      &warn,
	}, sink)

	stats, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Acked != 1 {
		t.Errorf("expected 1 acked, got %d", stats.Acked)
	}
	if want := "no key configured, generating messages with a null key\n"; warn.String() != want {
		t.Errorf("null-key info message = %q, want %q", warn.String(), want)
	}
	for _, o := range sink.recorded {
		if len(o.Key) != 0 {
			t.Errorf("expected nil/empty key bytes for null key, got %v", o.Key)
		}
	}
}

// fakePlan is the Key seam's test adapter: it reports a fixed Key (or error)
// and records the payloads it was applied to.
type fakePlan struct {
	key     any
	err     error
	applied []any
}

func (p *fakePlan) Apply(payload any) (any, error) {
	p.applied = append(p.applied, payload)
	if p.err != nil {
		return nil, p.err
	}
	return p.key, nil
}

// TestRunKeyPlanProducesKey proves the Pipeline attaches whatever the Key plan
// returns, and applies it to the generated Payload.
func TestRunKeyPlanProducesKey(t *testing.T) {
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	plan := &fakePlan{key: "planned-key"}
	sink := &fakeSink{}
	var warn bytes.Buffer
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 2, KeyPlan: plan, Encoder: JsonEncoder{}, Warn: &warn}, sink)

	stats, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Acked != 2 {
		t.Errorf("expected 2 acked, got %d", stats.Acked)
	}
	for _, o := range sink.recorded {
		if string(o.Key) != "planned-key" {
			t.Errorf("key bytes = %q, want the planned Key encoded", o.Key)
		}
	}
	if len(plan.applied) != 2 {
		t.Errorf("plan applied %d times, want 2", len(plan.applied))
	}
	if strings.Contains(warn.String(), "no key configured") {
		t.Errorf("a Key plan is configured: must not warn about a null key, got %q", warn.String())
	}
}

// TestRunKeyPlanErrorAborts proves a Key the plan cannot produce stops the run
// before the record is counted: it is true of every record, not just this one.
func TestRunKeyPlanErrorAborts(t *testing.T) {
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := &fakeSink{}
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 3,
		KeyPlan: &fakePlan{err: errors.New("key schema cannot be honoured")}, Encoder: JsonEncoder{}}, sink)

	stats, err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected the Key plan error to abort the run")
	}
	if stats.Total != 0 || stats.Acked != 0 {
		t.Errorf("expected zero stats on abort, got %+v", stats)
	}
	if sink.count() != 0 {
		t.Errorf("expected no sink calls, got %d", sink.count())
	}
}

// TestRunPlantsKeyIntoPayload drives the real keyplan.Plan over a binding
// schema: the Key the record carries is the value the Payload carries at the
// path.
func TestRunPlantsKeyIntoPayload(t *testing.T) {
	schema := map[string]any{
		"type":     "object",
		"required": []any{"customer"},
		"properties": map[string]any{
			"customer": map[string]any{
				"type":       "object",
				"required":   []any{"id"},
				"properties": map[string]any{"id": map[string]any{"type": "string"}},
			},
		},
	}
	binding := map[string]any{"type": "string", "format": "uuid"}
	gen := generator.New(synth.New(1, testNow()))
	plan, err := keyplan.New(&bindingKeyGenerator{gen: gen, schema: binding},
		generator.NewKeyChecker(schema, binding, nil), "customer.id")
	if err != nil {
		t.Fatalf("keyplan.New: %v", err)
	}
	sink := &fakeSink{}
	p := New(Config{Generator: gen, Schema: schema, Count: 3, KeyPlan: plan, Encoder: JsonEncoder{}}, sink)

	stats, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Acked != 3 {
		t.Fatalf("expected 3 acked, got %+v", stats)
	}
	for i, o := range sink.recorded {
		var payload map[string]any
		if err := json.Unmarshal(o.Payload, &payload); err != nil {
			t.Fatalf("record %d: unmarshal payload: %v", i, err)
		}
		planted := payload["customer"].(map[string]any)["id"]
		if planted != string(o.Key) {
			t.Errorf("record %d: Key %q, payload holds %v at customer.id", i, o.Key, planted)
		}
	}
}

// TestRunUnhonorableKeySchemaAborts proves a key schema the generator cannot
// honour surfaces its typed error through the plan and stops the run.
func TestRunUnhonorableKeySchemaAborts(t *testing.T) {
	gen := generator.New(synth.New(1, testNow()))
	plan, err := keyplan.New(&bindingKeyGenerator{gen: gen, schema: map[string]any{"type": "widget"}}, nil, "")
	if err != nil {
		t.Fatalf("keyplan.New: %v", err)
	}
	sink := &fakeSink{}
	p := New(Config{Generator: gen, Schema: schemaFor(""), Count: 1, KeyPlan: plan, Encoder: JsonEncoder{}}, sink)

	stats, err := p.Run(context.Background())
	var ue *generator.UnsupportedSchemaError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *generator.UnsupportedSchemaError, got %v", err)
	}
	if ue.Keyword != "type" {
		t.Errorf("keyword = %q, want type", ue.Keyword)
	}
	if stats.Total != 0 || stats.Acked != 0 {
		t.Errorf("expected zero stats on abort, got %+v", stats)
	}
}

// bindingKeyGenerator binds a key schema to the generator, the way the process
// edge does.
type bindingKeyGenerator struct {
	gen    *generator.Generator
	schema map[string]any
}

func (g *bindingKeyGenerator) Value() (any, error) { return g.gen.Value(g.schema) }
