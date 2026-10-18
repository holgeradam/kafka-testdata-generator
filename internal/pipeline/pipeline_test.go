package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/logging"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
)

// fakeGenerator is a controlled PayloadGenerator: it returns a fixed Payload
// (and optional error) for every Generate call, so pipeline tests exercise Pipeline
// mechanics without loading a schema or driving an RNG.
type fakeGenerator struct {
	key     any
	payload any
	headers []pipeline.Header
	err     error
}

func (f *fakeGenerator) Generate() (pipeline.Generated, error) {
	return pipeline.Generated{Key: f.key, Payload: f.payload, Headers: f.headers}, f.err
}

// fakeEncoder is the Encoder seam's test adapter: the Payload as JSON and the
// Key as its printed form, so tests can read both back. The real adapters live
// in internal/wire and are tested there.
type fakeEncoder struct{}

func (fakeEncoder) Encode(generated pipeline.Generated) ([]byte, []byte, error) {
	var keyBytes []byte
	if generated.Key != nil {
		keyBytes = []byte(fmt.Sprint(generated.Key))
	}
	payloadBytes, err := json.Marshal(generated.Payload)
	return keyBytes, payloadBytes, err
}

type fakeSink struct {
	mu       sync.Mutex
	recorded []pipeline.Outgoing
	err      error
}

func (f *fakeSink) Send(_ context.Context, o pipeline.Outgoing) error {
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

func (b *blockingSink) Send(ctx context.Context, _ pipeline.Outgoing) error {
	close(b.blocked)
	<-ctx.Done()
	return ctx.Err()
}

func (b *blockingSink) Close() error { return nil }

func TestRunProducesCountPayloads(t *testing.T) {
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := &fakeSink{}
	p := pipeline.New(pipeline.Config{
		Generator: gen,
		Count:     3,
		Encoder:   fakeEncoder{},
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
	p := pipeline.New(pipeline.Config{Generator: gen, Count: 2, Encoder: fakeEncoder{}}, sink)

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
	p := pipeline.New(pipeline.Config{Generator: gen, Count: 100000, Encoder: fakeEncoder{}}, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan pipeline.Stats, 1)
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
// hang the pipeline. The blocked send returns the context error (matching the
// real produce path); the interrupted record was neither delivered nor failed,
// so it is not counted at all (#120 decision 5).
func TestRunCancellationInterruptsBlockedSend(t *testing.T) {
	gen := &fakeGenerator{payload: map[string]any{"id": "a"}}
	sink := newBlockingSink()
	p := pipeline.New(pipeline.Config{Generator: gen, Count: 100000, Encoder: fakeEncoder{}}, sink)

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		stats pipeline.Stats
		err   error
	}
	done := make(chan result, 1)
	go func() {
		s, err := p.Run(ctx)
		done <- result{s, err}
	}()

	// Wait until the first Send is blocked in-flight, then cancel.
	select {
	case <-sink.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("Send never blocked")
	}
	cancel()

	select {
	case r := <-done:
		if r.err != nil {
			t.Errorf("an interrupted run is not an error, got %v", r.err)
		}
		if r.stats.Total != 0 || r.stats.Acked != 0 || r.stats.Failed != 0 {
			t.Errorf("the interrupted record must not be counted, got %+v", r.stats)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel interrupting a blocked send")
	}
}

// errEncoder fails every Encode with err.
type errEncoder struct{ err error }

func (e errEncoder) Encode(pipeline.Generated) ([]byte, []byte, error) { return nil, nil, e.err }

// TestRunStopsOnTheFirstFailedRecord is #120 decision 1: by default a record
// that cannot be encoded or delivered stops the run, returning the cause with
// the Stats so far, instead of being counted and looped past.
func TestRunStopsOnTheFirstFailedRecord(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name    string
		encoder pipeline.Encoder
		sink    *fakeSink
	}{
		{"send", fakeEncoder{}, &fakeSink{err: boom}},
		{"encode", errEncoder{boom}, &fakeSink{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			log := &logging.Recorder{}
			stats, err := pipeline.New(pipeline.Config{
				Generator: &fakeGenerator{payload: map[string]any{"id": "a"}},
				Count:     3,
				Keyed:     true,
				Encoder:   c.encoder,
				Logger:    log,
			}, c.sink).Run(context.Background())

			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want the cause", err)
			}
			if stats.Total != 1 || stats.Acked != 0 || stats.Failed != 1 {
				t.Errorf("stats = %+v, want Total 1, Acked 0, Failed 1", stats)
			}
			if got := log.Entries(); len(got) != 0 {
				t.Errorf("the stopping error is returned, not also logged: %+v", got)
			}
		})
	}
}

// TestRunContinueOnErrorFinishesTheRun is #120 decisions 1 and 2:
// ContinueOnError attempts every record, logs each failure's cause at error
// level, and ends without an error; the caller reads the failures from Stats.
func TestRunContinueOnErrorFinishesTheRun(t *testing.T) {
	boom := errors.New("boom")
	log := &logging.Recorder{}
	stats, err := pipeline.New(pipeline.Config{
		Generator:       &fakeGenerator{payload: map[string]any{"id": "a"}},
		Count:           3,
		Encoder:         fakeEncoder{},
		Keyed:           true,
		ContinueOnError: true,
		Logger:          log,
	}, &fakeSink{err: boom}).Run(context.Background())

	if err != nil {
		t.Fatalf("err = %v, want nil: failures are in Stats", err)
	}
	if stats.Total != 3 || stats.Acked != 0 || stats.Failed != 3 {
		t.Errorf("stats = %+v, want Total 3, Acked 0, Failed 3", stats)
	}
	entries := log.Entries()
	if len(entries) != 3 {
		t.Fatalf("logged %d records, want one per failure (3): %+v", len(entries), entries)
	}
	for i, e := range entries {
		want := []any{"record", int64(i + 1), "stage", "sending", "error", boom}
		if e.Level != logging.Error || e.Msg != "record failed" || !reflect.DeepEqual(e.Attrs, want) {
			t.Errorf("entry %d = %+v, want an error-level %q with %v", i, e, "record failed", want)
		}
	}
}

// TestRunStopsOnConformanceErrorEvenWhenContinuing is #120 decision 3: an
// Encoder's ConformanceError signals a tool bug, not a delivery hiccup, so it
// stops the run under ContinueOnError too.
func TestRunStopsOnConformanceErrorEvenWhenContinuing(t *testing.T) {
	stats, err := pipeline.New(pipeline.Config{
		Generator:       &fakeGenerator{payload: map[string]any{"id": "a"}},
		Count:           3,
		Encoder:         errEncoder{&pipeline.ConformanceError{Err: errors.New("value does not fit the avsc")}},
		ContinueOnError: true,
	}, &fakeSink{}).Run(context.Background())

	var ce *pipeline.ConformanceError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want a *pipeline.ConformanceError", err)
	}
	if stats.Total != 1 {
		t.Errorf("stats = %+v, want the run stopped after the first record", stats)
	}
}

// TestRunPacesFailedRecords is #120 decision 6: the rate limit separates every
// attempt, failed or not, so a failing run cannot spin hot, and a
// cancellation ends the wait rather than outlasting it.
func TestRunPacesFailedRecords(t *testing.T) {
	cfg := func(rate time.Duration, count int) pipeline.Config {
		return pipeline.Config{
			Generator:       &fakeGenerator{payload: map[string]any{"id": "a"}},
			Count:           count,
			RateLimit:       rate,
			Encoder:         fakeEncoder{},
			ContinueOnError: true,
		}
	}
	sink := &fakeSink{err: errors.New("boom")}

	stats, _ := pipeline.New(cfg(30*time.Millisecond, 3), sink).Run(context.Background())
	if stats.Elapsed < 90*time.Millisecond {
		t.Errorf("3 failed records at 30ms took %s, want at least 90ms", stats.Elapsed)
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	pipeline.New(cfg(time.Hour, 3), sink).Run(ctx)
	if time.Since(start) > 5*time.Second {
		t.Error("cancel did not interrupt the rate-limit wait")
	}
}

func TestRunAbortsOnGenerationError(t *testing.T) {
	sink := &fakeSink{}
	gen := &fakeGenerator{err: &generator.UnsupportedSchemaError{Keyword: "type", Path: generator.RootPath}}
	p := pipeline.New(pipeline.Config{
		Generator: gen,
		Count:     3,
		Encoder:   fakeEncoder{},
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
	log := &logging.Recorder{}
	p := pipeline.New(pipeline.Config{
		Generator: gen,
		Count:     1,
		Encoder:   fakeEncoder{},
		Logger:    log,
	}, sink)

	stats, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Acked != 1 {
		t.Errorf("expected 1 acked, got %d", stats.Acked)
	}
	got := log.Entries()
	if len(got) != 1 || got[0].Level != logging.Info || got[0].Msg != "no key configured, generating messages with a null key" {
		t.Errorf("logged = %+v, want the null-key notice once, at info", got)
	}
	for _, o := range sink.recorded {
		if len(o.Key) != 0 {
			t.Errorf("expected nil/empty key bytes for null key, got %v", o.Key)
		}
	}
}

// TestRunCarriesTheKey proves the Pipeline encodes the Key each generated
// message carries, and announces no null Key when the run is keyed.
func TestRunCarriesTheKey(t *testing.T) {
	gen := &fakeGenerator{key: "generated-key", payload: map[string]any{"id": "a"}}
	sink := &fakeSink{}
	log := &logging.Recorder{}
	p := pipeline.New(pipeline.Config{Generator: gen, Count: 2, Keyed: true, Encoder: fakeEncoder{}, Logger: log}, sink)

	stats, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Acked != 2 {
		t.Errorf("expected 2 acked, got %d", stats.Acked)
	}
	for _, o := range sink.recorded {
		if string(o.Key) != "generated-key" {
			t.Errorf("key bytes = %q, want the message's Key encoded", o.Key)
		}
	}
	if got := log.Entries(); len(got) != 0 {
		t.Errorf("a keyed run must not announce a null key, got %+v", got)
	}
}

// TestRunCarriesHeaders proves the Headers a Wire format generated reach the
// sink with their record, untouched by the Encoder (#92).
func TestRunCarriesHeaders(t *testing.T) {
	headers := []pipeline.Header{{Name: "tenant", Value: []byte("acme")}}
	sink := &fakeSink{}
	p := pipeline.New(pipeline.Config{Generator: &fakeGenerator{payload: map[string]any{"id": "a"}, headers: headers}, Count: 2, Encoder: fakeEncoder{}}, sink)
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i, o := range sink.recorded {
		if !reflect.DeepEqual(o.Headers, headers) {
			t.Errorf("record %d: Headers = %v, want %v", i, o.Headers, headers)
		}
	}
}
