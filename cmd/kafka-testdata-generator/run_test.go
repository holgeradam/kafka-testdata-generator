package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/runplan"
)

// closingSink records whether the process edge closed it, which is the
// property #32 is about: no failure path may skip cleanup.
type closingSink struct {
	mu     sync.Mutex
	closed bool
	sent   int
	err    error
}

func (s *closingSink) Send(ctx context.Context, o pipeline.Outgoing) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent++
	return s.err
}

func (s *closingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *closingSink) wasClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// withSink swaps the sink seam for the duration of a test.
func withSink(t *testing.T, sink pipeline.Sink) {
	t.Helper()
	original := buildSink
	buildSink = func(ctx context.Context, r *runplan.Run, stdout, stderr io.Writer) (pipeline.Sink, error) {
		return sink, nil
	}
	t.Cleanup(func() { buildSink = original })
}

func writeSpec(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "spec.yaml")
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

const runnableSpec = `
asyncapi: '2.6.0'
info: {title: Run, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required: [orderId]
          properties:
            orderId: {type: string}
`

// unhonorableSpec cannot be generated from: the Pipeline aborts on the first
// record with a typed error.
const unhonorableSpec = `
asyncapi: '2.6.0'
info: {title: Broken, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required: [thing]
          properties:
            thing: {type: widget}
`

// TestRunClosesSinkOnPipelineError is the regression this issue exists for: a
// failure after the sink is built must still close it, so the Kafka producer is
// flushed rather than leaked.
func TestRunClosesSinkOnPipelineError(t *testing.T) {
	sink := &closingSink{}
	withSink(t, sink)
	var stdout, stderr strings.Builder

	code := run(context.Background(), "ktg",
		[]string{"-spec", writeSpec(t, unhonorableSpec), "-topic", "orders", "-dry-run", "-count", "2"},
		&stdout, &stderr)

	if code != 1 {
		t.Errorf("exit code = %d, want 1 for an unhonorable schema", code)
	}
	if !sink.wasClosed() {
		t.Error("the sink was not closed on the failure path")
	}
	if !strings.Contains(stderr.String(), "unsupported type") {
		t.Errorf("stderr = %q, want the typed generation error", stderr.String())
	}
}

// TestRunClosesSinkOnSuccess pins the same property on the happy path.
func TestRunClosesSinkOnSuccess(t *testing.T) {
	sink := &closingSink{}
	withSink(t, sink)
	var stdout, stderr strings.Builder

	code := run(context.Background(), "ktg",
		[]string{"-spec", writeSpec(t, runnableSpec), "-topic", "orders", "-dry-run", "-count", "3"},
		&stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr: %s", code, stderr.String())
	}
	if !sink.wasClosed() {
		t.Error("the sink was not closed after a successful run")
	}
	if sink.sent != 3 {
		t.Errorf("sink received %d records, want 3", sink.sent)
	}
	if !strings.Contains(stderr.String(), "total=3") {
		t.Errorf("stderr = %q, want the stats line", stderr.String())
	}
}

// TestRunWritesToGivenWriters proves run() reports through its arguments rather
// than package-level stdout and stderr.
func TestRunWritesToGivenWriters(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run(context.Background(), "ktg",
		[]string{"-spec", writeSpec(t, runnableSpec), "-topic", "orders", "-dry-run", "-count", "1", "-seed", "1"},
		&stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "orderId") {
		t.Errorf("stdout = %q, want the generated payload", stdout.String())
	}
	if strings.Contains(stdout.String(), "Stats") {
		t.Errorf("stats belong on stderr, got them on stdout: %q", stdout.String())
	}
}

// TestRunUsageOnlyForFlagRules proves the usage block accompanies a rule about
// the flags themselves, and not a rejected spec, avsc or key path, whose error
// already says what is wrong - a spec refusal built from text alone included
// (#101).
func TestRunUsageOnlyForFlagRules(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		wantUsage bool
	}{
		{"missing flag", []string{"-topic", "orders"}, true},
		{"renamed flag", []string{"-spec", writeSpec(t, runnableSpec), "-topic", "orders", "-key", "orderId"}, true},
		{"unknown flag", []string{"-spec", writeSpec(t, runnableSpec), "-topic", "orders", "-bogus"}, true},
		{"missing spec file", []string{"-spec", filepath.Join(t.TempDir(), "gone.yaml"), "-topic", "orders"}, false},
		{"-format json beside -avro-schema", []string{"-spec", writeSpec(t, runnableSpec), "-topic", "orders", "-format", "json", "-avro-schema", "v.avsc"}, true},
		{"-registry in a JSON run", []string{"-spec", writeSpec(t, runnableSpec), "-topic", "orders", "-dry-run", "-registry", "http://localhost:8081"}, true},
		{"Topic parameter outside its enum", []string{"-spec", writeSpec(t, `
asyncapi: 3.0.0
info: {title: T, version: '1'}
channels:
  regional:
    address: 'orders.{region}'
    parameters: {region: {enum: [eu]}}
    messages: {created: {payload: {type: object}}}
`), "-topic", "orders.us", "-dry-run"}, false},
		{"different Key bindings", []string{"-spec", writeSpec(t, `
asyncapi: 2.6.0
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        oneOf:
          - {name: A, bindings: {kafka: {key: {type: string}}}, payload: {type: object}}
          - {name: B, payload: {type: object}}
`), "-topic", "orders", "-dry-run"}, false},
		{"mixed payload formats", []string{"-spec", writeSpec(t, `
asyncapi: 2.6.0
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        oneOf:
          - {name: A, schemaFormat: 'application/vnd.apache.avro;version=1.9.0', payload: {type: record, name: A, fields: []}}
          - {name: B, payload: {type: object}}
`), "-topic", "orders", "-dry-run"}, false},
		{"header location without headers", []string{"-spec", writeSpec(t, `
asyncapi: 3.0.0
info: {title: T, version: '1'}
channels:
  tenants:
    address: 'orders.{tenant}'
    parameters: {tenant: {location: '$message.header#/tenant'}}
    messages: {created: {payload: {type: object}}}
`), "-topic", "orders.acme", "-dry-run"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			if code := run(context.Background(), "ktg", c.args, &stdout, &stderr); code != 1 {
				t.Fatalf("exit code = %d, want 1", code)
			}
			hasUsage := strings.Contains(stderr.String(), "Usage: ktg")
			if hasUsage != c.wantUsage {
				t.Errorf("usage printed = %v, want %v\nstderr: %s", hasUsage, c.wantUsage, stderr.String())
			}
		})
	}
}

// TestRunHelp proves -h is a request, not a failure: the usage block goes to
// stdout, where it can be paged, and the run exits 0 without an error line.
func TestRunHelp(t *testing.T) {
	for _, arg := range []string{"-h", "-help"} {
		var stdout, stderr strings.Builder
		if code := run(context.Background(), "ktg", []string{arg}, &stdout, &stderr); code != 0 {
			t.Errorf("%s: exit code = %d, want 0", arg, code)
		}
		if !strings.Contains(stdout.String(), "Usage: ktg") {
			t.Errorf("%s: stdout = %q, want the usage block", arg, stdout.String())
		}
		if stderr.Len() != 0 {
			t.Errorf("%s: stderr = %q, want nothing", arg, stderr.String())
		}
	}
}

// TestRunCancelledContextStops proves an already-cancelled context ends the run
// cleanly, which is the path SIGINT takes.
func TestRunCancelledContextStops(t *testing.T) {
	sink := &closingSink{}
	withSink(t, sink)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var stdout, stderr strings.Builder
	code := run(ctx, "ktg",
		[]string{"-spec", writeSpec(t, runnableSpec), "-topic", "orders", "-dry-run", "-count", "0"},
		&stdout, &stderr)

	if code != 0 {
		t.Errorf("exit code = %d, want 0 for a cancelled run", code)
	}
	if !sink.wasClosed() {
		t.Error("the sink was not closed after cancellation")
	}
}

// TestRunSinkErrorReported proves a sink that cannot be built stops the run
// with its message and without a panic.
func TestRunSinkErrorReported(t *testing.T) {
	original := buildSink
	buildSink = func(ctx context.Context, r *runplan.Run, stdout, stderr io.Writer) (pipeline.Sink, error) {
		return nil, errors.New("broker unreachable")
	}
	t.Cleanup(func() { buildSink = original })

	var stdout, stderr strings.Builder
	code := run(context.Background(), "ktg",
		[]string{"-spec", writeSpec(t, runnableSpec), "-topic", "orders", "-count", "1"},
		&stdout, &stderr)

	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "broker unreachable") {
		t.Errorf("stderr = %q, want the sink error", stderr.String())
	}
}
