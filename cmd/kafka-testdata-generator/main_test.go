package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode"
)

func TestScenarioBasicDryRun(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	cmd := exec.Command(bin, "-spec", spec, "-channel", "orders.created", "-dry-run", "-count", "3")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, out)
	}

	// Should produce 3 JSON lines to stdout
	lines := filterJSONLines(string(out))
	if len(lines) != 3 {
		t.Errorf("expected 3 JSON lines, got %d", len(lines))
	}
}

// TestScenarioDeterministic pins both -seed and -now: without -now, date fields
// follow the wall clock and two runs straddling a second boundary differ.
func TestScenarioDeterministic(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	cmd1 := exec.Command(bin, "-spec", spec, "-channel", "orders.created", "-dry-run", "-count", "2", "-seed", "42", "-now", "2026-01-02T03:04:05Z")
	out1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("command 1 failed: %v\noutput: %s", err, out1)
	}

	cmd2 := exec.Command(bin, "-spec", spec, "-channel", "orders.created", "-dry-run", "-count", "2", "-seed", "42", "-now", "2026-01-02T03:04:05Z")
	out2, err := cmd2.CombinedOutput()
	if err != nil {
		t.Fatalf("command 2 failed: %v\noutput: %s", err, out2)
	}

	lines1 := filterJSONLines(string(out1))
	lines2 := filterJSONLines(string(out2))

	if len(lines1) != len(lines2) {
		t.Fatalf("different number of lines: %d vs %d", len(lines1), len(lines2))
	}

	for i := range lines1 {
		if lines1[i] != lines2[i] {
			t.Errorf("line %d differs:\n  run1: %s\n  run2: %s", i, lines1[i], lines2[i])
		}
	}
}

func TestScenarioPiping(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	// Generate and pipe through jq to extract orderId
	cmd := exec.Command("sh", "-c",
		bin+" -spec "+spec+" -channel orders.created -dry-run -count 2 | head -1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, out)
	}

	line := filterJSONLines(string(out))
	if len(line) == 0 {
		t.Fatal("no JSON output")
	}
}

func TestScenarioRateLimit(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	cmd := exec.Command(bin, "-spec", spec, "-channel", "orders.created", "-dry-run", "-count", "3", "-rate", "10ms")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, out)
	}

	lines := filterJSONLines(string(out))
	if len(lines) != 3 {
		t.Errorf("expected 3 JSON lines, got %d", len(lines))
	}
}

func TestScenarioStats(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	cmd := exec.Command(bin, "-spec", spec, "-channel", "orders.created", "-dry-run", "-count", "5")
	combined, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, combined)
	}

	output := string(combined)
	if !strings.Contains(output, "total=5") {
		t.Error("stats should show total=5")
	}
	if !strings.Contains(output, "acked=5") {
		t.Error("stats should show acked=5")
	}
	if !strings.Contains(output, "failed=0") {
		t.Error("stats should show failed=0")
	}
}

func writeTempSpec(t *testing.T, spec string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "recursive.yaml")
	if err := os.WriteFile(p, []byte(spec), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

// writeTempAvsc writes an avsc to a temp file and returns its path.
func writeTempAvsc(t *testing.T, name, avsc string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(avsc), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestScenarioRecursiveSpec exercises a self-referential (category-tree) spec
// end to end: it must terminate quickly at any seed, produce finite output, and
// be deterministic for a fixed seed.
func TestScenarioRecursiveSpec(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `
asyncapi: '2.6.0'
info:
  title: Categories
  version: '1.0.0'
components:
  schemas:
    Category:
      type: object
      required:
        - name
      properties:
        name:
          type: string
        children:
          type: array
          minItems: 1
          maxItems: 2
          items:
            $ref: '#/components/schemas/Category'
channels:
  categories:
    publish:
      message:
        payload:
          $ref: '#/components/schemas/Category'
`)

	seeds := []string{"1", "42", "20260831", "9999"}
	for _, seed := range seeds {
		// Termination at any seed: the command must finish quickly.
		done := make(chan error, 1)
		cmd := exec.Command(bin, "-spec", spec, "-channel", "categories",
			"-dry-run", "-count", "5", "-seed", seed)
		var out []byte
		var err error
		go func() {
			out, err = cmd.CombinedOutput()
			done <- err
		}()

		select {
		case <-done:
			if err != nil {
				t.Fatalf("seed %s: command failed: %v\noutput: %s", seed, err, out)
			}
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			t.Fatalf("seed %s: recursive spec did not terminate within 5s", seed)
		}

		lines := filterJSONLines(string(out))
		if len(lines) != 5 {
			t.Errorf("seed %s: expected 5 JSON lines, got %d", seed, len(lines))
		}
	}

	// Determinism: identical output for a fixed seed.
	runForSeed := func(seed string) []string {
		cmd := exec.Command(bin, "-spec", spec, "-channel", "categories",
			"-dry-run", "-count", "3", "-seed", seed)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("seed %s: command failed: %v", seed, err)
		}
		return filterJSONLines(string(out))
	}
	a := runForSeed("777")
	b := runForSeed("777")
	if len(a) != len(b) {
		t.Fatalf("deterministic runs differ in length: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("recursive output differs on line %d:\n  run1: %s\n  run2: %s", i, a[i], b[i])
		}
	}
}

func TestScenarioDryRunWarnsOnAcks(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-acks", "all").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "dry-run mode disregards Kafka options") {
		t.Errorf("expected dry-run Kafka-options warning when -acks set, got: %s", out)
	}
}

// TestScenarioDryRunKeyDoesNotWarn verifies -keyPath is not reported as
// disregarded in dry run: the Key is echoed (ADR-0003), so the warning must
// stay silent. Both facts are asserted on the same output so they cannot
// contradict.
func TestScenarioDryRunKeyDoesNotWarn(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, keyPathSpec)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders",
		"-dry-run", "-count", "2", "-seed", "42", "-keyPath", "orderId").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Key: ") {
		t.Errorf("expected Key echo in dry run, got: %s", out)
	}
	if strings.Contains(string(out), "dry-run mode disregards") {
		t.Errorf("dry run honours -key, so it must not warn that it is disregarded, got: %s", out)
	}
}

// TestScenarioDryRunWarnsOnExplicitBroker verifies the warning fires whenever
// -broker is passed in dry run, even with a value equal to the default.
func TestScenarioDryRunWarnsOnExplicitBroker(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-broker", "localhost:9092").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "dry-run mode disregards Kafka options") {
		t.Errorf("expected dry-run Kafka-options warning when -broker set, got: %s", out)
	}
}

func TestScenarioSignalHandling(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	cmd := exec.Command(bin, "-spec", spec, "-channel", "orders.created", "-dry-run", "-count", "100")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start command: %v", err)
	}

	// Send SIGINT after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cmd.Process.Signal(syscall.SIGINT)
	}()

	// Wait should complete quickly after signal
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
		// Process exited - the important thing is that it didn't hang
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		t.Fatal("command did not exit after SIGINT within 5 seconds")
	}
}

func TestScenarioDeterministicWithNow(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `
asyncapi: '2.6.0'
info:
  title: Mixed
  version: '1.0.0'
channels:
  mixed:
    publish:
      message:
        payload:
          type: object
          required:
            - name
            - created
          properties:
            name:
              type: string
            created:
              type: string
              format: date
            updated:
              type: string
              format: date-time
`)

	run := func() []string {
		cmd := exec.Command(bin, "-spec", spec, "-channel", "mixed",
			"-dry-run", "-count", "3", "-seed", "42", "-now", "2026-01-02T03:04:05Z")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("command failed: %v", err)
		}
		return filterJSONLines(string(out))
	}

	lines1, lines2 := run(), run()
	if len(lines1) != len(lines2) {
		t.Fatalf("different number of lines: %d vs %d", len(lines1), len(lines2))
	}
	for i := range lines1 {
		if lines1[i] != lines2[i] {
			t.Errorf("line %d differs:\n  run1: %s\n  run2: %s", i, lines1[i], lines2[i])
		}
	}

	// A different -now must change date fields but keep seed-driven fields identical.
	runWithNow := func(now string) []string {
		cmd := exec.Command(bin, "-spec", spec, "-channel", "mixed",
			"-dry-run", "-count", "3", "-seed", "42", "-now", now)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("command failed: %v", err)
		}
		return filterJSONLines(string(out))
	}

	linesA := runWithNow("2026-01-02T03:04:05Z")
	linesB := runWithNow("2025-06-15T10:00:00Z")
	if len(linesA) != len(linesB) {
		t.Fatalf("different number of lines: %d vs %d", len(linesA), len(linesB))
	}
	for i := range linesA {
		var objA, objB map[string]any
		if err := json.Unmarshal([]byte(linesA[i]), &objA); err != nil {
			t.Fatalf("line %d: invalid JSON: %v", i, err)
		}
		if err := json.Unmarshal([]byte(linesB[i]), &objB); err != nil {
			t.Fatalf("line %d: invalid JSON: %v", i, err)
		}
		if objA["name"] != objB["name"] {
			t.Errorf("line %d: seed-driven field 'name' differs: %v vs %v", i, objA["name"], objB["name"])
		}
		if objA["created"] == objB["created"] {
			t.Errorf("line %d: date field 'created' must differ for different -now", i)
		}
	}
}

// TestScenarioSKUConforms proves the example spec's SKU pattern
// (^[A-Z]{3}-[A-Z]{2}-\d{4}$) is honoured end to end: every generated sku value
// must match it. This is the 'no silently-violating classes' acceptance case.
func TestScenarioSKUConforms(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "10").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, out)
	}

	skuRe := regexp.MustCompile(`"sku":"([^"]+)"`)
	matches := skuRe.FindAllStringSubmatch(string(out), -1)
	if len(matches) == 0 {
		t.Fatal("no sku values found in output")
	}
	pattern, cerr := regexp.Compile(`^[A-Z]{3}-[A-Z]{2}-\d{4}$`)
	if cerr != nil {
		t.Fatal(cerr)
	}
	for _, m := range matches {
		if !pattern.MatchString(m[1]) {
			t.Errorf("sku %q does not match pattern ^[A-Z]{3}-[A-Z]{2}-\\d{4}$", m[1])
		}
	}
}

func TestScenarioFormatAvroDryRunRendersAvroJSON(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "rich.avsc", `{"type":"record","name":"Order","fields":[
		{"name":"id","type":"string"},
		{"name":"qty","type":"int"},
		{"name":"day","type":{"type":"int","logicalType":"date"}},
		{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}
	]}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "3", "-seed", "42", "-now", "2026-01-02T03:04:05Z",
		"-format", "avro", "-avro-schema", avsc).CombinedOutput()
	if err != nil {
		t.Fatalf("avro dry-run failed: %v\noutput: %s", err, out)
	}
	if !strings.Contains(string(out), "total=3") {
		t.Errorf("expected stats total=3, got:\n%s", out)
	}

	lines := filterJSONLines(string(out))
	if len(lines) != 3 {
		t.Fatalf("expected 3 AVRO JSON lines, got %d\n%s", len(lines), out)
	}
	// Dates render as readable calendars days, decimals as base-10 strings,
	// never as a raw byte blob or a numeric timestamp.
	if !strings.Contains(string(out), `"day":"`) {
		t.Errorf("date must render as a readable calendar string, got:\n%s", out)
	}
	if !strings.Contains(string(out), `"amt":"`) {
		t.Errorf("decimal must render as a base-10 string, got:\n%s", out)
	}
	if strings.Contains(string(out), "AA==") {
		t.Errorf("bytes must not render as base64, got:\n%s", out)
	}
}

// TestScenarioFormatAvroDryRunDeterministic proves the formal rendering keeps
// the fixed seed+now byte-determinism of the AVRO generation path.
func TestScenarioFormatAvroDryRunDeterministic(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "order.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"qty","type":"int"}]}`)

	run := func() []string {
		out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
			"-dry-run", "-count", "3", "-seed", "42", "-now", "2026-01-02T03:04:05Z",
			"-format", "avro", "-avro-schema", avsc).CombinedOutput()
		if err != nil {
			t.Fatalf("avro dry-run failed: %v\noutput: %s", err, out)
		}
		return filterJSONLines(string(out))
	}

	lines := run()
	if len(lines) != 3 {
		t.Fatalf("expected 3 AVRO JSON lines, got %d", len(lines))
	}
	second := run()
	if len(lines) != len(second) {
		t.Fatalf("deterministic runs differ in length: %d vs %d", len(lines), len(second))
	}
	for i := range lines {
		if lines[i] != second[i] {
			t.Errorf("avro dry-run output differs on line %d:\n  run1: %s\n  run2: %s", i, lines[i], second[i])
		}
	}
}

func TestScenarioAvroDryRunIgnoresRegistry(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "order.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "2", "-format", "avro", "-avro-schema", avsc,
		"-registry", "http://127.0.0.1:1").CombinedOutput()
	if err != nil {
		t.Fatalf("avro dry-run with -registry must succeed, got %v\noutput: %s", err, out)
	}
	if !strings.Contains(string(out), "dry-run mode disregards") {
		t.Errorf("expected dry-run registry warning, got:\n%s", out)
	}
	if l := filterJSONLines(string(out)); len(l) != 2 {
		t.Errorf("expected 2 JSON lines, got %d\n%s", len(l), out)
	}
}

// TestScenarioAvroDryRunDoesNotContactRegistry proves the acceptance case 'no
// registry HTTP occurs in Dry run' with a live tripwire: a fake registry that
// fails the test if the binary so much as connects. The run must succeed, emit
// AVRO JSON, and leave the request counter at zero.
func TestScenarioAvroDryRunDoesNotContactRegistry(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "order.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)

	var mu sync.Mutex
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "2", "-format", "avro", "-avro-schema", avsc,
		"-registry", srv.URL).CombinedOutput()
	if err != nil {
		t.Fatalf("avro dry-run must not depend on the registry: %v\noutput: %s", err, out)
	}
	if !strings.Contains(string(out), "dry-run mode disregards") {
		t.Errorf("expected dry-run registry warning, got:\n%s", out)
	}
	if l := filterJSONLines(string(out)); len(l) != 2 {
		t.Errorf("expected 2 AVRO JSON lines, got %d\n%s", len(l), out)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Errorf("dry-run must never contact the registry, got %d HTTP requests", hits)
	}
}

// TestScenarioAvroProduceContactsBrokerNotRegistry verifies the produce path
// wires -registry into the AvroEncoder without short-circuiting: with an
// unreachable broker the run fails at the broker ping, never on registry flag
// handling (registration itself is covered by the pipeline AvroEncoder tests).
func TestScenarioAvroProduceContactsBrokerNotRegistry(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "order.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-count", "1", "-format", "avro", "-avro-schema", avsc,
		"-broker", "127.0.0.1:1", "-registry", "http://127.0.0.1:1").CombinedOutput()
	if err == nil {
		t.Fatal("expected the unreachable broker to fail the run")
	}
	if !strings.Contains(string(out), "unreachable") {
		t.Errorf("expected broker-unreachable error, got: %s", out)
	}
	if strings.Contains(string(out), "schema registry") {
		t.Errorf("run should fail at the broker ping before any registry call, got: %s", out)
	}
}

// TestScenarioAvroKeyBindingIgnored verifies spec key bindings are ignored under
// -format avro (they generate JSON values, not AVRO-natural keys): a warning
// names the override and the run proceeds.
func TestScenarioAvroKeyBindingIgnored(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `
asyncapi: '2.6.0'
info:
  title: Bindings
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        bindings:
          kafka:
            key:
              type: string
        payload:
          type: object
          properties:
            id:
              type: string
`)
	avsc := writeTempAvsc(t, "order.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders",
		"-dry-run", "-count", "1", "-format", "avro", "-avro-schema", avsc).CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, out)
	}
	if !strings.Contains(string(out), "ignored under -format avro") {
		t.Errorf("expected key-binding ignored warning, got:\n%s", out)
	}
}

// TestScenarioAvroUnhonorableAvsc verifies an avsc the generator cannot honour
// stops the run with the typed generation error instead of emitting data that
// violates the avsc (issue #22 AC4, ADR-0006/0007): a record validly referencing
// itself with no null escape can never terminate, so generation must fail.
func TestScenarioAvroUnhonorableAvsc(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "cycle.avsc", `{"type":"record","name":"Node","fields":[{"name":"next","type":"Node"}]}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "2", "-seed", "1", "-format", "avro", "-avro-schema", avsc).CombinedOutput()
	if err == nil {
		t.Fatal("expected an unhonorable avsc to fail the run")
	}
	if !strings.Contains(string(out), "cannot generate a conforming value") {
		t.Errorf("expected the typed generation error, got: %s", out)
	}
}

func TestScenarioAvroKeySchemaDryRunGeneratesKey(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	valueAvsc := writeTempAvsc(t, "value.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)
	keyAvsc := writeTempAvsc(t, "key.avsc", `{"type":"string"}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "2", "-seed", "42", "-format", "avro",
		"-avro-schema", valueAvsc, "-avro-key-schema", keyAvsc).CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, out)
	}
	if !strings.Contains(string(out), "Key: ") {
		t.Errorf("expected a Key echo for -avro-key-schema under avro dry-run, got:\n%s", out)
	}
	if l := filterJSONLines(string(out)); len(l) != 2 {
		t.Errorf("expected 2 AVRO JSON payload lines, got %d\n%s", len(l), out)
	}
	if strings.Contains(string(out), "not yet implemented") {
		t.Errorf("vertical-3 stopgap warning must be gone, got:\n%s", out)
	}
	if strings.Contains(string(out), "no key configured") {
		t.Errorf("key avsc configured: no null-key warning expected, got:\n%s", out)
	}
}

// testBinary is built once per package run: the scenarios below exercise the
// real process, but they all exercise the same build.
var testBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ktg-e2e")
	if err != nil {
		fmt.Fprintf(os.Stderr, "temp dir: %v\n", err)
		os.Exit(1)
	}
	testBinary = filepath.Join(dir, "kafka-testdata-generator")
	build := exec.Command("go", "build", "-o", testBinary, "./cmd/kafka-testdata-generator")
	build.Dir = filepath.Join("..", "..")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s", err, out)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// buildBinary returns the binary built once for this package.
func buildBinary(t *testing.T) string {
	t.Helper()
	return testBinary
}

func filterJSONLines(output string) []string {
	var lines []string
	for _, line := range strings.Split(output, "\n") {
		if len(line) > 0 && line[0] == '{' {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestScenarioKeyBindingGeneratesKey(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `
asyncapi: '2.6.0'
info:
  title: Bindings
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        bindings:
          kafka:
            key:
              type: string
        payload:
          type: object
          properties:
            id:
              type: string
`)
	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders",
		"-dry-run", "-count", "2", "-seed", "42").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, out)
	}
	// Key lines go to stderr via CombinedOutput; stdout has NDJSON payloads.
	// The key binding should produce non-empty keys (visible as Key: lines).
	if !strings.Contains(string(out), "Key: ") {
		t.Errorf("expected Key echo from binding, got:\n%s", out)
	}
}

func TestScenarioNullKeyInfoMessage(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `
asyncapi: '2.6.0'
info:
  title: NoKey
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          properties:
            id:
              type: string
`)
	combined, err := exec.Command(bin, "-spec", spec, "-channel", "orders",
		"-dry-run", "-count", "1", "-seed", "42").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, combined)
	}
	if !strings.Contains(string(combined), "no key configured, generating messages with a null key") {
		t.Errorf("expected mode-neutral null-key info message, got:\n%s", combined)
	}
	if strings.Contains(string(combined), "producing") {
		t.Errorf("dry run produces nothing, so the info message must not say producing, got:\n%s", combined)
	}
}

func TestScenarioKeyBindingResolvesRef(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `
asyncapi: '2.6.0'
info:
  title: RefBinding
  version: '1.0.0'
components:
  schemas:
    OrderKey:
      type: string
      format: uuid
channels:
  orders:
    publish:
      message:
        bindings:
          kafka:
            key:
              $ref: '#/components/schemas/OrderKey'
        payload:
          type: object
          properties:
            id:
              type: string
`)
	combined, err := exec.Command(bin, "-spec", spec, "-channel", "orders",
		"-dry-run", "-count", "2", "-seed", "42").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, combined)
	}
	// UUID-formatted keys should be echoed from the binding.
	if !strings.Contains(string(combined), "Key: ") {
		t.Errorf("expected Key echo from resolved ref binding, got:\n%s", combined)
	}
}

func init() {
	// Ensure we can find go binary
	if _, err := exec.LookPath("go"); err != nil {
		fmt.Fprintf(os.Stderr, "go not found in PATH\n")
		os.Exit(1)
	}
}

// TestScenarioHeuristicsAgreeAcrossFormats is the #28 acceptance test: a JSON
// Schema and an avsc declaring the same string fields yield the same values for
// the same -seed and -now, because both walkers ask the one Synthesizer. Fields
// are declared in the order both walkers visit them (sorted, all required), so
// the draw sequences line up exactly.
func TestScenarioHeuristicsAgreeAcrossFormats(t *testing.T) {
	bin := buildBinary(t)
	fields := []string{"city", "country", "currency", "customerName", "description", "email", "orderId", "status", "street", "websiteUrl"}

	spec := "asyncapi: '2.6.0'\ninfo: {title: Same, version: '1.0.0'}\nchannels:\n  orders:\n    publish:\n      message:\n        payload:\n          type: object\n          required: [" + strings.Join(fields, ", ") + "]\n          properties:\n"
	var avscFields []string
	for _, f := range fields {
		spec += "            " + f + ": {type: string}\n"
		avscFields = append(avscFields, `{"name":"`+f+`","type":"string"}`)
	}
	specPath := writeTempSpec(t, spec)
	avsc := writeTempAvsc(t, "same.avsc", `{"type":"record","name":"Same","fields":[`+strings.Join(avscFields, ",")+`]}`)

	run := func(extra ...string) map[string]any {
		args := append([]string{"-spec", specPath, "-channel", "orders", "-dry-run", "-count", "1",
			"-seed", "1", "-now", "2026-09-16T00:00:00Z"}, extra...)
		out, err := exec.Command(bin, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("command failed: %v\noutput: %s", err, out)
		}
		lines := filterJSONLines(string(out))
		if len(lines) != 1 {
			t.Fatalf("expected 1 JSON line, got %d\n%s", len(lines), out)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
			t.Fatalf("unmarshal %q: %v", lines[0], err)
		}
		return m
	}
	jsonOut := run()
	avroOut := run("-format", "avro", "-avro-schema", avsc)

	random := regexp.MustCompile(`^[a-z0-9]{8}$`)
	for _, f := range fields {
		if jsonOut[f] != avroOut[f] {
			t.Errorf("%s: JSON %q, AVRO %q; want identical values", f, jsonOut[f], avroOut[f])
		}
		if s, _ := avroOut[f].(string); random.MatchString(s) {
			t.Errorf("%s: AVRO value %q is random text, want a heuristic value", f, s)
		}
	}
}

// TestScenarioAvroKeyAndPayloadShareOneStream proves Payload and Key draw from
// one Synthesizer per run (#31 decision 4): with a string key avsc and a single
// string payload field, the two values no longer mirror each other.
func TestScenarioAvroKeyAndPayloadShareOneStream(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	valueAvsc := writeTempAvsc(t, "value.avsc", `{"type":"record","name":"Note","fields":[{"name":"note","type":"string"}]}`)
	keyAvsc := writeTempAvsc(t, "key.avsc", `{"type":"string"}`)

	cmd := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-seed", "42", "-format", "avro",
		"-avro-schema", valueAvsc, "-avro-key-schema", keyAvsc)
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("command failed: %v\nstderr: %s", err, stderr.String())
	}
	keys := avroKeys(t, stderr.String())
	if len(keys) != 1 {
		t.Fatalf("expected one Key echo in stderr:\n%s", stderr.String())
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &payload); err != nil {
		t.Fatalf("unmarshal payload %q: %v", stdout.String(), err)
	}
	if payload["note"] == keys[0] {
		t.Errorf("Key %q mirrors the Payload's first draw %q; want one shared stream", keys[0], payload["note"])
	}
}

// TestScenarioExtendedHeuristicsBothFormats proves the four extension bundles
// (#43) reach both wire formats through the one Synthesizer: identical values
// for the same field names, seed and now, and none of them random text.
func TestScenarioExtendedHeuristicsBothFormats(t *testing.T) {
	bin := buildBinary(t)
	fields := []string{"company", "hostname", "iban", "ip", "jobTitle", "language", "postalCode", "state", "timezone", "username"}

	spec := "asyncapi: '2.6.0'\ninfo: {title: Ext, version: '1.0.0'}\nchannels:\n  orders:\n    publish:\n      message:\n        payload:\n          type: object\n          required: [" + strings.Join(fields, ", ") + "]\n          properties:\n"
	var avscFields []string
	for _, f := range fields {
		spec += "            " + f + ": {type: string}\n"
		avscFields = append(avscFields, `{"name":"`+f+`","type":"string"}`)
	}
	specPath := writeTempSpec(t, spec)
	avsc := writeTempAvsc(t, "ext.avsc", `{"type":"record","name":"Ext","fields":[`+strings.Join(avscFields, ",")+`]}`)

	run := func(extra ...string) map[string]any {
		args := append([]string{"-spec", specPath, "-channel", "orders", "-dry-run", "-count", "1",
			"-seed", "5", "-now", "2026-09-18T00:00:00Z"}, extra...)
		out, err := exec.Command(bin, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("command failed: %v\noutput: %s", err, out)
		}
		lines := filterJSONLines(string(out))
		if len(lines) != 1 {
			t.Fatalf("expected 1 JSON line, got %d\n%s", len(lines), out)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
			t.Fatalf("unmarshal %q: %v", lines[0], err)
		}
		return m
	}
	jsonOut := run()
	avroOut := run("-format", "avro", "-avro-schema", avsc)

	random := regexp.MustCompile(`^[a-z0-9]{8}$`)
	for _, f := range fields {
		if jsonOut[f] != avroOut[f] {
			t.Errorf("%s: JSON %q, AVRO %q; want identical values", f, jsonOut[f], avroOut[f])
		}
		if s, _ := jsonOut[f].(string); random.MatchString(s) {
			t.Errorf("%s: value %q is random text, want a heuristic value", f, s)
		}
	}
}

// keyPathSpec is the fixture for the Key plan scenarios: a kafka key binding
// (the key schema) plus a payload whose paths are guaranteed, so -keyPath has
// somewhere to plant into.
const keyPathSpec = `
asyncapi: '2.6.0'
info: {title: Keys, version: '1.0.0'}
channels:
  orders:
    publish:
      message:
        bindings:
          kafka:
            key:
              type: string
        payload:
          type: object
          required: [orderId, customer, items, total]
          properties:
            orderId: {type: string}
            nickname: {type: string}
            total: {type: number}
            customer:
              type: object
              required: [id]
              properties:
                id: {type: string}
            items:
              type: array
              minItems: 2
              items:
                type: object
                required: [sku]
                properties:
                  sku: {type: string}
`

// keyAt reads the value the payload carries at a dotted path, for comparing it
// with the echoed Key.
func keyAt(t *testing.T, payload map[string]any, path string) any {
	t.Helper()
	var current any = payload
	for _, step := range strings.Split(path, ".") {
		if i := strings.Index(step, "["); i >= 0 {
			idx, err := strconv.Atoi(strings.TrimSuffix(step[i+1:], "]"))
			if err != nil {
				t.Fatalf("bad path %q", path)
			}
			current = current.(map[string]any)[step[:i]].([]any)[idx]
			continue
		}
		current = current.(map[string]any)[step]
	}
	return current
}

// TestScenarioKeyPathPlantsIntoPayload is the acceptance case of #51: the Key
// generated from the key schema is planted at -keyPath, so every record's Key
// equals the value the Payload carries there.
func TestScenarioKeyPathPlantsIntoPayload(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, keyPathSpec)

	for _, path := range []string{"orderId", "customer.id", "items[1].sku"} {
		t.Run(path, func(t *testing.T) {
			cmd := exec.Command(bin, "-spec", spec, "-channel", "orders",
				"-dry-run", "-count", "3", "-seed", "42", "-keyPath", path)
			var stdout, stderr strings.Builder
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("command failed: %v\nstderr: %s", err, stderr.String())
			}
			keys := regexp.MustCompile(`Key: (.+)`).FindAllStringSubmatch(stderr.String(), -1)
			lines := filterJSONLines(stdout.String())
			if len(keys) != 3 || len(lines) != 3 {
				t.Fatalf("expected 3 keys and 3 payloads, got %d and %d\nstderr: %s", len(keys), len(lines), stderr.String())
			}
			for i, line := range lines {
				var payload map[string]any
				if err := json.Unmarshal([]byte(line), &payload); err != nil {
					t.Fatalf("record %d: unmarshal: %v", i, err)
				}
				if got := keyAt(t, payload, path); got != keys[i][1] {
					t.Errorf("record %d: Key %q, payload holds %v at %s", i, keys[i][1], got, path)
				}
			}
		})
	}
}

func TestScenarioAvroKeyPathPlantsIntoPayload(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	valueAvsc := writeTempAvsc(t, "value.avsc", `{"type":"record","name":"Order","fields":[
		{"name":"ref","type":{"type":"string","logicalType":"uuid"}},
		{"name":"customer","type":{"type":"record","name":"Customer","fields":[{"name":"id","type":"string"}]}}]}`)
	keyAvsc := writeTempAvsc(t, "key.avsc", `{"type":"string"}`)

	for _, path := range []string{"ref", "customer.id"} {
		t.Run(path, func(t *testing.T) {
			key := keyAvsc
			if path == "ref" {
				key = writeTempAvsc(t, "uuidkey.avsc", `{"type":"string","logicalType":"uuid"}`)
			}
			cmd := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
				"-dry-run", "-count", "3", "-seed", "9", "-format", "avro",
				"-avro-schema", valueAvsc, "-avro-key-schema", key, "-keyPath", path)
			var stdout, stderr strings.Builder
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("command failed: %v\nstderr: %s", err, stderr.String())
			}
			keys := avroKeys(t, stderr.String())
			lines := filterJSONLines(stdout.String())
			if len(keys) != 3 || len(lines) != 3 {
				t.Fatalf("expected 3 keys and 3 payloads, got %d and %d\nstderr: %s", len(keys), len(lines), stderr.String())
			}
			for i, line := range lines {
				var payload map[string]any
				if err := json.Unmarshal([]byte(line), &payload); err != nil {
					t.Fatalf("record %d: unmarshal: %v", i, err)
				}
				if got := keyAt(t, payload, path); got != keys[i] {
					t.Errorf("record %d: Key %v, payload holds %v at %s", i, keys[i], got, path)
				}
			}
		})
	}
}

// avroKeys decodes every "Key: " echo of an AVRO Dry run, which prints the
// Key in the Avro JSON encoding of the key avsc (#64).
func avroKeys(t *testing.T, stderr string) []any {
	t.Helper()
	var keys []any
	for _, m := range regexp.MustCompile(`(?m)^Key: (.+)$`).FindAllStringSubmatch(stderr, -1) {
		var k any
		if err := json.Unmarshal([]byte(m[1]), &k); err != nil {
			t.Fatalf("Key echo %q is not Avro JSON: %v", m[1], err)
		}
		keys = append(keys, k)
	}
	return keys
}

// TestScenarioAvroRecordKeyDryRun is the end-to-end reproduction of #64: a
// record Key shows in the same Avro JSON encoding as the Payload, not as the
// generator's Go values (a big.Rat fraction, a union wrapper, a byte array).
func TestScenarioAvroRecordKeyDryRun(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	valueAvsc := writeTempAvsc(t, "value.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)
	keyAvsc := writeTempAvsc(t, "key.avsc", `{"type":"record","name":"OrderKey","fields":[
		{"name":"region","type":["null","string"]},
		{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}},
		{"name":"tag","type":{"type":"fixed","name":"Tag","size":2}}]}`)

	cmd := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "5", "-seed", "3", "-format", "avro",
		"-avro-schema", valueAvsc, "-avro-key-schema", keyAvsc)
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("command failed: %v\nstderr: %s", err, stderr.String())
	}
	// Bytes 0x7F-0x9F are invisible when printed raw; they must be escaped
	// (#68). With this seed the second Key's fixed holds bytes 150,153.
	for _, r := range stdout.String() + stderr.String() {
		if r != '\n' && !unicode.IsPrint(r) {
			t.Errorf("Dry-run output holds the non-printable character %U", r)
		}
	}
	keys := avroKeys(t, stderr.String())
	if len(keys) != 5 {
		t.Fatalf("expected 5 Key echoes, got %d\nstderr: %s", len(keys), stderr.String())
	}
	decimal := regexp.MustCompile(`^\d+\.\d{2}$`)
	for i, k := range keys {
		key, ok := k.(map[string]any)
		if !ok {
			t.Fatalf("record %d: Key %v is not a record", i, k)
		}
		if s, _ := key["amount"].(string); !decimal.MatchString(s) {
			t.Errorf("record %d: amount %v, want base-10 text with scale 2", i, key["amount"])
		}
		if r := key["region"]; r != nil {
			if _, ok := r.(string); !ok {
				t.Errorf("record %d: region %v, want the union's active branch, unwrapped", i, r)
			}
		}
		if s, _ := key["tag"].(string); len([]rune(s)) != 2 {
			t.Errorf("record %d: tag %v, want a 2-character Latin-1 string", i, key["tag"])
		}
	}
}
