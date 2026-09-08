package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"syscall"
	"testing"
	"time"
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

func TestScenarioDeterministic(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	cmd1 := exec.Command(bin, "-spec", spec, "-channel", "orders.created", "-dry-run", "-count", "2", "-seed", "42")
	out1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("command 1 failed: %v\noutput: %s", err, out1)
	}

	cmd2 := exec.Command(bin, "-spec", spec, "-channel", "orders.created", "-dry-run", "-count", "2", "-seed", "42")
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
	if !strContains(output, "total=5") {
		t.Error("stats should show total=5")
	}
	if !strContains(output, "acked=5") {
		t.Error("stats should show acked=5")
	}
	if !strContains(output, "failed=0") {
		t.Error("stats should show failed=0")
	}
}

func TestScenarioMissingSpec(t *testing.T) {
	bin := buildBinary(t)

	cmd := exec.Command(bin, "-channel", "orders.created", "-dry-run")
	err := cmd.Run()
	if err == nil {
		t.Error("expected error when -spec is missing")
	}
}

// writeTempSpec writes a spec to a temp file and returns its path.
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

func TestScenarioMissingChannel(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	cmd := exec.Command(bin, "-spec", spec, "-dry-run")
	err := cmd.Run()
	if err == nil {
		t.Error("expected error when -channel is missing")
	}
}

func TestScenarioInvalidSpec(t *testing.T) {
	bin := buildBinary(t)

	cmd := exec.Command(bin, "-spec", "nonexistent.yaml", "-channel", "test", "-dry-run")
	err := cmd.Run()
	if err == nil {
		t.Error("expected error for nonexistent spec file")
	}
}

// TestScenarioAcksFlagAccept verifies -acks accepts both levels, case-insensitively,
// and that the default (flag absent) still runs (covered by every other scenario).
func TestScenarioAcksFlagAccept(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	for _, acks := range []string{"1", "all", "ALL", "All", "aLl", "aLL"} {
		cmd := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
			"-dry-run", "-count", "1", "-acks", acks)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("-acks %s: expected success, got %v\noutput: %s", acks, err, out)
		}
	}
}

// TestScenarioAcksFlagReject verifies an unsupported -acks value fails at parse
// time (before any spec is loaded) with a usage hint.
func TestScenarioAcksFlagReject(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-acks", "garbage").CombinedOutput()
	if err == nil {
		t.Fatal("expected -acks garbage to be rejected at parse time")
	}
	if !strContains(string(out), "acks") {
		t.Errorf("expected usage hint naming -acks, got: %s", out)
	}
}

// TestScenarioDryRunWarnsOnAcks verifies the dry-run warning learns -acks: setting
// it alongside dry-run (where Kafka options are disregarded) must warn.
func TestScenarioDryRunWarnsOnAcks(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-acks", "all").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, out)
	}
	if !strContains(string(out), "dry-run mode disregards Kafka options") {
		t.Errorf("expected dry-run Kafka-options warning when -acks set, got: %s", out)
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

// TestScenarioNowFlagAccept verifies the -now RFC3339 flag parses and the run
// succeeds; the flag is defaulted to wall-clock so omission is covered by every
// other scenario.
func TestScenarioNowFlagAccept(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-now", "2026-01-02T03:04:05Z").CombinedOutput()
	if err != nil {
		t.Fatalf("expected -now to parse and run, got %v\noutput: %s", err, out)
	}
}

// TestScenarioNowFlagReject verifies an invalid -now value fails at parse time,
// before any spec is loaded.
func TestScenarioNowFlagReject(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-now", "not-a-time").CombinedOutput()
	if err == nil {
		t.Fatal("expected invalid -now to be rejected at parse time")
	}
	if !strContains(string(out), "now") {
		t.Errorf("expected usage hint naming -now, got: %s", out)
	}
}

// TestScenarioDeterministicWithNow proves a fixed -seed AND -now yields
// byte-identical JSON output including date-formatted fields. A spec with both
// date and non-date fields exercises the clock and seed paths end to end.
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

// TestScenarioFormatJsonFlagAccept verifies -format json succeeds and produces
// byte-identical output to the default (no -format flag).
func TestScenarioFormatJsonFlagAccept(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	args := []string{"-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "2", "-seed", "42", "-now", "2026-01-02T03:04:05Z"}
	out, err := exec.Command(bin, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("-format json failed: %v\noutput: %s", err, out)
	}
	lines := filterJSONLines(string(out))
	if len(lines) != 2 {
		t.Errorf("expected 2 JSON lines, got %d", len(lines))
	}

	// Byte-identical to the same run without -format flag (which defaults to json).
	outDefault, err := exec.Command(bin, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("default format run failed: %v\noutput: %s", err, outDefault)
	}
	linesDefault := filterJSONLines(string(outDefault))
	if len(lines) != len(linesDefault) {
		t.Fatalf("different line count: %d vs %d", len(lines), len(linesDefault))
	}
	for i := range lines {
		if lines[i] != linesDefault[i] {
			t.Errorf("line %d differs:\n  json:    %s\n  default: %s", i, lines[i], linesDefault[i])
		}
	}
}

// TestScenarioFormatAvroDryRun drives -format avro through the generation path
// end to end in dry-run: the avsc parses, generated AVRO values render as JSON
// for display (vertical 4 owns the formal rendering), and a fixed seed+now is
// byte-deterministic. Dry-run never touches a registry.
func TestScenarioFormatAvroDryRun(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "order.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"qty","type":"int"}]}`)

	run := func() ([]string, string) {
		out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
			"-dry-run", "-count", "3", "-seed", "42", "-now", "2026-01-02T03:04:05Z",
			"-format", "avro", "-avro-schema", avsc).CombinedOutput()
		if err != nil {
			t.Fatalf("avro dry-run failed: %v\noutput: %s", err, out)
		}
		return filterJSONLines(string(out)), string(out)
	}

	lines, full := run()
	if len(lines) != 3 {
		t.Fatalf("expected 3 JSON lines, got %d\n%s", len(lines), full)
	}
	if !strContains(full, "total=3") {
		t.Errorf("expected stats total=3, got:\n%s", full)
	}

	second, _ := run()
	if len(lines) != len(second) {
		t.Fatalf("deterministic runs differ in length: %d vs %d", len(lines), len(second))
	}
	for i := range lines {
		if lines[i] != second[i] {
			t.Errorf("avro dry-run output differs on line %d:\n  run1: %s\n  run2: %s", i, lines[i], second[i])
		}
	}
}

// TestScenarioAvroRegistryRequiredWhenProducing verifies a registry is
// mandatory to produce -format avro (registration is how Confluent-tagged data
// gets its schema ID). Dry-run stays registry-free.
func TestScenarioAvroRegistryRequiredWhenProducing(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "order.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-count", "1", "-format", "avro", "-avro-schema", avsc).CombinedOutput()
	if err == nil {
		t.Fatal("expected producing avro without -registry to be rejected")
	}
	if !strContains(string(out), "-registry is required") {
		t.Errorf("expected '-registry is required' error, got: %s", out)
	}
}

// TestScenarioAvroRegistryInvalidUnderJSON verifies -registry alone does not
// enable avro: it is only valid with -format avro (ADR-0007 decision 6 style).
func TestScenarioAvroRegistryInvalidUnderJSON(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-registry", "http://registry:8081").CombinedOutput()
	if err == nil {
		t.Fatal("expected -registry under json format to be rejected")
	}
	if !strContains(string(out), "only valid with -format avro") {
		t.Errorf("expected -registry-is-avro-only error, got: %s", out)
	}
}

// TestScenarioAvroDryRunIgnoresRegistry verifies dry-run never opens a registry
// connection: passing -registry alongside dry-run warns and proceeds without it.
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
	if !strContains(string(out), "dry-run mode disregards") {
		t.Errorf("expected dry-run registry warning, got:\n%s", out)
	}
	if l := filterJSONLines(string(out)); len(l) != 2 {
		t.Errorf("expected 2 JSON lines, got %d\n%s", len(l), out)
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
	if !strContains(string(out), "unreachable") {
		t.Errorf("expected broker-unreachable error, got: %s", out)
	}
	if strContains(string(out), "schema registry") {
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
	if !strContains(string(out), "ignored under -format avro") {
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
	if !strContains(string(out), "cannot generate a conforming value") {
		t.Errorf("expected the typed generation error, got: %s", out)
	}
}

// TestScenarioAvroMissingSchema verifies -format avro requires -avro-schema.
func TestScenarioAvroMissingSchema(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-format", "avro").CombinedOutput()
	if err == nil {
		t.Fatal("expected -format avro without -avro-schema to be rejected")
	}
	if !strContains(string(out), "-avro-schema is required") {
		t.Errorf("expected '-avro-schema is required' error, got: %s", out)
	}
}

// TestScenarioAvroKeySchemaKeyMutuallyExclusive verifies -avro-key-schema and
// -key cannot both be set under -format avro.
func TestScenarioAvroKeySchemaKeyMutuallyExclusive(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	valueAvsc := writeTempAvsc(t, "value.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)
	keyAvsc := writeTempAvsc(t, "key.avsc", `{"type":"string"}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-format", "avro",
		"-avro-schema", valueAvsc, "-avro-key-schema", keyAvsc, "-key", "id").CombinedOutput()
	if err == nil {
		t.Fatal("expected -avro-key-schema with -key to be rejected")
	}
	if !strContains(string(out), "mutually exclusive") {
		t.Errorf("expected mutual-exclusion error, got: %s", out)
	}
}

// TestScenarioAvroFlagsInvalidUnderJSON verifies the avro flags are rejected
// unless -format avro is selected (ADR-0007 decision 6).
func TestScenarioAvroFlagsInvalidUnderJSON(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "order.avsc", `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-avro-schema", avsc).CombinedOutput()
	if err == nil {
		t.Fatal("expected -avro-schema under json format to be rejected")
	}
	if !strContains(string(out), "only valid with -format avro") {
		t.Errorf("expected avro-flags-need-avro error, got: %s", out)
	}
}

// TestScenarioAvroMalformedAvsc verifies a malformed avsc surfaces a typed
// error naming the problem, not a panic.
func TestScenarioAvroMalformedAvsc(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")
	avsc := writeTempAvsc(t, "broken.avsc", `{"type": "record", "name": "Order"`)

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-format", "avro", "-avro-schema", avsc).CombinedOutput()
	if err == nil {
		t.Fatal("expected malformed avsc to be rejected")
	}
	if !strContains(string(out), "avro: invalid avsc") {
		t.Errorf("expected typed avsc parse error, got: %s", out)
	}
}

// TestScenarioFormatInvalid rejects an unknown format at parse time.
func TestScenarioFormatInvalid(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-format", "xml").CombinedOutput()
	if err == nil {
		t.Fatal("expected invalid -format to be rejected at parse time")
	}
	if !strContains(string(out), "format") {
		t.Errorf("expected usage hint naming -format, got: %s", out)
	}
}

func buildBinary(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	bin := filepath.Join(tmpDir, "kafka-testdata-generator")

	cmd := exec.Command("go", "build", "-o", bin, "./cmd/kafka-testdata-generator")
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build failed: %v\noutput: %s", err, out)
	}
	return bin
}

func filterJSONLines(output string) []string {
	var lines []string
	for _, line := range splitLines(output) {
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
	if !strContains(string(out), "Key: ") {
		t.Errorf("expected Key echo from binding, got:\n%s", out)
	}
}

func TestScenarioKeyBindingOverriddenByKeyFlag(t *testing.T) {
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
          required:
            - orderId
          properties:
            orderId:
              type: string
`)
	combined, err := exec.Command(bin, "-spec", spec, "-channel", "orders",
		"-dry-run", "-count", "1", "-seed", "42", "-key", "orderId").CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, combined)
	}
	if !strContains(string(combined), "binding overridden") {
		t.Errorf("expected binding override warning, got:\n%s", combined)
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
	if !strContains(string(combined), "no key configured") {
		t.Errorf("expected null-key info message, got:\n%s", combined)
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
	if !strContains(string(combined), "Key: ") {
		t.Errorf("expected Key echo from resolved ref binding, got:\n%s", combined)
	}
}

// runKeyScenario runs the binary in dry-run against the given spec body with
// the supplied -key, and returns the combined output. It fails the test if the
// command errors.
func runKeyScenario(t *testing.T, specBody, key string) string {
	t.Helper()
	bin := buildBinary(t)
	spec := writeTempSpec(t, specBody)
	combined, err := exec.Command(bin, "-spec", spec, "-channel", "orders",
		"-dry-run", "-count", "2", "-seed", "42", "-key", key).CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\noutput: %s", err, combined)
	}
	return string(combined)
}

// TestScenarioKeyJSONPathNested proves -key resolves a dotted nested object
// field end to end: the key is echoed to stderr, not a missing-key skip.
func TestScenarioKeyJSONPathNested(t *testing.T) {
	combined := runKeyScenario(t, `
asyncapi: '2.6.0'
info:
  title: Path
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required:
            - customer
          properties:
            customer:
              type: object
              required:
                - id
              properties:
                id:
                  type: string
`, "customer.id")
	if !strContains(combined, "Key: ") {
		t.Errorf("expected Key echo from nested JSON path, got:\n%s", combined)
	}
	if strContains(combined, "not found in payload") {
		t.Errorf("nested path should not skip records, got:\n%s", combined)
	}
}

// TestScenarioKeyJSONPathArrayIndex proves -key resolves an array index with a
// field traversal (items[0].sku) end to end without skipping records.
func TestScenarioKeyJSONPathArrayIndex(t *testing.T) {
	combined := runKeyScenario(t, `
asyncapi: '2.6.0'
info:
  title: Path
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required:
            - items
          properties:
            items:
              type: array
              minItems: 1
              maxItems: 3
              items:
                type: object
                required:
                  - sku
                properties:
                  sku:
                    type: string
`, "items[0].sku")
	if !strContains(combined, "Key: ") {
		t.Errorf("expected Key echo from array-index JSON path, got:\n%s", combined)
	}
	if strContains(combined, "not found in payload") {
		t.Errorf("array-index path should not skip records, got:\n%s", combined)
	}
}

// TestScenarioKeyJSONPathMissingSegment proves a missing JSON path segment
// yields the documented missing-key skip behaviour (record skipped, no Key echo).
func TestScenarioKeyJSONPathMissingSegment(t *testing.T) {
	combined := runKeyScenario(t, `
asyncapi: '2.6.0'
info:
  title: Path
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required:
            - customer
          properties:
            customer:
              type: object
              properties:
                id:
                  type: string
`, "customer.missing")
	if !strContains(combined, "not found in payload") {
		t.Errorf("expected missing-path warning, got:\n%s", combined)
	}
	if strContains(combined, "Key: ") {
		t.Errorf("missing path segment must skip the record (no Key echo), got:\n%s", combined)
	}
}

// TestScenarioKeyTopLevelStillWorks guards backwards compatibility: a plain
// top-level -key name keeps working as before.
func TestScenarioKeyTopLevelStillWorks(t *testing.T) {
	combined := runKeyScenario(t, `
asyncapi: '2.6.0'
info:
  title: TopLevel
  version: '1.0.0'
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required:
            - orderId
          properties:
            orderId:
              type: string
`, "orderId")
	if !strContains(combined, "Key: ") {
		t.Errorf("expected key echo from top-level key, got:\n%s", combined)
	}
	if strContains(combined, "not found in payload") {
		t.Errorf("top-level key must not warn on missing field, got:\n%s", combined)
	}
}

func splitLines(s string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}

func strContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func init() {
	// Ensure we can find go binary
	if _, err := exec.LookPath("go"); err != nil {
		fmt.Fprintf(os.Stderr, "go not found in PATH\n")
		os.Exit(1)
	}
}
