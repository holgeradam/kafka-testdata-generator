package main

import (
	"context"

	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
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

	cmd := exec.Command(bin, "-spec", spec, "-topic", "orders.created", "-dry-run", "-count", "3")
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

	cmd1 := exec.Command(bin, "-spec", spec, "-topic", "orders.created", "-dry-run", "-count", "2", "-seed", "42", "-now", "2026-01-02T03:04:05Z")
	out1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("command 1 failed: %v\noutput: %s", err, out1)
	}

	cmd2 := exec.Command(bin, "-spec", spec, "-topic", "orders.created", "-dry-run", "-count", "2", "-seed", "42", "-now", "2026-01-02T03:04:05Z")
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

// TestScenarioV3ExampleMatchesV2 is #82 end to end: the AsyncAPI 3.0 example
// restates the 2.x one, so the same seed and clock give the same output.
func TestScenarioV3ExampleMatchesV2(t *testing.T) {
	bin := buildBinary(t)
	run := func(spec string) string {
		t.Helper()
		out, err := exec.Command(bin, "-spec", filepath.Join("..", "..", "examples", spec), "-topic", "orders.created",
			"-dry-run", "-count", "20", "-seed", "42", "-now", "2026-01-02T03:04:05Z").Output()
		if err != nil {
			t.Fatalf("%s: %v\n%s", spec, err, out)
		}
		return string(out)
	}
	v2, v3 := run("order.asyncapi.yaml"), run("order.asyncapi.v3.yaml")
	if len(filterJSONLines(v3)) != 20 {
		t.Fatalf("3.0 example: want 20 records, got:\n%s", v3)
	}
	if v2 != v3 {
		t.Errorf("outputs differ:\n2.x:\n%s\n3.0:\n%s", v2, v3)
	}
}

// TestScenarioTopicParameters is #83 end to end: -topic fills a templated 3.0
// address, and the Topic parameter's value is planted in every record of
// every Message type; a value outside the parameter's enum, or a header
// location in a Message type without headers, stops the run before a record
// exists.
func TestScenarioTopicParameters(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `asyncapi: 3.0.0
info: {title: Orders, version: '1'}
channels:
  regional:
    address: 'orders.{region}'
    parameters:
      region: {enum: [eu, us], location: '$message.payload#/region'}
    messages:
      created:
        name: OrderCreated
        payload: {type: object, required: [kind, region], properties: {kind: {const: created}, region: {type: string, pattern: '^[a-z]{2}$'}}}
      updated:
        name: OrderUpdated
        payload: {type: object, required: [kind, region], properties: {kind: {const: updated}, region: {type: string}}}
  tenants:
    address: 'tenants.{tenant}'
    parameters:
      tenant: {location: '$message.header#/tenant'}
    messages: {created: {payload: {type: object}}}
`)
	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.eu", "-dry-run", "-count", "20", "-seed", "1").Output()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	kinds := map[string]bool{}
	for i, line := range filterJSONLines(string(out)) {
		var p map[string]any
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		kinds[fmt.Sprint(p["kind"])] = true
		if p["region"] != "eu" {
			t.Errorf("record %d: region = %v, want eu", i, p["region"])
		}
	}
	if len(kinds) != 2 {
		t.Errorf("Message types = %v, want both", kinds)
	}

	for topic, want := range map[string]string{
		"orders.apac":  `Kafka topic "orders.apac": region value apac is not in the parameter's enum [eu, us]`,
		"tenants.acme": "Topic parameter tenant: location $message.header#/tenant: created declares no headers",
	} {
		out, err := exec.Command(bin, "-spec", spec, "-topic", topic, "-dry-run", "-count", "1").CombinedOutput()
		if err == nil {
			t.Fatalf("%s: expected the run to stop, got:\n%s", topic, out)
		}
		if !strings.Contains(string(out), want) {
			t.Errorf("%s: output lacks %q:\n%s", topic, want, out)
		}
	}
}

// TestScenarioTopicParameters2 is #88 end to end: -topic fills a templated
// 2.x spec entry key, and the Topic parameter's value is planted in every
// record of every Message type; a value the parameter's schema refuses, or a
// parameter schema that is not a string, stops the run before a record exists.
func TestScenarioTopicParameters2(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `asyncapi: 2.6.0
info: {title: Orders, version: '1'}
channels:
  orders.{region}:
    parameters:
      region: {schema: {type: string, enum: [eu, us]}, location: '$message.payload#/region'}
    publish:
      message:
        oneOf:
          - name: OrderCreated
            payload: {type: object, required: [kind, region], properties: {kind: {const: created}, region: {type: string, pattern: '^[a-z]{2}$'}}}
          - name: OrderUpdated
            payload: {type: object, required: [kind, region], properties: {kind: {const: updated}, region: {type: string}}}
  users.{userId}:
    parameters:
      userId: {schema: {type: integer}}
    publish: {message: {payload: {type: object}}}
`)
	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.eu", "-dry-run", "-count", "20", "-seed", "1").Output()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	kinds := map[string]bool{}
	for i, line := range filterJSONLines(string(out)) {
		var p map[string]any
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		kinds[fmt.Sprint(p["kind"])] = true
		if p["region"] != "eu" {
			t.Errorf("record %d: region = %v, want eu", i, p["region"])
		}
	}
	if len(kinds) != 2 {
		t.Errorf("Message types = %v, want both", kinds)
	}

	for topic, want := range map[string]string{
		"orders.apac": `Kafka topic "orders.apac": region value apac does not conform to the parameter's schema`,
		"users.42":    "parameter userId: its schema does not allow a string, and a Topic parameter is a string",
	} {
		out, err := exec.Command(bin, "-spec", spec, "-topic", topic, "-dry-run", "-count", "1").CombinedOutput()
		if err == nil {
			t.Fatalf("%s: expected the run to stop, got:\n%s", topic, out)
		}
		if !strings.Contains(string(out), want) {
			t.Errorf("%s: output lacks %q:\n%s", topic, want, out)
		}
	}
}

func TestScenarioPiping(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	// Generate and pipe through jq to extract orderId
	cmd := exec.Command("sh", "-c",
		bin+" -spec "+spec+" -topic orders.created -dry-run -count 2 | head -1")
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

	cmd := exec.Command(bin, "-spec", spec, "-topic", "orders.created", "-dry-run", "-count", "3", "-rate", "10ms")
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

	cmd := exec.Command(bin, "-spec", spec, "-topic", "orders.created", "-dry-run", "-count", "5")
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
		cmd := exec.Command(bin, "-spec", spec, "-topic", "categories",
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
		cmd := exec.Command(bin, "-spec", spec, "-topic", "categories",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	cmd := exec.Command(bin, "-spec", spec, "-topic", "orders.created", "-dry-run", "-count", "100")
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
		cmd := exec.Command(bin, "-spec", spec, "-topic", "mixed",
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
		cmd := exec.Command(bin, "-spec", spec, "-topic", "mixed",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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
		out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

// TestScenarioAvroFromSpec is #90 end to end: a 3.0 spec declaring its
// Payload and Key in Avro runs AVRO with no AVRO flags. The Dry run shows
// Avro JSON records and Keys, the Key planted at -keyPath, and matches, byte
// for byte, the run given the same avsc files through -avro-schema and
// -avro-key-schema.
func TestScenarioAvroFromSpec(t *testing.T) {
	bin := buildBinary(t)
	const value = `{"type":"record","name":"OrderCreated","namespace":"com.acme","fields":[{"name":"orderId","type":{"type":"string","logicalType":"uuid"}},{"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},{"name":"total","type":"double"}]}`
	const key = `{"type":"string","logicalType":"uuid"}`
	spec := writeTempSpec(t, `asyncapi: 3.0.0
info: {title: Orders, version: '1'}
channels:
  orders:
    address: orders.created
    messages:
      created:
        name: OrderCreated
        bindings: {kafka: {key: {$ref: '#/components/schemas/OrderKey'}}}
        payload:
          schemaFormat: 'application/vnd.apache.avro+json;version=1.9.0'
          schema: {$ref: '#/components/schemas/OrderCreated'}
components:
  schemas:
    OrderKey: `+key+`
    OrderCreated: `+value+`
`)
	run := func(args ...string) (string, string) {
		t.Helper()
		cmd := exec.Command(bin, append([]string{"-topic", "orders.created", "-dry-run", "-count", "5", "-seed", "9", "-now", "2026-01-02T03:04:05Z", "-keyPath", "orderId"}, args...)...)
		var stdout, stderr strings.Builder
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("%v: %v\nstderr: %s", args, err, stderr.String())
		}
		return stdout.String(), stderr.String()
	}

	stdout, stderr := run("-spec", spec)
	keys := avroKeys(t, stderr)
	lines := filterJSONLines(stdout)
	if len(lines) != 5 || len(keys) != 5 {
		t.Fatalf("want 5 records and 5 Keys, got:\nstdout: %s\nstderr: %s", stdout, stderr)
	}
	for i, line := range lines {
		var p map[string]any
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if p["status"] != "NEW" && p["status"] != "PAID" {
			t.Errorf("record %d: status %v, want a symbol of the spec's enum", i, p["status"])
		}
		if p["orderId"] != keys[i] {
			t.Errorf("record %d: orderId %v, Key %v; want the Key planted", i, p["orderId"], keys[i])
		}
	}

	files := [][]string{{"-avro-schema", writeTempAvsc(t, "value.avsc", value)}, {"-avro-key-schema", writeTempAvsc(t, "key.avsc", key)}}
	fromFiles, fromFilesErr := run(append(append([]string{"-spec", filepath.Join("..", "..", "examples", "order.asyncapi.yaml")}, files[0]...), files[1]...)...)
	withoutStats := regexp.MustCompile(`(?m)^Stats .*$`)
	if fromFiles != stdout || withoutStats.ReplaceAllString(fromFilesErr, "") != withoutStats.ReplaceAllString(stderr, "") {
		t.Errorf("the spec's avsc and the same avsc as files differ:\nspec:\n%s%s\nfiles:\n%s%s", stdout, stderr, fromFiles, fromFilesErr)
	}
}

// TestScenarioAvroMix is #91 end to end: a 3.0 spec declaring two Avro
// Message types runs AVRO with no AVRO flags, and its Dry run shows records
// of both, each in the Avro JSON encoding of its own record, without the
// union's wrapper.
func TestScenarioAvroMix(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `asyncapi: 3.0.0
info: {title: Orders, version: '1'}
channels:
  orders:
    address: orders
    messages:
      created:
        name: OrderCreated
        payload:
          schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
          schema: {type: record, name: OrderCreated, namespace: com.acme, fields: [{name: kind, type: {type: enum, name: Created, symbols: [CREATED]}}, {name: billing, type: {$ref: '#/components/schemas/Address'}}]}
      paid:
        name: OrderPaid
        payload:
          schemaFormat: 'application/vnd.apache.avro;version=1.9.0'
          schema: {type: record, name: OrderPaid, namespace: com.acme, fields: [{name: kind, type: {type: enum, name: Paid, symbols: [PAID]}}, {name: billing, type: {$ref: '#/components/schemas/Address'}}, {name: amount, type: double}]}
components:
  schemas:
    Address: {type: record, name: Address, fields: [{name: city, type: string}]}
`)
	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders", "-dry-run", "-count", "20", "-seed", "3").Output()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}
	kinds := map[string]int{}
	for i, line := range filterJSONLines(string(out)) {
		var p map[string]any
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		kind := fmt.Sprint(p["kind"])
		kinds[kind]++
		if _, wrapped := p["com.acme.OrderPaid"]; wrapped {
			t.Fatalf("record %d is wrapped in the union: %s", i, line)
		}
		if _, ok := p["billing"].(map[string]any); !ok {
			t.Errorf("record %d: billing %v, want the shared Address record", i, p["billing"])
		}
		if _, paid := p["amount"]; paid != (kind == "PAID") {
			t.Errorf("record %d: kind %s with fields %v, want each record in its own Message type's shape", i, kind, p)
		}
	}
	if kinds["CREATED"] == 0 || kinds["PAID"] == 0 {
		t.Errorf("Message types in the Dry run = %v, want both", kinds)
	}
}

// headersSpec declares Headers on one of its two Message types (#92).
const headersSpec = `asyncapi: 3.0.0
info: {title: Orders, version: '1'}
channels:
  orders:
    address: orders
    messages:
      created:
        name: OrderCreated
        headers:
          type: object
          required: [tenant, attempt]
          properties:
            tenant: {type: string, enum: [acme]}
            attempt: {type: integer, minimum: 1, maximum: 3}
        payload: {type: object, required: [kind], properties: {kind: {const: created}}}
      paid:
        name: OrderPaid
        payload: {type: object, required: [kind], properties: {kind: {const: paid}}}
`

// TestScenarioHeadersDryRun is #92 end to end in a Dry run: each record of a
// Message type declaring headers is preceded on stderr by its Headers, in
// name order, and stdout keeps the Payload lines alone.
func TestScenarioHeadersDryRun(t *testing.T) {
	bin := buildBinary(t)
	cmd := exec.Command(bin, "-spec", writeTempSpec(t, headersSpec), "-topic", "orders", "-dry-run", "-count", "20", "-seed", "4")
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("run: %v\n%s", err, stderr.String())
	}
	headerLine := regexp.MustCompile(`^Headers: \{"attempt":"[123]","tenant":"acme"\}$`)
	var echoes int
	for _, line := range strings.Split(stderr.String(), "\n") {
		if strings.HasPrefix(line, "Headers: ") {
			echoes++
			if !headerLine.MatchString(line) {
				t.Errorf("header echo %q, want attempt and tenant in name order", line)
			}
		}
	}
	created := strings.Count(stdout.String(), `"created"`)
	if created == 0 || echoes != created {
		t.Errorf("%d header echoes for %d OrderCreated records, want one each\nstderr: %s", echoes, created, stderr.String())
	}
	if lines := filterJSONLines(stdout.String()); len(lines) != 20 || strings.Contains(stdout.String(), "Headers") {
		t.Errorf("stdout must hold the 20 Payload lines alone:\n%s", stdout.String())
	}
}

// TestScenarioHeadersProduced is #92 end to end when producing: the binary
// produces to a Kafka broker - an in-process one speaking the Kafka protocol -
// and each OrderCreated record read back carries its Headers.
func TestScenarioHeadersProduced(t *testing.T) {
	bin := buildBinary(t)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	broker := cluster.ListenAddrs()[0]
	out, err := exec.Command(bin, "-spec", writeTempSpec(t, headersSpec), "-topic", "orders", "-broker", broker, "-count", "10", "-seed", "4", "-rate", "0s").CombinedOutput()
	if err != nil {
		t.Fatalf("run: %v\n%s", err, out)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < 10 && ctx.Err() == nil {
		records = append(records, consumer.PollFetches(ctx).Records()...)
	}
	if len(records) != 10 {
		t.Fatalf("read %d records, want 10", len(records))
	}
	createdSeen := 0
	for i, r := range records {
		created := strings.Contains(string(r.Value), `"created"`)
		if created {
			createdSeen++
		}
		if !created {
			if len(r.Headers) != 0 {
				t.Errorf("record %d (OrderPaid): headers %v, want none", i, r.Headers)
			}
			continue
		}
		if len(r.Headers) != 2 || r.Headers[0].Key != "attempt" || r.Headers[1].Key != "tenant" || string(r.Headers[1].Value) != "acme" {
			t.Errorf("record %d (OrderCreated): headers %v, want attempt and tenant=acme", i, r.Headers)
		}
	}
	if createdSeen == 0 {
		t.Error("no OrderCreated record was produced, so no headers were checked")
	}
}

// TestScenarioHeaderTopicParameters is #93 end to end: -topic fills a
// Topic parameter whose location is in the Headers, in a 3.0 and a 2.x spec,
// in JSON and AVRO mode, and every record's Headers carry the value, in a Dry
// run and when produced; a value the header's schema refuses stops the run.
func TestScenarioHeaderTopicParameters(t *testing.T) {
	bin := buildBinary(t)
	const headers = `{type: object, required: [tenant, attempt], properties: {tenant: {type: string, pattern: '^[a-z]+$'}, attempt: {type: integer, minimum: 1, maximum: 3}}}`
	specs := map[string]string{
		"3.0 json": `asyncapi: 3.0.0
info: {title: Tenants, version: '1'}
channels:
  tenants:
    address: 'orders.{tenant}'
    parameters: {tenant: {location: '$message.header#/tenant'}}
    messages:
      created: {name: OrderCreated, headers: ` + headers + `, payload: {type: object, required: [kind], properties: {kind: {const: created}}}}
      paid: {name: OrderPaid, headers: ` + headers + `, payload: {type: object, required: [kind], properties: {kind: {const: paid}}}}
`,
		"2.x json": `asyncapi: 2.6.0
info: {title: Tenants, version: '1'}
channels:
  orders.{tenant}:
    parameters: {tenant: {schema: {type: string}, location: '$message.header#/tenant'}}
    publish: {message: {name: OrderCreated, headers: ` + headers + `, payload: {type: object, required: [kind], properties: {kind: {const: created}}}}}
`,
		"3.0 avro": `asyncapi: 3.0.0
info: {title: Tenants, version: '1'}
channels:
  tenants:
    address: 'orders.{tenant}'
    parameters: {tenant: {location: '$message.header#/tenant'}}
    messages:
      created:
        name: OrderCreated
        headers: ` + headers + `
        payload: {schemaFormat: 'application/vnd.apache.avro;version=1.9.0', schema: {type: record, name: OrderCreated, fields: [{name: kind, type: string}]}}
`,
	}
	echo := regexp.MustCompile(`^Headers: \{"attempt":"[123]","tenant":"acme"\}$`)
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			path := writeTempSpec(t, spec)
			cmd := exec.Command(bin, "-spec", path, "-topic", "orders.acme", "-dry-run", "-count", "10", "-seed", "6")
			var stdout, stderr strings.Builder
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("run: %v\n%s", err, stderr.String())
			}
			var echoes int
			for _, line := range strings.Split(stderr.String(), "\n") {
				if strings.HasPrefix(line, "Headers: ") {
					echoes++
					if !echo.MatchString(line) {
						t.Errorf("header echo %q, want tenant=acme planted", line)
					}
				}
			}
			if echoes != 10 {
				t.Errorf("%d header echoes, want one per record\nstderr: %s", echoes, stderr.String())
			}

			out, err := exec.Command(bin, "-spec", path, "-topic", "orders.ACME", "-dry-run", "-count", "1").CombinedOutput()
			if err == nil || !strings.Contains(string(out), "Topic parameter tenant: value ACME does not conform to the header at $message.header#/tenant") {
				t.Errorf("orders.ACME: err %v, output:\n%s\nwant the value refused by the header's pattern", err, out)
			}
		})
	}

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders.acme"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	broker := cluster.ListenAddrs()[0]
	if out, err := exec.Command(bin, "-spec", writeTempSpec(t, specs["3.0 json"]), "-topic", "orders.acme", "-broker", broker, "-count", "5", "-rate", "0s").CombinedOutput(); err != nil {
		t.Fatalf("produce: %v\n%s", err, out)
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("orders.acme"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < 5 && ctx.Err() == nil {
		records = append(records, consumer.PollFetches(ctx).Records()...)
	}
	if len(records) != 5 {
		t.Fatalf("read %d records, want 5", len(records))
	}
	for i, r := range records {
		if len(r.Headers) != 2 || r.Headers[1].Key != "tenant" || string(r.Headers[1].Value) != "acme" {
			t.Errorf("record %d: headers %v, want tenant=acme planted", i, r.Headers)
		}
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
	out, err := exec.Command(bin, "-spec", spec, "-topic", "orders",
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
	combined, err := exec.Command(bin, "-spec", spec, "-topic", "orders",
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
	combined, err := exec.Command(bin, "-spec", spec, "-topic", "orders",
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
		args := append([]string{"-spec", specPath, "-topic", "orders", "-dry-run", "-count", "1",
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

	cmd := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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
		args := append([]string{"-spec", specPath, "-topic", "orders", "-dry-run", "-count", "1",
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
			cmd := exec.Command(bin, "-spec", spec, "-topic", "orders",
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
			cmd := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

	cmd := exec.Command(bin, "-spec", spec, "-topic", "orders.created",
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

// TestScenarioSpecMistakesAreReported is #73 end to end: every spec mistake
// the reader used to swallow - a broken message $ref hidden by a fallback, a
// malformed Key binding read as no Key - now stops the run naming what is
// wrong, and a bindings $ref is followed.
func TestScenarioSpecMistakesAreReported(t *testing.T) {
	bin := buildBinary(t)
	const head = "asyncapi: '2.6.0'\ninfo: {title: T, version: '1'}\n"
	cases := []struct {
		name, spec, want string
	}{
		{"broken publish ref with a subscribe message", head + `channels:
  orders:
    publish: {message: {$ref: '#/components/messages/Typo'}}
    subscribe: {message: {payload: {type: object}}}
components: {messages: {Order: {payload: {type: object}}}}
`, `resolving $ref #/components/messages/Typo: "Typo" not found`},
		{"broken publish ref alone", head + `channels:
  orders:
    publish: {message: {$ref: '#/components/messages/Typo'}}
components: {messages: {Order: {payload: {type: object}}}}
`, `resolving $ref #/components/messages/Typo`},
		{"entry-level messages", head + `channels:
  orders:
    messages: {created: {payload: {type: object}}}
`, "AsyncAPI 3.0 syntax"},
		{"key binding not a schema", head + `channels:
  orders:
    publish: {message: {bindings: {kafka: {key: string}}, payload: {type: object}}}
`, "bindings.kafka.key must be a schema object"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := exec.Command(bin, "-spec", writeTempSpec(t, c.spec), "-topic", "orders",
				"-dry-run", "-count", "1", "-seed", "1").CombinedOutput()
			if err == nil {
				t.Fatalf("expected the run to stop, got:\n%s", out)
			}
			if !strings.Contains(string(out), c.want) {
				t.Errorf("output lacks %q:\n%s", c.want, out)
			}
		})
	}

	t.Run("bindings ref", func(t *testing.T) {
		spec := head + `channels:
  orders:
    publish:
      message:
        bindings: {$ref: '#/components/messageBindings/keyed'}
        payload: {type: object, required: [id], properties: {id: {type: string}}}
components:
  messageBindings:
    keyed: {kafka: {key: {type: string, format: uuid}}}
`
		out, err := exec.Command(bin, "-spec", writeTempSpec(t, spec), "-topic", "orders",
			"-dry-run", "-count", "1", "-seed", "1", "-keyPath", "id").CombinedOutput()
		if err != nil {
			t.Fatalf("command failed: %v\n%s", err, out)
		}
		if !regexp.MustCompile(`Key: [0-9a-f]{8}-`).Match(out) {
			t.Errorf("expected a uuid Key from the referenced binding, got:\n%s", out)
		}
	})
}

// TestScenarioMessageTypeMix is #74's acceptance: a Kafka topic with several
// Message types produces all of them across its records, the same -seed
// repeats the exact sequence, and the shared Key is planted in every type.
func TestScenarioMessageTypeMix(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `asyncapi: '2.6.0'
info: {title: Mix, version: '1'}
channels:
  orders:
    publish:
      message:
        oneOf:
          - $ref: '#/components/messages/OrderCreated'
          - $ref: '#/components/messages/OrderUpdated'
components:
  messageBindings:
    keyed: {kafka: {key: {type: string, format: uuid}}}
  messages:
    OrderCreated:
      bindings: {$ref: '#/components/messageBindings/keyed'}
      payload: {type: object, required: [kind, orderId], properties: {kind: {const: created}, orderId: {type: string}}}
    OrderUpdated:
      bindings: {$ref: '#/components/messageBindings/keyed'}
      payload: {type: object, required: [kind, orderId], properties: {kind: {const: updated}, orderId: {type: string}}}
`)
	run := func() (string, string) {
		cmd := exec.Command(bin, "-spec", spec, "-topic", "orders", "-dry-run",
			"-count", "30", "-seed", "5", "-now", "2026-01-02T03:04:05Z", "-keyPath", "orderId")
		var stdout, stderr strings.Builder
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("command failed: %v\nstderr: %s", err, stderr.String())
		}
		return stdout.String(), stderr.String()
	}
	out, errOut := run()
	lines := filterJSONLines(out)
	keys := regexp.MustCompile(`(?m)^Key: (.+)$`).FindAllStringSubmatch(errOut, -1)
	if len(lines) != 30 || len(keys) != 30 {
		t.Fatalf("expected 30 payloads and 30 keys, got %d and %d", len(lines), len(keys))
	}
	seen := map[string]int{}
	for i, line := range lines {
		var payload map[string]any
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		seen[fmt.Sprint(payload["kind"])]++
		if payload["orderId"] != keys[i][1] {
			t.Errorf("record %d (%v): orderId %v, Key %s; want the Key planted", i, payload["kind"], payload["orderId"], keys[i][1])
		}
	}
	if seen["created"] == 0 || seen["updated"] == 0 {
		t.Errorf("Message types across 30 records = %v, want both", seen)
	}
	if again, _ := run(); again != out {
		t.Error("the same -seed produced a different sequence")
	}
}

// TestScenarioKeyReuse is #75's acceptance: with -records-per-key above 1 the
// same Key recurs across records, in both Wire formats, and -keyPath plants
// whichever Key a record got, so Key and Payload still agree.
func TestScenarioKeyReuse(t *testing.T) {
	bin := buildBinary(t)
	jsonSpec := writeTempSpec(t, `asyncapi: '2.6.0'
info: {title: Reuse, version: '1'}
channels:
  orders:
    publish:
      message:
        bindings: {kafka: {key: {type: string, format: uuid}}}
        payload: {type: object, required: [orderId], properties: {orderId: {type: string}}}
`)
	valueAvsc := writeTempAvsc(t, "value.avsc", `{"type":"record","name":"Order","fields":[{"name":"orderId","type":"string"}]}`)
	keyAvsc := writeTempAvsc(t, "key.avsc", `{"type":"string"}`)
	cases := map[string][]string{
		"json": {"-spec", jsonSpec, "-topic", "orders"},
		"avro": {"-spec", jsonSpec, "-topic", "orders", "-format", "avro", "-avro-schema", valueAvsc, "-avro-key-schema", keyAvsc},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			args = append(args, "-dry-run", "-count", "40", "-seed", "11", "-records-per-key", "4", "-keyPath", "orderId")
			cmd := exec.Command(bin, args...)
			var stdout, stderr strings.Builder
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("command failed: %v\nstderr: %s", err, stderr.String())
			}
			keys := regexp.MustCompile(`(?m)^Key: (.+)$`).FindAllStringSubmatch(stderr.String(), -1)
			lines := filterJSONLines(stdout.String())
			if len(keys) != 40 || len(lines) != 40 {
				t.Fatalf("expected 40 keys and payloads, got %d and %d", len(keys), len(lines))
			}
			distinct := map[string]bool{}
			for i, line := range lines {
				var payload map[string]any
				if err := json.Unmarshal([]byte(line), &payload); err != nil {
					t.Fatalf("record %d: %v", i, err)
				}
				key := strings.Trim(keys[i][1], `"`) // AVRO shows a string Key quoted
				if payload["orderId"] != key {
					t.Errorf("record %d: orderId %v, Key %s; want the Key planted", i, payload["orderId"], keys[i][1])
				}
				distinct[key] = true
			}
			if len(distinct) >= 30 {
				t.Errorf("40 records over %d distinct Keys; want Keys recurring (about 10)", len(distinct))
			}
		})
	}
}
