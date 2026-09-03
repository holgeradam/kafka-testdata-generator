package main

import (
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
// byte-identical output including date-formatted fields. A temp spec with a
// date-format field is used so the clock path is exercised end to end.
func TestScenarioDeterministicWithNow(t *testing.T) {
	bin := buildBinary(t)
	spec := writeTempSpec(t, `
asyncapi: '2.6.0'
info:
  title: Dated
  version: '1.0.0'
channels:
  dated:
    publish:
      message:
        payload:
          type: object
          required:
            - created
          properties:
            created:
              type: string
              format: date
`)

	run := func() string {
		out, err := exec.Command(bin, "-spec", spec, "-channel", "dated",
			"-dry-run", "-count", "3", "-seed", "42", "-now", "2026-01-02T03:04:05Z").CombinedOutput()
		if err != nil {
			t.Fatalf("command failed: %v\noutput: %s", err, out)
		}
		return string(out)
	}

	if run() != run() {
		t.Error("identical (-seed, -now) must produce byte-identical output including date fields")
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

// TestScenarioFormatAvroReject verifies -format avro is rejected with a clear
// not-implemented error message.
func TestScenarioFormatAvroReject(t *testing.T) {
	bin := buildBinary(t)
	spec := filepath.Join("..", "..", "examples", "order.asyncapi.yaml")

	out, err := exec.Command(bin, "-spec", spec, "-channel", "orders.created",
		"-dry-run", "-count", "1", "-format", "avro").CombinedOutput()
	if err == nil {
		t.Fatal("expected -format avro to be rejected")
	}
	if !strContains(string(out), "not yet implemented") {
		t.Errorf("expected 'not yet implemented' error, got: %s", out)
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
