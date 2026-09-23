package pipeline

import (
	"os/exec"
	"strings"
	"testing"
)

// TestPipelineIsFormatBlind locks the claim CONTEXT.md makes about the
// Pipeline at package level: the wire-format adapters live in internal/wire, so
// neither the avsc model nor the Confluent client is among its dependencies.
func TestPipelineIsFormatBlind(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == "github.com/holgeradam/kafka-testdata-generator/internal/avro" ||
			strings.HasPrefix(dep, "github.com/confluentinc/") {
			t.Errorf("internal/pipeline depends on %s; wire-format code belongs in internal/wire", dep)
		}
	}
}
