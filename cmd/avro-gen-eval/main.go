// PROTOTYPE - throwaway code for issue #15 evaluation.
// Tests two things:
//   -mode=pattern: baseline determinism of the current Generator
//   -mode=avro:    Avro random generator determinism and diversity
//
// Run: go run ./cmd/avro-gen-eval -mode=pattern -schema=examples/order.asyncapi.yaml
// Run: go run ./cmd/avro-gen-eval -mode=avro -schema=examples/order.avsc
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/iskorotkov/avro/v2"
)

const iterations = 50

func main() {
	mode := flag.String("mode", "pattern", "prototype mode: pattern (baseline) or avro (new generator)")
	schemaPath := flag.String("schema", "", "path to schema file (asyncapi yaml for pattern, avsc for avro)")
	seed := flag.Int64("seed", 42, "random seed for determinism check")
	flag.Parse()

	if *mode == "avro" && *schemaPath == "" {
		fmt.Fprintln(os.Stderr, "error: -schema flag is required for avro mode (path to .avsc file)")
		os.Exit(1)
	}

	switch *mode {
	case "pattern":
		runPatternPrototype(*schemaPath, *seed)
	case "avro":
		runAvroPrototype(*schemaPath, *seed)
	default:
		fmt.Fprintf(os.Stderr, "error: unknown mode %q (use pattern or avro)\n", *mode)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Mode 1: Pattern baseline - tests the current Generator's determinism
// ---------------------------------------------------------------------------

func runPatternPrototype(schemaPath string, seed int64) {
	fmt.Println("=== Prototype 1: Pattern Baseline ===")
	fmt.Printf("Schema: %s\n", schemaPath)
	fmt.Printf("Seed:   %d\n", seed)
	fmt.Printf("Iterations: %d\n\n", iterations)

	// Parse the AsyncAPI spec to get a JSON Schema
	jsonSchema, err := loadOrderSchema(schemaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading schema: %v\n", err)
		os.Exit(1)
	}

	// Generate with current generator, run 1: capture output
	now := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	g1 := generator.New(seed, now)
	run1 := generateBatch(g1, jsonSchema)

	// Generate with current generator, run 2: same seed, should be identical
	g2 := generator.New(seed, now)
	run2 := generateBatch(g2, jsonSchema)

	// Compare determinism
	deterministic := true
	for i := range run1 {
		if !bytes.Equal(run1[i], run2[i]) {
			fmt.Printf("FAIL: record %d differs between runs\n", i)
			deterministic = false
		}
	}

	if deterministic {
		fmt.Printf("PASS: all %d records byte-identical across two runs with seed=%d\n", iterations, seed)
	} else {
		fmt.Println("FAIL: deterministic output check failed")
	}

	// Diversity check: how many unique records?
	unique := make(map[string]int)
	for _, raw := range run1 {
		unique[string(raw)]++
	}
	fmt.Printf("\nDiversity: %d unique records out of %d total\n", len(unique), iterations)
	if len(unique) == 1 {
		fmt.Println("WARNING: all records are identical - no diversity")
	}

	// Field diversity breakdown
	fmt.Println("\nField diversity (sample record 0):")
	var sample map[string]any
	if err := json.Unmarshal(run1[0], &sample); err == nil {
		printFieldDiversity(sample, "")
	}

	// Print first 3 records as examples
	fmt.Println("\nFirst 3 records:")
	for i := 0; i < 3 && i < len(run1); i++ {
		var pretty bytes.Buffer
		json.Indent(&pretty, run1[i], "", "  ")
		fmt.Printf("  [%d] %s\n", i, pretty.String())
	}
}

// ---------------------------------------------------------------------------
// Mode 2: Avro random generator - tests our own generator
// ---------------------------------------------------------------------------

func runAvroPrototype(schemaPath string, seed int64) {
	fmt.Println("=== Prototype 2: Avro Random Generator ===")
	fmt.Printf("Schema: %s\n", schemaPath)
	fmt.Printf("Seed:   %d\n", seed)
	fmt.Printf("Iterations: %d\n\n", iterations)

	// Parse the Avro schema
	schemaJSON, err := os.ReadFile(schemaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading schema: %v\n", err)
		os.Exit(1)
	}

	schema, err := avro.Parse(string(schemaJSON))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing avro schema: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Parsed schema type: %s\n", string(schema.Type()))
	fmt.Printf("Schema: %s\n\n", schema.String())

	// Create the Avro random generator
	avg := newAvroGenerator(seed)

	// Generate run 1
	run1 := avg.generateBatch(schema, iterations)

	// Generate run 2 (same seed)
	avg2 := newAvroGenerator(seed)
	run2 := avg2.generateBatch(schema, iterations)

	// Compare determinism
	deterministic := true
	for i := range run1 {
		if !bytes.Equal(run1[i], run2[i]) {
			fmt.Printf("FAIL: record %d differs between runs\n", i)
			deterministic = false
		}
	}

	if deterministic {
		fmt.Printf("PASS: all %d records byte-identical across two runs with seed=%d\n", iterations, seed)
	} else {
		fmt.Println("FAIL: deterministic output check failed")
	}

	// Diversity check
	unique := make(map[string]int)
	for _, raw := range run1 {
		unique[string(raw)]++
	}
	fmt.Printf("\nDiversity: %d unique records out of %d total\n", len(unique), iterations)
	if len(unique) == 1 {
		fmt.Println("WARNING: all records are identical - no diversity")
	}

	// Print first 3 records
	fmt.Println("\nFirst 3 records:")
	for i := 0; i < 3 && i < len(run1); i++ {
		var pretty bytes.Buffer
		json.Indent(&pretty, run1[i], "", "  ")
		fmt.Printf("  [%d] %s\n", i, pretty.String())
	}
}

// ---------------------------------------------------------------------------
// Avro random generator (the thing we're prototyping)
// ---------------------------------------------------------------------------

type avroGenerator struct {
	rng *rand.Rand
}

func newAvroGenerator(seed int64) *avroGenerator {
	return &avroGenerator{rng: rand.New(rand.NewSource(seed))}
}

func (g *avroGenerator) generateBatch(schema avro.Schema, count int) [][]byte {
	results := make([][]byte, count)
	for i := 0; i < count; i++ {
		value := g.generateValue(schema)
		raw, err := json.Marshal(value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal error at record %d: %v\n", i, err)
			continue
		}
		results[i] = raw
	}
	return results
}

func (g *avroGenerator) generateValue(schema avro.Schema) any {
	switch s := schema.(type) {
	case *avro.RecordSchema:
		return g.generateRecord(s)
	case *avro.PrimitiveSchema:
		return g.generatePrimitive(s)
	case *avro.EnumSchema:
		return g.generateEnum(s)
	case *avro.ArraySchema:
		return g.generateArray(s)
	case *avro.MapSchema:
		return g.generateMap(s)
	case *avro.UnionSchema:
		return g.generateUnion(s)
	case *avro.FixedSchema:
		return g.generateFixed(s)
	default:
		return nil
	}
}

func (g *avroGenerator) generateRecord(schema *avro.RecordSchema) map[string]any {
	result := make(map[string]any)
	for _, field := range schema.Fields() {
		result[field.Name()] = g.generateValue(field.Type())
	}
	return result
}

func (g *avroGenerator) generatePrimitive(schema *avro.PrimitiveSchema) any {
	switch schema.Type() {
	case avro.String:
		return g.randomString(8)
	case avro.Int:
		return int32(g.rng.Intn(1000))
	case avro.Long:
		return int64(g.rng.Intn(100000))
	case avro.Float:
		return float32(g.rng.Float64() * 1000)
	case avro.Double:
		return g.rng.Float64() * 1000
	case avro.Boolean:
		return g.rng.Intn(2) == 1
	case avro.Bytes:
		b := make([]byte, 8)
		g.rng.Read(b)
		return b
	default:
		return nil
	}
}

func (g *avroGenerator) generateEnum(schema *avro.EnumSchema) string {
	symbols := schema.Symbols()
	return symbols[g.rng.Intn(len(symbols))]
}

func (g *avroGenerator) generateArray(schema *avro.ArraySchema) []any {
	count := 1 + g.rng.Intn(5)
	result := make([]any, count)
	for i := 0; i < count; i++ {
		result[i] = g.generateValue(schema.Items())
	}
	return result
}

func (g *avroGenerator) generateMap(schema *avro.MapSchema) map[string]any {
	count := 1 + g.rng.Intn(5)
	result := make(map[string]any)
	for i := 0; i < count; i++ {
		key := g.randomString(4)
		result[key] = g.generateValue(schema.Values())
	}
	return result
}

func (g *avroGenerator) generateUnion(schema *avro.UnionSchema) any {
	types := schema.Types()
	// Weight toward non-null (70% chance of non-null)
	idx := 0
	if len(types) > 1 && g.rng.Intn(10) < 7 {
		idx = 1 + g.rng.Intn(len(types)-1)
	}
	return g.generateValue(types[idx])
}

func (g *avroGenerator) generateFixed(schema *avro.FixedSchema) []byte {
	b := make([]byte, schema.Size())
	g.rng.Read(b)
	return b
}

func (g *avroGenerator) randomString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[g.rng.Intn(len(chars))]
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// loadOrderSchema loads the order schema from an AsyncAPI spec.
// For the prototype, we just use a hardcoded JSON Schema matching the order.
func loadOrderSchema(path string) (map[string]any, error) {
	// The prototype doesn't need to parse the full AsyncAPI spec.
	// We use a hardcoded order schema that matches the .avsc file.
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"orderId":    map[string]any{"type": "string", "pattern": "ORD-[0-9]{6}"},
			"customerId": map[string]any{"type": "string", "pattern": "CUST-[A-Z]{3}-[0-9]{4}"},
			"amount":     map[string]any{"type": "number", "minimum": 0.01, "maximum": 9999.99},
			"currency":   map[string]any{"type": "string", "enum": []any{"USD", "EUR", "GBP", "JPY"}},
			"status":     map[string]any{"type": "string", "enum": []any{"pending", "confirmed", "shipped", "delivered", "cancelled"}},
			"items": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"sku":      map[string]any{"type": "string", "pattern": "[A-Z]{3}-[0-9]{4}"},
						"quantity": map[string]any{"type": "integer", "minimum": 1, "maximum": 100},
						"price":    map[string]any{"type": "number", "minimum": 0.01, "maximum": 999.99},
					},
					"required": []any{"sku", "quantity", "price"},
				},
			},
			"shippingAddress": map[string]any{
				"oneOf": []any{
					map[string]any{"type": "null"},
					map[string]any{
						"type": "object",
						"properties": map[string]any{
							"street":  map[string]any{"type": "string"},
							"city":    map[string]any{"type": "string"},
							"country": map[string]any{"type": "string"},
						},
						"required": []any{"street", "city", "country"},
					},
				},
			},
		},
		"required": []any{"orderId", "customerId", "amount", "currency", "status", "items"},
	}, nil
}

func generateBatch(g *generator.Generator, schema map[string]any) [][]byte {
	results := make([][]byte, iterations)
	for i := 0; i < iterations; i++ {
		value, err := g.Value(schema, "root")
		if err != nil {
			fmt.Fprintf(os.Stderr, "generation error at record %d: %v\n", i, err)
			continue
		}
		raw, err := json.Marshal(value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal error at record %d: %v\n", i, err)
			continue
		}
		results[i] = raw
	}
	return results
}

func printFieldDiversity(sample map[string]any, prefix string) {
	keys := make([]string, 0, len(sample))
	for k := range sample {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		fullKey := prefix + "." + k
		if prefix == "" {
			fullKey = k
		}
		v := sample[k]
		switch val := v.(type) {
		case map[string]any:
			fmt.Printf("  %s: (object)\n", fullKey)
			printFieldDiversity(val, fullKey)
		case []any:
			fmt.Printf("  %s: (array, len=%d)\n", fullKey, len(val))
		default:
			fmt.Printf("  %s: %v (%T)\n", fullKey, val, val)
		}
	}
}
