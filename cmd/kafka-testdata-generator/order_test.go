package main

import (
	"context"
	"strings"
	"testing"
)

// dryRun runs the CLI in Dry run and returns the records it printed, one per
// line.
func dryRun(t *testing.T, spec, topic string, args ...string) []string {
	t.Helper()
	var stdout, stderr strings.Builder
	all := append([]string{"-spec", writeSpec(t, spec), "-topic", topic, "-dry-run", "-seed", "3", "-now", "2026-09-28T12:00:00Z"}, args...)
	if code := run(context.Background(), "ktg", all, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d\nstderr: %s", code, stderr.String())
	}
	return strings.Split(strings.TrimSpace(stdout.String()), "\n")
}

// TestRunEncodesTheChosenBranchInItsOrder is #112's regression: a record
// generated from a later oneOf branch encodes in that branch's declared
// order, its array items by that branch's items, not an earlier branch's.
func TestRunEncodesTheChosenBranchInItsOrder(t *testing.T) {
	objects := dryRun(t, `
asyncapi: 3.0.0
info: {title: T, version: '1'}
channels:
  orders:
    address: orders
    messages:
      m:
        payload:
          oneOf:
            - {type: object, required: [a, b], additionalProperties: false, properties: {a: {type: string, const: first}, b: {type: string}}}
            - {type: object, required: [b, a], additionalProperties: false, properties: {b: {type: string}, a: {type: string, const: second}}}
`, "orders", "-count", "20")
	arrays := dryRun(t, `
asyncapi: 3.0.0
info: {title: T, version: '1'}
channels:
  orders:
    address: orders
    messages:
      m:
        payload:
          oneOf:
            - {type: array, minItems: 1, maxItems: 1, items: {type: object, required: [x, y], additionalProperties: false, properties: {x: {type: integer, const: 1}, y: {type: integer}}}}
            - {type: array, minItems: 1, maxItems: 1, items: {type: object, required: [y, x], additionalProperties: false, properties: {y: {type: integer}, x: {type: integer, const: 2}}}}
`, "orders", "-count", "20")

	seen := map[string]bool{}
	for _, r := range objects {
		switch {
		case strings.Contains(r, `"first"`):
			seen["first"] = true
			if !strings.HasPrefix(r, `{"a":"first","b":`) {
				t.Errorf("first branch record %s, want a before b", r)
			}
		case strings.Contains(r, `"second"`):
			seen["second"] = true
			if !strings.HasPrefix(r, `{"b":`) || !strings.HasSuffix(r, `,"a":"second"}`) {
				t.Errorf("second branch record %s, want b before a, as that branch declares", r)
			}
		}
	}
	for _, r := range arrays {
		switch {
		case strings.HasPrefix(r, `[{"x":1,`):
			seen["x"] = true
		case strings.HasPrefix(r, `[{"y":`) && strings.HasSuffix(r, `,"x":2}]`):
			seen["y"] = true
		default:
			t.Errorf("array record %s, want its items in their own branch's order", r)
		}
	}
	if len(seen) != 4 {
		t.Errorf("branches seen %v, want every branch of both specs", seen)
	}
}

// TestRunEncodesLiteralsAsWritten proves a literal the generator returns
// as-is keeps the order it is written in, whatever order its schema declares
// (#112), nested objects and array items included, while a generated object
// keeps its schema's declared order, allOf branches in turn.
func TestRunEncodesLiteralsAsWritten(t *testing.T) {
	records := dryRun(t, `
asyncapi: 2.6.0
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required: [zeta, customer, tags, merged]
          properties:
            zeta: {type: string, const: z}
            customer:
              type: object
              properties: {id: {type: integer}, name: {type: string}}
              example: {name: Acme, id: 7, address: {street: Main, city: Oslo}}
            tags:
              type: array
              items: {type: object}
              examples: [[{b: 1, a: 2}, {d: 3, c: 4}]]
            merged:
              allOf:
                - {type: object, required: [m2, m1], properties: {m2: {const: 2}, m1: {const: 1}}}
                - {required: [k], properties: {k: {const: 0}}}
`, "orders", "-count", "3")
	want := `{"zeta":"z","customer":{"name":"Acme","id":7,"address":{"street":"Main","city":"Oslo"}},"tags":[{"b":1,"a":2},{"d":3,"c":4}],"merged":{"m2":2,"m1":1,"k":0}}`
	for i, r := range records {
		if r != want {
			t.Errorf("record %d = %s\nwant       %s", i, r, want)
		}
	}
}

// TestRunGeneratesPropertiesNamedLikeKeywords is a regression found with
// #112: a property named properties or example is a property like any other,
// generated in its declared place, where the recorded order used to be
// stamped into the properties object and fail the run as a property of its
// own.
func TestRunGeneratesPropertiesNamedLikeKeywords(t *testing.T) {
	records := dryRun(t, `
asyncapi: 2.6.0
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        payload:
          type: object
          required: [properties, example]
          properties:
            properties: {type: object, required: [b, a], properties: {b: {const: 1}, a: {const: 2}}}
            example: {type: object, properties: {d: {type: string}, c: {type: string}}, example: {d: x, c: y}}
`, "orders", "-count", "2")
	want := `{"properties":{"b":1,"a":2},"example":{"d":"x","c":"y"}}`
	for i, r := range records {
		if r != want {
			t.Errorf("record %d = %s, want %s", i, r, want)
		}
	}
}
