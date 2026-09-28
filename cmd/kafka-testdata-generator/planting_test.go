package main

import (
	"context"
	"strings"
	"testing"
)

// literalKeySpec declares a Key and a Payload whose customer object carries
// the literal the generator returns as-is.
func literalKeySpec(literal string) string {
	return `
asyncapi: 2.6.0
info: {title: T, version: '1'}
channels:
  orders:
    publish:
      message:
        bindings: {kafka: {key: {type: string, format: uuid}}}
        payload:
          type: object
          required: [customer, note]
          properties:
            customer:
              type: object
              required: [id]
              properties: {id: {type: string}}
              ` + literal + `
            note: {type: string, example: hello}
`
}

// literalParamSpec declares a Topic parameter whose location steps through
// meta, which carries the literal, in the Payload or the Headers.
func literalParamSpec(part, literal string) string {
	return `
asyncapi: 3.0.0
info: {title: T, version: '1'}
channels:
  tenants:
    address: 'orders.{region}'
    parameters: {region: {location: '$message.` + part + `#/meta/region'}}
    messages:
      created:
        headers:
          type: object
          required: [meta]
          properties:
            meta: {type: object, required: [region], properties: {region: {type: string}}, ` + literal + `}
        payload:
          type: object
          required: [meta]
          properties:
            meta: {type: object, required: [region], properties: {region: {type: string}}, ` + literal + `}
`
}

// TestRunRefusesPlantingThroughLiterals is the regression for #107: a
// Planting whose path steps into a schema the generator answers with a
// literal (example, examples, const, enum) is refused before the run starts,
// naming the keyword and the flag that owns the Planting, instead of failing
// on the first record.
func TestRunRefusesPlantingThroughLiterals(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{"-keyPath through an example", []string{"-spec", writeSpec(t, literalKeySpec(`example: {name: Acme}`)), "-topic", "orders", "-keyPath", "customer.id"},
			[]string{`-keyPath "customer.id"`, "an example"}},
		{"-keyPath through examples", []string{"-spec", writeSpec(t, literalKeySpec(`examples: [{name: Acme}]`)), "-topic", "orders", "-keyPath", "customer.id"},
			[]string{`-keyPath "customer.id"`, "examples"}},
		{"-keyPath through a const", []string{"-spec", writeSpec(t, literalKeySpec(`const: {id: c1}`)), "-topic", "orders", "-keyPath", "customer.id"},
			[]string{`-keyPath "customer.id"`, "a const"}},
		{"-keyPath through an enum", []string{"-spec", writeSpec(t, literalKeySpec(`enum: [{id: c1}]`)), "-topic", "orders", "-keyPath", "customer.id"},
			[]string{`-keyPath "customer.id"`, "an enum"}},
		{"Payload location through an example", []string{"-spec", writeSpec(t, literalParamSpec("payload", "example: {source: web}")), "-topic", "orders.eu"},
			[]string{"Topic parameter region", "$message.payload#/meta/region", "an example"}},
		{"header location through a const", []string{"-spec", writeSpec(t, literalParamSpec("header", "const: {region: eu}")), "-topic", "orders.eu"},
			[]string{"Topic parameter region", "$message.header#/meta/region", "a const"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			args := append(c.args, "-dry-run", "-count", "3", "-seed", "1")
			if code := run(context.Background(), "ktg", args, &stdout, &stderr); code != 1 {
				t.Fatalf("exit code = %d, want 1\nstderr: %s", code, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want no record: the refusal comes before the run", stdout.String())
			}
			for _, w := range c.want {
				if !strings.Contains(stderr.String(), w) {
					t.Errorf("stderr = %q, want it to mention %q", stderr.String(), w)
				}
			}
			if strings.Contains(c.name, "location") && strings.Contains(stderr.String(), "-keyPath") {
				t.Errorf("stderr = %q names -keyPath, but no -keyPath was given", stderr.String())
			}
		})
	}
}

// TestRunPlantsOverLiteralsAtThePathEnd proves a literal at the end of a path
// is no obstacle: the planted value replaces what the generator produced, a
// Topic parameter conforming to the enum there and the Key replacing an
// example, which constrains nothing (#107).
func TestRunPlantsOverLiteralsAtThePathEnd(t *testing.T) {
	enumParam := writeSpec(t, `
asyncapi: 3.0.0
info: {title: T, version: '1'}
channels:
  tenants:
    address: 'orders.{region}'
    parameters: {region: {location: '$message.payload#/region'}}
    messages:
      created:
        payload:
          type: object
          required: [region]
          properties:
            region: {type: string, enum: [eu, us]}
`)
	var stdout, stderr strings.Builder
	if code := run(context.Background(), "ktg", []string{"-spec", enumParam, "-topic", "orders.eu", "-dry-run", "-count", "5", "-seed", "1"}, &stdout, &stderr); code != 0 {
		t.Fatalf("enum location: exit code = %d\nstderr: %s", code, stderr.String())
	}
	if n := strings.Count(stdout.String(), `{"region":"eu"}`); n != 5 {
		t.Errorf("enum location: stdout = %q, want five records with region eu", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), "ktg", []string{"-spec", writeSpec(t, literalKeySpec(`example: {name: Acme}`)), "-topic", "orders", "-keyPath", "note", "-dry-run", "-count", "3", "-seed", "1"}, &stdout, &stderr); code != 0 {
		t.Fatalf("example at the Key path: exit code = %d\nstderr: %s", code, stderr.String())
	}
	if strings.Contains(stdout.String(), `"note":"hello"`) {
		t.Errorf("example at the Key path: stdout = %q, want the Key planted over the example", stdout.String())
	}
}
