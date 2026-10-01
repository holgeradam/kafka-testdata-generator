package generator_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// orderOf builds an object schema whose properties are all required and
// declared in the given order, as the spec reader records it (#96).
func orderOf(props ...any) map[string]any {
	properties := map[string]any{}
	var names []any
	for i := 0; i < len(props); i += 2 {
		properties[props[i].(string)] = props[i+1]
		names = append(names, props[i])
	}
	return map[string]any{"type": "object", "required": names, "properties": properties, ordered.Keyword: names}
}

// c is a schema whose value is the constant v.
func c(v any) map[string]any { return map[string]any{"const": v} }

// generated encodes n values generated from schema with one seeded
// generator, as the JSON Encoder does: without a schema.
func generated(t *testing.T, schema map[string]any, n int) []string {
	t.Helper()
	gen := generator.New(synth.New(5, fixedNow()))
	out := make([]string, n)
	for i := range out {
		v, err := gen.Value(schema)
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = string(b)
	}
	return out
}

// TestValueFollowsDeclaredOrder proves every object the generator emits has
// its keys in the order the schema it was generated from declares them (#96,
// #112): nested objects, array items, a $ref's target, the merge of an allOf
// in branch order; a schema without a recorded order keeps sorted order.
func TestValueFollowsDeclaredOrder(t *testing.T) {
	cases := map[string]struct {
		schema map[string]any
		want   string
	}{
		"flat":   {orderOf("zeta", c("z"), "alpha", c("a")), `{"zeta":"z","alpha":"a"}`},
		"nested": {orderOf("b", orderOf("y", c(3), "x", c(2)), "a", c(1)), `{"b":{"y":3,"x":2},"a":1}`},
		"array items": {orderOf("items", map[string]any{"type": "array", "minItems": float64(1), "maxItems": float64(1), "items": orderOf("n", c(2), "m", c(1))}),
			`{"items":[{"n":2,"m":1}]}`},
		"ref": {map[string]any{"$ref": "#/$defs/Node", "$defs": map[string]any{"Node": orderOf("next", orderOf("id", c(2)), "id", c(1))}},
			`{"next":{"id":2},"id":1}`},
		"allOf": {map[string]any{"allOf": []any{orderOf("z", c(3)), orderOf("b", c(2), "a", c(1))}}, `{"z":3,"b":2,"a":1}`},
		"no recorded order": {map[string]any{"type": "object", "required": []any{"b", "a"}, "properties": map[string]any{"b": c(1), "a": c(2)}},
			`{"a":2,"b":1}`},
		"scalar": {c("x"), `"x"`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := generated(t, tc.schema, 1)[0]; got != tc.want {
				t.Errorf("generated %s, want %s", got, tc.want)
			}
		})
	}
}

// TestValueFollowsTheChosenBranch proves a record generated from a oneOf or
// anyOf branch has its keys, and its array items', in that branch's declared
// order, whichever branch was drawn (#112).
func TestValueFollowsTheChosenBranch(t *testing.T) {
	for _, keyword := range []string{"oneOf", "anyOf"} {
		t.Run(keyword, func(t *testing.T) {
			schema := map[string]any{keyword: []any{
				orderOf("a", c("first"), "b", c(1)),
				orderOf("b", c(2), "a", c("second")),
				map[string]any{"type": "array", "minItems": float64(1), "maxItems": float64(1), "items": orderOf("y", c(3), "x", c("third"))},
			}}
			want := map[string]bool{`{"a":"first","b":1}`: true, `{"b":2,"a":"second"}`: true, `[{"y":3,"x":"third"}]`: true}
			seen := map[string]bool{}
			for _, got := range generated(t, schema, 60) {
				if !want[got] {
					t.Fatalf("generated %s, want one of the branches in its own order", got)
				}
				seen[got] = true
			}
			if len(seen) != 3 {
				t.Errorf("branches seen %v, want all three", seen)
			}
		})
	}
}

// literal builds a schema that answers with the literal under keyword, with
// the order tree the spec reader records beside it.
func literal(keyword string, value, tree any) map[string]any {
	s := map[string]any{"type": "object", keyword: value}
	if tree != nil {
		s[ordered.LiteralKeyword] = map[string]any{keyword: tree}
	}
	return s
}

func tree(keys ...string) ordered.Object {
	var t ordered.Object
	for _, k := range keys {
		t.Add(k, nil)
	}
	return t
}

// TestValueKeepsLiteralsAsWritten proves a literal the generator returns
// as-is keeps the order it is written in, from the order tree the spec
// reader records (#112): nested objects, array items and enum members each
// by their own tree; keys the tree does not know, and a literal without one,
// in sorted order.
func TestValueKeepsLiteralsAsWritten(t *testing.T) {
	nested := ordered.Object{}
	nested.Add("z", tree("q", "p"))
	nested.Add("a", nil)
	cases := map[string]struct {
		schema map[string]any
		want   string
	}{
		"example": {literal("example", map[string]any{"b": 1, "a": 2}, tree("b", "a")), `{"b":1,"a":2}`},
		"nested": {literal("const", map[string]any{"a": 1, "z": map[string]any{"p": 1, "q": 2}}, nested),
			`{"z":{"q":2,"p":1},"a":1}`},
		"examples": {literal("examples", []any{[]any{map[string]any{"d": 1, "c": 2}}}, []any{[]any{tree("d", "c")}}), `[{"d":1,"c":2}]`},
		"enum":     {literal("enum", []any{map[string]any{"y": 1, "x": 2}}, []any{tree("y", "x")}), `{"y":1,"x":2}`},
		"unknown keys follow, sorted": {literal("example", map[string]any{"b": 1, "a": 2, "d": 3, "c": 4}, tree("b", "a")),
			`{"b":1,"a":2,"c":4,"d":3}`},
		"no tree": {literal("example", map[string]any{"b": 1, "a": 2}, nil), `{"a":2,"b":1}`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := generated(t, tc.schema, 1)[0]; got != tc.want {
				t.Errorf("generated %s, want %s", got, tc.want)
			}
		})
	}
}

// TestValueEncodesAsJSONDoes proves ordering changes nothing but the order:
// strings escape exactly as encoding/json escapes them.
func TestValueEncodesAsJSONDoes(t *testing.T) {
	got := generated(t, orderOf("a", c("<tag> & \"q\""), "b", c(1.5)), 1)[0]
	want, _ := json.Marshal(map[string]any{"a": "<tag> & \"q\"", "b": 1.5})
	if got != string(want) {
		t.Errorf("generated %s, want %s", got, want)
	}
	if strings.Contains(got, ordered.Keyword) {
		t.Errorf("generated %s carries the recorded order", got)
	}
}

// plainValue generates from schema, with every object a map, for tests about
// what is generated rather than its order.
func plainValue(g *generator.Generator, schema map[string]any) (any, error) {
	v, err := g.Value(schema)
	return ordered.Plain(v), err
}
