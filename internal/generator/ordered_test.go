package generator

import (
	"encoding/json"
	"testing"
)

// orderOf builds an object schema whose properties are declared in the given
// order, as the spec reader records it (#96).
func orderOf(props ...any) map[string]any {
	properties := map[string]any{}
	var names []any
	for i := 0; i < len(props); i += 2 {
		properties[props[i].(string)] = props[i+1]
		names = append(names, props[i])
	}
	return map[string]any{"type": "object", "properties": properties, OrderKeyword: names}
}

func encoded(t *testing.T, schema map[string]any, value any) string {
	t.Helper()
	b, err := json.Marshal(Ordered(schema, value))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestOrderedFollowsDeclaredOrder proves an object's keys come out in the
// order its schema declares its properties, at every level: nested objects,
// array items, through a $ref into $defs, and through allOf, oneOf and anyOf.
// Keys the schema does not declare follow, sorted; a schema without a
// recorded order keeps the sorted order.
func TestOrderedFollowsDeclaredOrder(t *testing.T) {
	str := map[string]any{"type": "string"}
	cases := map[string]struct {
		schema map[string]any
		value  any
		want   string
	}{
		"flat": {orderOf("zeta", str, "alpha", str), map[string]any{"alpha": "a", "zeta": "z"}, `{"zeta":"z","alpha":"a"}`},
		"nested": {orderOf("b", orderOf("y", str, "x", str), "a", str),
			map[string]any{"a": "1", "b": map[string]any{"x": "2", "y": "3"}}, `{"b":{"y":"3","x":"2"},"a":"1"}`},
		"array items": {orderOf("items", map[string]any{"type": "array", "items": orderOf("n", str, "m", str)}),
			map[string]any{"items": []any{map[string]any{"m": "1", "n": "2"}}}, `{"items":[{"n":"2","m":"1"}]}`},
		"ref": {map[string]any{"$ref": "#/$defs/Node", "$defs": map[string]any{"Node": orderOf("next", map[string]any{"$ref": "#/$defs/Node"}, "id", str)}},
			map[string]any{"id": "1", "next": map[string]any{"id": "2"}}, `{"next":{"id":"2"},"id":"1"}`},
		"allOf": {map[string]any{"allOf": []any{orderOf("z", str), orderOf("b", str, "a", str)}},
			map[string]any{"a": "1", "b": "2", "z": "3"}, `{"z":"3","b":"2","a":"1"}`},
		"oneOf": {map[string]any{"oneOf": []any{orderOf("q", str, "p", str), orderOf("y", str, "x", str)}},
			map[string]any{"x": "1", "y": "2"}, `{"y":"2","x":"1"}`},
		"undeclared keys": {orderOf("b", str), map[string]any{"d": "1", "c": "2", "b": "3"}, `{"b":"3","c":"2","d":"1"}`},
		"no recorded order": {map[string]any{"type": "object", "properties": map[string]any{"b": str, "a": str}},
			map[string]any{"b": "1", "a": "2"}, `{"a":"2","b":"1"}`},
		"scalars": {str, "x", `"x"`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := encoded(t, c.schema, c.value); got != c.want {
				t.Errorf("encoded %s, want %s", got, c.want)
			}
		})
	}
}

// TestOrderedEscapesAsJSONDoes proves ordering changes nothing but the order:
// strings escape exactly as encoding/json escapes them.
func TestOrderedEscapesAsJSONDoes(t *testing.T) {
	value := map[string]any{"a": "<tag> & \"q\"", "b": 1.5}
	want, _ := json.Marshal(value)
	if got := encoded(t, orderOf("a", map[string]any{}, "b", map[string]any{}), value); got != string(want) {
		t.Errorf("encoded %s, want %s", got, want)
	}
}
