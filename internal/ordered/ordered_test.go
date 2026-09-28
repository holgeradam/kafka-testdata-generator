package ordered

import (
	"encoding/json"
	"reflect"
	"testing"
)

func object(kv ...any) Object {
	var o Object
	for i := 0; i < len(kv); i += 2 {
		o.Add(kv[i].(string), kv[i+1])
	}
	return o
}

// TestObjectEncodesInOrder proves keys encode in the order they were added,
// nested Objects and arrays included.
func TestObjectEncodesInOrder(t *testing.T) {
	b, err := json.Marshal(object("z", 1, "a", []any{object("y", true, "b", nil)}))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"z":1,"a":[{"y":true,"b":null}]}`; string(b) != want {
		t.Errorf("encoded %s, want %s", b, want)
	}
}

// TestObjectGetSet proves a value is read and replaced by key, in place, so
// planting into a copy of the Object reaches the one it was copied from, and
// that an absent key is reported rather than added.
func TestObjectGetSet(t *testing.T) {
	o := object("id", "x", "region", "xx")
	alias := o
	if v, ok := o.Get("region"); !ok || v != "xx" {
		t.Errorf("Get(region) = %v, %v; want xx", v, ok)
	}
	if _, ok := o.Get("nope"); ok {
		t.Error("Get(nope) found a value, want none")
	}
	if !alias.Set("region", "eu") {
		t.Fatal("Set(region) = false, want it replaced")
	}
	if v, _ := o.Get("region"); v != "eu" {
		t.Errorf("after Set through a copy, region = %v, want eu", v)
	}
	if o.Set("nope", 1) || len(o.Keys) != 2 {
		t.Errorf("Set(nope) added a key: %v", o.Keys)
	}
}

// TestPlain proves Plain turns every Object into a map, recursively, for a
// consumer that reads plain JSON values, such as a validator.
func TestPlain(t *testing.T) {
	got := Plain([]any{object("a", object("b", 1)), "s", map[string]any{"m": object("c", 2)}})
	want := []any{map[string]any{"a": map[string]any{"b": 1}}, "s", map[string]any{"m": map[string]any{"c": 2}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Plain = %#v, want %#v", got, want)
	}
}
