package planting

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
)

// fakeWalk stands in for a schema language: the paths it guarantees, by their
// dotted form, each with the values its field holds and whether it holds the
// Key. A token indexes an array where the guaranteed path has an index there.
type fakeWalk map[string]fakeField

type fakeField struct {
	values []string
	keyErr error
}

func (w fakeWalk) Locate(path []Step) ([]Step, Field, error) {
	steps := make([]Step, len(path))
	for i, s := range path {
		steps[i] = s
		if s.Token {
			steps[i] = Step{Field: s.Field, Index: -1}
			if n, err := strconv.Atoi(s.Field); err == nil {
				if _, ok := w.prefix(append(steps[:i:i], Step{Index: n})); ok {
					steps[i] = Step{Index: n}
				}
			}
		}
		if _, ok := w.prefix(steps[:i+1]); !ok {
			return nil, nil, &StepError{Step: i, Err: errors.New("no such field")}
		}
	}
	f, ok := w[PathString(steps)]
	if !ok {
		return nil, nil, &StepError{Step: len(path) - 1, Err: errors.New("no such field")}
	}
	return steps, f, nil
}

// prefix reports whether some guaranteed path starts with steps.
func (w fakeWalk) prefix(steps []Step) (string, bool) {
	p := PathString(steps)
	for k := range w {
		if k == p || strings.HasPrefix(k, p+".") || strings.HasPrefix(k, p+"[") {
			return k, true
		}
	}
	return "", false
}

func (f fakeField) HoldsKey() error { return f.keyErr }

func (f fakeField) Holds(value string) error {
	for _, v := range f.values {
		if v == value {
			return nil
		}
	}
	return fmt.Errorf("not one of %v", f.values)
}

// orderWalk guarantees id, meta.region and items[0].sku in the Payload.
func orderWalk() fakeWalk {
	return fakeWalk{
		"id":           {values: []string{"o1"}},
		"meta.region":  {values: []string{"eu", "us"}},
		"items[0].sku": {values: []string{"s1"}},
	}
}

func tenantHeaders() fakeWalk { return fakeWalk{"tenant": {values: []string{"acme"}}} }

func payloadParam(name, value string, pointer ...string) Parameter {
	return Parameter{Name: name, Value: value, Location: "$message.payload#/" + strings.Join(pointer, "/"), Pointer: pointer}
}

func headerParam(name, value string, pointer ...string) Parameter {
	return Parameter{Name: name, Value: value, Location: "$message.header#/" + strings.Join(pointer, "/"), Pointer: pointer, InHeaders: true}
}

// TestPlantEveryPlanting proves each Message type's Set plants the Key at the
// Key path and every Topic parameter at its location, in the Payload or the
// Headers, reading a pointer token as an index where the schema has an array.
func TestPlantEveryPlanting(t *testing.T) {
	types := []MessageType{
		{Name: "A", Payload: orderWalk(), Headers: tenantHeaders()},
		{Name: "B", Payload: orderWalk(), Headers: tenantHeaders()},
	}
	sets, err := New(types, "id", []Parameter{
		payloadParam("region", "eu", "meta", "region"),
		payloadParam("sku", "s1", "items", "0", "sku"),
		headerParam("tenant", "acme", "tenant"),
		{Name: "env", Value: "prod"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i, set := range sets {
		payload := map[string]any{"id": "x", "meta": map[string]any{"region": "xx"}, "items": []any{map[string]any{"sku": "x"}}}
		headers := map[string]any{"tenant": "x"}
		if err := set.Plant(payload, headers, "k1"); err != nil {
			t.Fatalf("type %d: Plant: %v", i, err)
		}
		want := map[string]any{"id": "k1", "meta": map[string]any{"region": "eu"}, "items": []any{map[string]any{"sku": "s1"}}}
		if !reflect.DeepEqual(payload, want) {
			t.Errorf("type %d: Payload = %v, want %v", i, payload, want)
		}
		if headers["tenant"] != "acme" {
			t.Errorf("type %d: Headers = %v, want tenant acme", i, headers)
		}
	}
}

// TestPlantWithoutPlantings proves a run with no Key path and no located
// Topic parameter leaves every value as generated, the Key included.
func TestPlantWithoutPlantings(t *testing.T) {
	sets, err := New([]MessageType{{Name: "A", Payload: orderWalk()}}, "", []Parameter{{Name: "env", Value: "prod"}})
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"id": "x"}
	if err := sets[0].Plant(payload, nil, "k1"); err != nil || payload["id"] != "x" {
		t.Errorf("Plant = %v, payload %v; want it untouched", err, payload)
	}
}

// TestNewRefuses proves every Planting the run cannot honour stops it before
// any record exists, owned by the flag it comes from, naming the Message type
// when there are several.
func TestNewRefuses(t *testing.T) {
	both := []MessageType{{Name: "A", Payload: orderWalk(), Headers: tenantHeaders()}, {Name: "B", Payload: fakeWalk{"id": {}}, Headers: tenantHeaders()}}
	one := both[:1]
	keyTooWide := []MessageType{{Name: "A", Payload: fakeWalk{"id": {keyErr: errors.New("the schema here is an integer but the key schema is a string")}}}}
	cases := map[string]struct {
		types   []MessageType
		keyPath string
		params  []Parameter
		flag    string
		want    string
	}{
		"location not guaranteed in one type": {both, "", []Parameter{payloadParam("region", "eu", "meta", "region")}, "topic",
			`Topic parameter region: location $message.payload#/meta/region: in Message type B: at "/meta": no such field`},
		"value the field refuses": {one, "", []Parameter{payloadParam("region", "asia", "meta", "region")}, "topic",
			"Topic parameter region: value asia does not conform to the Payload field at $message.payload#/meta/region: not one of [eu us]"},
		"value the header refuses": {one, "", []Parameter{headerParam("tenant", "other", "tenant")}, "topic",
			"Topic parameter tenant: value other does not conform to the header at $message.header#/tenant: not one of [acme]"},
		"a Message type without headers": {[]MessageType{one[0], {Name: "B", Payload: orderWalk()}}, "", []Parameter{headerParam("tenant", "acme", "tenant")}, "topic",
			"Topic parameter tenant: location $message.header#/tenant: in Message type B: B declares no headers"},
		"header location not guaranteed": {one, "", []Parameter{headerParam("trace", "t", "trace")}, "topic",
			`Topic parameter trace: location $message.header#/trace: at "/trace": no such field`},
		"two parameters in one field": {one, "", []Parameter{payloadParam("region", "eu", "meta", "region"), payloadParam("area", "us", "meta", "region")}, "topic",
			"Topic parameters region and area plant into the same field ($message.payload#/meta/region and $message.payload#/meta/region)"},
		"two parameters in one header": {one, "", []Parameter{headerParam("tenant", "acme", "tenant"), headerParam("org", "acme", "tenant")}, "topic",
			"Topic parameters tenant and org plant into the same field"},
		"a parameter over the Key path": {one, "meta", []Parameter{payloadParam("region", "eu", "meta", "region")}, "keyPath",
			"Topic parameter region: location $message.payload#/meta/region overlaps -keyPath meta; both would plant into the same field"},
		"malformed Key path": {one, "items[", nil, "keyPath",
			`-keyPath "items[": an array index is missing its closing bracket`},
		"Key path not guaranteed in one type": {both, "meta.region", nil, "keyPath",
			`in Message type B: -keyPath "meta.region": at "meta": no such field`},
		"Key the field cannot hold": {keyTooWide, "id", nil, "keyPath",
			`-keyPath "id": at "id": the schema here is an integer but the key schema is a string`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := New(c.types, c.keyPath, c.params)
			var pe *Error
			if !errors.As(err, &pe) || pe.Flag != c.flag {
				t.Fatalf("err = %v, want a *planting.Error of -%s", err, c.flag)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %q, want it to mention %q", err, c.want)
			}
			if c.flag == "keyPath" && !strings.Contains(name, "over") {
				var path *PathError
				if !errors.As(err, &path) {
					t.Errorf("err = %v, want a *PathError for a Key path refusal", err)
				}
			}
		})
	}
}

// TestNewKeyPathBesideHeaders proves the Key never clashes with a header
// location: the Key is planted into the Payload only.
func TestNewKeyPathBesideHeaders(t *testing.T) {
	types := []MessageType{{Name: "A", Payload: fakeWalk{"tenant": {}}, Headers: tenantHeaders()}}
	if _, err := New(types, "tenant", []Parameter{headerParam("tenant", "acme", "tenant")}); err != nil {
		t.Errorf("New = %v, want a Key path and a header location of one name accepted", err)
	}
}

// TestNewEscapesPointers proves a refused location names its failing token as
// a JSON Pointer, escaping ~ and / as RFC 6901 does.
func TestNewEscapesPointers(t *testing.T) {
	_, err := New([]MessageType{{Name: "A", Payload: orderWalk()}}, "", []Parameter{{Name: "p", Value: "v", Location: "$message.payload#/a~1b/c~0d", Pointer: []string{"a/b", "c~d"}}})
	if want := `at "/a~1b": no such field`; err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err = %v, want it to mention %q", err, want)
	}
}

// TestPlantMissNamesItsOwner proves a value that does not carry a checked
// path, a defect, is reported against the Planting it belongs to: a Topic
// parameter never as -keyPath, the Key as -keyPath (#107).
func TestPlantMissNamesItsOwner(t *testing.T) {
	sets, err := New([]MessageType{{Name: "A", Payload: orderWalk(), Headers: tenantHeaders()}}, "id", []Parameter{payloadParam("region", "eu", "meta", "region")})
	if err != nil {
		t.Fatal(err)
	}
	err = sets[0].Plant(map[string]any{"id": "x", "meta": map[string]any{"source": "web"}}, nil, "k1")
	var pe *Error
	if !errors.As(err, &pe) || pe.Flag != "topic" {
		t.Fatalf("Plant = %v, want a *planting.Error of -topic", err)
	}
	for _, want := range []string{"Topic parameter region", "$message.payload#/meta/region", `no field "region"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Plant = %q, want it to mention %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "keyPath") {
		t.Errorf("Plant = %q names -keyPath for a Topic parameter", err)
	}

	err = sets[0].Plant(map[string]any{"meta": map[string]any{"region": "xx"}}, nil, "k1")
	var path *PathError
	if !errors.As(err, &pe) || pe.Flag != "keyPath" || !errors.As(err, &path) || !strings.Contains(err.Error(), `the Payload has no field "id"`) {
		t.Errorf("Plant = %v, want a -keyPath *PathError saying the Payload has no field id", err)
	}
}

// TestPlantMissingPath covers what New rules out: a value that does not carry
// a checked path. Planting must stop with a typed error, not miss silently.
func TestPlantMissingPath(t *testing.T) {
	sets, err := New([]MessageType{{Name: "A", Payload: fakeWalk{"customer.id": {}}}}, "customer.id", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []any{
		map[string]any{},
		map[string]any{"customer": nil},
		map[string]any{"customer": "not an object"},
		[]any{},
	} {
		var pe *PathError
		if err := sets[0].Plant(payload, nil, "k"); !errors.As(err, &pe) {
			t.Errorf("Plant(%v) = %v, want a *PathError", payload, err)
		}
	}
}

// TestPlantIntoOrderedObjects proves planting reaches into the ordered
// objects the JSON Schema generator emits (#112) as into plain maps, keeping
// every key where it is.
func TestPlantIntoOrderedObjects(t *testing.T) {
	sets, err := New([]MessageType{{Name: "A", Payload: orderWalk(), Headers: tenantHeaders()}}, "id", []Parameter{
		payloadParam("region", "eu", "meta", "region"),
		payloadParam("sku", "s1", "items", "0", "sku"),
		headerParam("tenant", "acme", "tenant"),
	})
	if err != nil {
		t.Fatal(err)
	}
	obj := func(kv ...any) ordered.Object {
		var o ordered.Object
		for i := 0; i < len(kv); i += 2 {
			o.Add(kv[i].(string), kv[i+1])
		}
		return o
	}
	payload := obj("meta", obj("region", "xx", "at", 1), "id", "x", "items", []any{obj("sku", "x")})
	headers := obj("trace", "t", "tenant", "x")
	if err := sets[0].Plant(payload, headers, "k1"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		v    any
		want string
	}{
		{payload, `{"meta":{"region":"eu","at":1},"id":"k1","items":[{"sku":"s1"}]}`},
		{headers, `{"trace":"t","tenant":"acme"}`},
	} {
		if b, _ := json.Marshal(c.v); string(b) != c.want {
			t.Errorf("planted %s, want %s", b, c.want)
		}
	}
	if err := sets[0].Plant(obj("id", "x", "meta", obj("source", "web")), nil, "k"); err == nil || !strings.Contains(err.Error(), `no field "region"`) {
		t.Errorf("Plant into an Object without the field = %v, want the miss named", err)
	}
}
