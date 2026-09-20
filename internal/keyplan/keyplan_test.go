package keyplan

import (
	"errors"
	"reflect"
	"testing"
)

// fakeGenerator is the Key source in these tests: it yields values in order,
// then repeats the last one.
type fakeGenerator struct {
	values []any
	err    error
	calls  int
}

func (g *fakeGenerator) Value() (any, error) {
	if g.err != nil {
		return nil, g.err
	}
	v := g.values[min(g.calls, len(g.values)-1)]
	g.calls++
	return v, nil
}

// fakeChecker records the path it was asked about and reports a fixed verdict.
type fakeChecker struct {
	err  error
	seen []Step
}

func (c *fakeChecker) Check(path []Step) error {
	c.seen = path
	return c.err
}

func TestParsePath(t *testing.T) {
	cases := map[string][]Step{
		"id":            {{Field: "id", Index: -1}},
		"customer.id":   {{Field: "customer", Index: -1}, {Field: "id", Index: -1}},
		"items[0].sku":  {{Field: "items", Index: -1}, {Index: 0}, {Field: "sku", Index: -1}},
		"rows[12]":      {{Field: "rows", Index: -1}, {Index: 12}},
		"a.b[1].c[2].d": {{Field: "a", Index: -1}, {Field: "b", Index: -1}, {Index: 1}, {Field: "c", Index: -1}, {Index: 2}, {Field: "d", Index: -1}},
	}
	for in, want := range cases {
		got, err := ParsePath(in)
		if err != nil {
			t.Errorf("ParsePath(%q) error: %v", in, err)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("ParsePath(%q) = %+v, want %+v", in, got, want)
		}
	}
}

func TestParsePathRejectsMalformed(t *testing.T) {
	for _, in := range []string{"", "items[", "items[]", "items[x]", "items[-1]"} {
		if _, err := ParsePath(in); err == nil {
			t.Errorf("ParsePath(%q) = nil error, want *PathError", in)
		} else {
			var pe *PathError
			if !errors.As(err, &pe) {
				t.Errorf("ParsePath(%q) error = %T, want *PathError", in, err)
			}
		}
	}
}

// TestNewRunsChecker proves the startup checks run in the constructor, against
// the parsed path, and that a failed check stops the plan from existing.
func TestNewRunsChecker(t *testing.T) {
	c := &fakeChecker{}
	if _, err := New(&fakeGenerator{values: []any{"k"}}, c, "customer.id"); err != nil {
		t.Fatalf("New error: %v", err)
	}
	want := []Step{{Field: "customer", Index: -1}, {Field: "id", Index: -1}}
	if !reflect.DeepEqual(c.seen, want) {
		t.Errorf("checker saw %+v, want %+v", c.seen, want)
	}

	boom := errors.New("path is not guaranteed")
	if _, err := New(&fakeGenerator{values: []any{"k"}}, &fakeChecker{err: boom}, "customer.id"); !errors.Is(err, boom) {
		t.Errorf("New error = %v, want the checker's error", err)
	}
}

// TestApplyPlantsValue proves Apply returns the generated Key and plants the
// same value into the Payload, so the two can never disagree.
func TestApplyPlantsValue(t *testing.T) {
	cases := []struct {
		name    string
		path    string
		payload map[string]any
		want    func(map[string]any) any
	}{
		{"top level", "id", map[string]any{"id": "old"}, func(p map[string]any) any { return p["id"] }},
		{"nested", "customer.id", map[string]any{"customer": map[string]any{"id": "old"}},
			func(p map[string]any) any { return p["customer"].(map[string]any)["id"] }},
		{"array item", "items[1].sku", map[string]any{"items": []any{
			map[string]any{"sku": "a"}, map[string]any{"sku": "b"}}},
			func(p map[string]any) any { return p["items"].([]any)[1].(map[string]any)["sku"] }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan, err := New(&fakeGenerator{values: []any{"planted"}}, &fakeChecker{}, c.path)
			if err != nil {
				t.Fatalf("New error: %v", err)
			}
			key, err := plan.Apply(c.payload)
			if err != nil {
				t.Fatalf("Apply error: %v", err)
			}
			if key != "planted" {
				t.Errorf("Apply key = %v, want the generated value", key)
			}
			if got := c.want(c.payload); got != key {
				t.Errorf("payload holds %v at %s, want the Key %v", got, c.path, key)
			}
		})
	}
}

// TestApplyWithoutPathLeavesPayload proves a plan with no path still produces a
// Key and does not touch the Payload.
func TestApplyWithoutPathLeavesPayload(t *testing.T) {
	plan, err := New(&fakeGenerator{values: []any{"k1", "k2"}}, nil, "")
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	payload := map[string]any{"id": "untouched"}
	key, err := plan.Apply(payload)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if key != "k1" {
		t.Errorf("Apply key = %v, want k1", key)
	}
	if payload["id"] != "untouched" {
		t.Errorf("payload was modified: %v", payload)
	}
	if key, _ := plan.Apply(payload); key != "k2" {
		t.Errorf("second Apply key = %v, want a fresh draw k2", key)
	}
}

func TestApplyPropagatesGeneratorError(t *testing.T) {
	boom := errors.New("key schema cannot be honoured")
	plan, err := New(&fakeGenerator{err: boom}, &fakeChecker{}, "id")
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if _, err := plan.Apply(map[string]any{"id": ""}); !errors.Is(err, boom) {
		t.Errorf("Apply error = %v, want the generator's error", err)
	}
}

// TestApplyMissingPathErrors covers the case the startup checks are meant to
// rule out: the Payload does not carry the path. It must be a typed error, not
// a silent miss.
func TestApplyMissingPathErrors(t *testing.T) {
	plan, err := New(&fakeGenerator{values: []any{"k"}}, &fakeChecker{}, "customer.id")
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	for _, payload := range []map[string]any{
		{},
		{"customer": nil},
		{"customer": "not an object"},
	} {
		if _, err := plan.Apply(payload); err == nil {
			t.Errorf("Apply(%v) = nil error, want a plant error", payload)
		}
	}
}
