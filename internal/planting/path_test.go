package planting_test

import (
	"errors"
	"github.com/holgeradam/kafka-testdata-generator/internal/planting"
	"reflect"
	"testing"
)

func TestParsePath(t *testing.T) {
	cases := map[string][]planting.Step{
		"id":            {{Field: "id", Index: -1}},
		"customer.id":   {{Field: "customer", Index: -1}, {Field: "id", Index: -1}},
		"items[0].sku":  {{Field: "items", Index: -1}, {Index: 0}, {Field: "sku", Index: -1}},
		"rows[12]":      {{Field: "rows", Index: -1}, {Index: 12}},
		"a.b[1].c[2].d": {{Field: "a", Index: -1}, {Field: "b", Index: -1}, {Index: 1}, {Field: "c", Index: -1}, {Index: 2}, {Field: "d", Index: -1}},
	}
	for in, want := range cases {
		got, err := planting.ParsePath(in)
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
		var pe *planting.PathError
		if _, err := planting.ParsePath(in); !errors.As(err, &pe) {
			t.Errorf("ParsePath(%q) error = %v, want a *PathError", in, err)
		}
	}
}

// TestOverlap proves two paths overlap when one leads into or to the other,
// so planting both would have one overwrite the other.
func TestOverlap(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"region", "region", true},
		{"meta", "meta.region", true},
		{"meta.region", "meta", true},
		{"items[0]", "items[0].sku", true},
		{"meta.region", "meta.tenant", false},
		{"items[0].sku", "items[1].sku", false},
		{"region", "regions", false},
	}
	for _, c := range cases {
		a, _ := planting.ParsePath(c.a)
		b, _ := planting.ParsePath(c.b)
		if got := planting.Overlap(a, b); got != c.want {
			t.Errorf("Overlap(%s, %s) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
