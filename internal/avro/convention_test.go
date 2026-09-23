package avro

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	codec "github.com/confluentinc/confluent-avro-go/v2"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// logicalFixtures holds one avsc per logical type in the convention table,
// wrapped in a nullable union and a record so the overlay is exercised as a
// field, a union branch, and on every base type it allows. A table entry
// without a fixture fails TestConventionCoversEveryLogicalType.
var logicalFixtures = map[LogicalTypeKind][]string{
	"date":                   {`{"type":"int","logicalType":"date"}`},
	"time-millis":            {`{"type":"int","logicalType":"time-millis"}`},
	"time-micros":            {`{"type":"long","logicalType":"time-micros"}`},
	"timestamp-millis":       {`{"type":"long","logicalType":"timestamp-millis"}`},
	"timestamp-micros":       {`{"type":"long","logicalType":"timestamp-micros"}`},
	"local-timestamp-millis": {`{"type":"long","logicalType":"local-timestamp-millis"}`},
	"local-timestamp-micros": {`{"type":"long","logicalType":"local-timestamp-micros"}`},
	"uuid":                   {`{"type":"string","logicalType":"uuid"}`},
	"decimal": {
		`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"fixed","name":"Price","size":8,"logicalType":"decimal","precision":12,"scale":3}`,
	},
}

// structuralFixtures cover every construct outside the logical table: each
// primitive, and record, enum, fixed, union, array and map.
var structuralFixtures = []string{
	`"null"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"bytes"`, `"string"`,
	`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
	`{"type":"fixed","name":"F","size":4}`,
	`{"type":"array","items":"long"}`,
	`{"type":"map","values":"string"}`,
	`["null","string",{"type":"record","name":"Inner","fields":[{"name":"n","type":"int"}]}]`,
	`{"type":"record","name":"Node","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","Node"]}]}`,
}

// TestConventionCoversEveryLogicalType proves the fixtures and the table stay
// in step: adding a logical type without a fixture, or leaving a fixture for a
// type the table no longer holds, fails here.
func TestConventionCoversEveryLogicalType(t *testing.T) {
	for _, lt := range logicalTypes {
		if len(logicalFixtures[lt.kind]) == 0 {
			t.Errorf("logical type %q has no fixture in logicalFixtures", lt.kind)
		}
	}
	for kind := range logicalFixtures {
		if lookupLogical(kind) == nil {
			t.Errorf("fixture for %q, which the convention table does not hold", kind)
		}
	}
}

// TestConventionRejectsWrongBase proves every table entry refuses a base type
// it does not list, with a typed error rather than a model the codec would
// encode as the bare base type.
func TestConventionRejectsWrongBase(t *testing.T) {
	for _, lt := range logicalTypes {
		for _, base := range []TypeKind{KindInt, KindLong, KindString, KindBytes} {
			if lt.allows(base) {
				continue
			}
			avsc := fmt.Sprintf(`{"type":%q,"logicalType":%q,"precision":4}`, base, lt.kind)
			_, err := Parse([]byte(avsc))
			assertParseError(t, err)
		}
	}
}

// TestConventionProperty is the one test the value convention is held to
// (#66): for every logical type and every structural construct, and a range
// of seeds, the generated value renders in the Avro JSON encoding, marshals
// with the codec against the same parse, and decodes back to the same datum.
// Synthesis, rendering and binary encoding therefore agree by test rather than
// by hand.
func TestConventionProperty(t *testing.T) {
	var fixtures []string
	for _, lt := range logicalTypes {
		for _, f := range logicalFixtures[lt.kind] {
			fixtures = append(fixtures,
				f,
				fmt.Sprintf(`["null",%s]`, f),
				fmt.Sprintf(`{"type":"record","name":"Wrap","fields":[{"name":"v","type":%s}]}`, f))
		}
	}
	fixtures = append(fixtures, structuralFixtures...)

	api := codec.Config{}.Freeze()
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, avsc := range fixtures {
		t.Run(avsc, func(t *testing.T) {
			model, err := Parse([]byte(avsc))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			for seed := int64(0); seed < 12; seed++ {
				v, err := NewGenerator(synth.New(seed, now)).Value(model.Root)
				if err != nil {
					t.Fatalf("seed %d: generate: %v", seed, err)
				}
				if _, err := RenderJSON(model.Root, v); err != nil {
					t.Fatalf("seed %d: render %#v: %v", seed, v, err)
				}
				wire, err := api.Marshal(model.Codec(), v)
				if err != nil {
					t.Fatalf("seed %d: marshal %#v: %v", seed, v, err)
				}
				var decoded any
				if err := api.Unmarshal(model.Codec(), wire, &decoded); err != nil {
					t.Fatalf("seed %d: decode: %v", seed, err)
				}
				if err := sameDatum(model.Root, v, decoded); err != nil {
					t.Fatalf("seed %d: decoded %#v differs from generated %#v: %v", seed, decoded, v, err)
				}
			}
		})
	}
}

// sameDatum reports whether decoded - what the codec's generic decoder returns
// for the bytes it encoded from generated - is the same Avro datum as
// generated. It is the test's oracle, so it compares across the two shapes the
// codec uses rather than re-encoding (a lossy first encode would re-encode to
// the same bytes): decoded unions may arrive unwrapped, ints as int, local
// timestamps in time.Local. A timestamp compares as an instant, a local
// timestamp by wall clock, a decimal by value.
func sameDatum(t Type, generated, decoded any) error {
	switch m := t.(type) {
	case *Primitive:
		if m.Logical != nil {
			return sameLogical(m.Logical, generated, decoded)
		}
		if b, ok := generated.([]byte); ok {
			if d, ok := decoded.([]byte); !ok || string(b) != string(d) {
				return fmt.Errorf("bytes %x, decoded %#v", b, decoded)
			}
			return nil
		}
		if fmt.Sprint(generated) != fmt.Sprint(decoded) {
			return fmt.Errorf("%s %v, decoded %v", m.Kind, generated, decoded)
		}
		return nil
	case *Fixed:
		if m.Logical != nil {
			return sameLogical(m.Logical, generated, decoded)
		}
		if fmt.Sprint(generated) != fmt.Sprint(decoded) {
			return fmt.Errorf("fixed %v, decoded %v", generated, decoded)
		}
		return nil
	case *Enum:
		if generated != decoded {
			return fmt.Errorf("enum %v, decoded %v", generated, decoded)
		}
		return nil
	case *Record:
		g, d := generated.(map[string]any), decoded.(map[string]any)
		for _, f := range m.Fields {
			if err := sameDatum(f.Type, g[f.Name], d[f.Name]); err != nil {
				return fmt.Errorf("%s.%s: %w", m.Name, f.Name, err)
			}
		}
		return nil
	case *Array:
		g, d := generated.([]any), decoded.([]any)
		if len(g) != len(d) {
			return fmt.Errorf("array of %d, decoded %d", len(g), len(d))
		}
		for i := range g {
			if err := sameDatum(m.Items, g[i], d[i]); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
		return nil
	case *Map:
		g, d := generated.(map[string]any), decoded.(map[string]any)
		if len(g) != len(d) {
			return fmt.Errorf("map of %d, decoded %d", len(g), len(d))
		}
		for k := range g {
			if err := sameDatum(m.Values, g[k], d[k]); err != nil {
				return fmt.Errorf("[%q]: %w", k, err)
			}
		}
		return nil
	case *Union:
		for name, v := range generated.(map[string]any) {
			for _, b := range m.Branches {
				if unionBranchName(b) != name {
					continue
				}
				if w, ok := decoded.(map[string]any); ok && len(w) == 1 {
					if inner, ok := w[name]; ok {
						decoded = inner
					}
				}
				return sameDatum(b, v, decoded)
			}
			return fmt.Errorf("union branch %q not in schema", name)
		}
		return fmt.Errorf("union value carries no branch")
	}
	return fmt.Errorf("unknown model node %T", t)
}

func sameLogical(lt *LogicalType, generated, decoded any) error {
	switch g := generated.(type) {
	case time.Time:
		d, ok := decoded.(time.Time)
		const wall = "2006-01-02T15:04:05.999999999"
		switch {
		case !ok:
			return fmt.Errorf("%s %v, decoded %#v", lt.Kind, g, decoded)
		case strings.HasPrefix(string(lt.Kind), "local-"):
			if g.Format(wall) != d.Format(wall) {
				return fmt.Errorf("%s wall clock %s, decoded %s", lt.Kind, g.Format(wall), d.Format(wall))
			}
		case !g.Equal(d):
			return fmt.Errorf("%s %v, decoded %v", lt.Kind, g, d)
		}
		return nil
	case *big.Rat:
		if d, ok := decoded.(*big.Rat); !ok || g.Cmp(d) != 0 {
			return fmt.Errorf("%s %v, decoded %#v", lt.Kind, g, decoded)
		}
		return nil
	}
	if fmt.Sprint(generated) != fmt.Sprint(decoded) {
		return fmt.Errorf("%s %v, decoded %v", lt.Kind, generated, decoded)
	}
	return nil
}
