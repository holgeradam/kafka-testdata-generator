package avro

import (
	"errors"
	"math/big"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// mustParse parses avsc into the model, failing the test on a parse error.
func mustParse(t *testing.T, avsc string) *Schema {
	t.Helper()
	s, err := Parse([]byte(avsc))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	return s
}

// assertGenerateError asserts err is a *GenerateError carrying a message, so
// unhonorable generation always lands on the same typed-error shape.
func assertGenerateError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var ge *GenerateError
	if !errors.As(err, &ge) {
		t.Fatalf("expected *GenerateError, got %T (%v)", err, err)
	}
	if ge.Error() == "" {
		t.Error("GenerateError must carry a message")
	}
}

func TestGenerateDeterministic(t *testing.T) {
	avsc := `{"type":"record","name":"O","fields":[
		{"name":"id","type":"string"},
		{"name":"qty","type":"int"},
		{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
		{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
		{"name":"tags","type":{"type":"array","items":"string"}}
	]}`
	root := mustParse(t, avsc).Root

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	a := NewGenerator(synth.New(42, now))
	va, err := a.Value(root)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}
	vb, err := NewGenerator(synth.New(42, now)).Value(root)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}
	if !reflect.DeepEqual(va, vb) {
		t.Errorf("same seed+now must generate identical values:\n  a: %#v\n  b: %#v", va, vb)
	}

	c := NewGenerator(synth.New(99, now))
	vc, err := c.Value(root)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}
	if reflect.DeepEqual(va, vc) {
		t.Error("different seeds must generate different values")
	}
}

func TestGeneratePrimitives(t *testing.T) {
	avsc := `{"type":"record","name":"O","fields":[
		{"name":"n","type":"null"},
		{"name":"b","type":"boolean"},
		{"name":"i","type":"int"},
		{"name":"l","type":"long"},
		{"name":"f","type":"float"},
		{"name":"d","type":"double"},
		{"name":"by","type":"bytes"},
		{"name":"s","type":"string"}
	]}`
	root := mustParse(t, avsc).Root
	g := NewGenerator(synth.New(7, time.Now()))
	rec, err := g.Value(root)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}
	m := rec.(map[string]any)

	cases := []struct {
		field string
		want  any
	}{
		{"n", nil}, // null always decodes to a nil value
	}
	for _, c := range cases {
		if got := m[c.field]; got != c.want {
			t.Errorf("%s = %v, want %v", c.field, got, c.want)
		}
	}
	types := []struct {
		field string
		want  string
	}{
		{"b", "bool"},
		{"i", "int32"},
		{"l", "int64"},
		{"f", "float32"},
		{"d", "float64"},
		{"by", "[]uint8"},
		{"s", "string"},
	}
	for _, c := range types {
		if got := m[c.field]; reflect.TypeOf(got).String() != c.want {
			t.Errorf("%s has Go type %T, want %s", c.field, got, c.want)
		}
	}
}

func TestGenerateLogicalTypes(t *testing.T) {
	avsc := `{"type":"record","name":"O","fields":[
		{"name":"day","type":{"type":"int","logicalType":"date"}},
		{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
		{"name":"tms","type":{"type":"long","logicalType":"timestamp-micros"}},
		{"name":"tm","type":{"type":"int","logicalType":"time-millis"}},
		{"name":"tmu","type":{"type":"long","logicalType":"time-micros"}},
		{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}
	]}`
	root := mustParse(t, avsc).Root
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	g := NewGenerator(synth.New(7, now))
	for i := 0; i < 20; i++ {
		m, err := g.Value(root)
		if err != nil {
			t.Fatalf("generation failed: %v", err)
		}
		rec := m.(map[string]any)

		if got := rec["day"].(time.Time); got.Unix()%86400 != 0 {
			t.Errorf("date %v is not a UTC midnight", got)
		}
		if rec["ts"].(time.Time).Location() != time.UTC {
			t.Errorf("timestamp-millis %v is not UTC", rec["ts"])
		}
		if rec["tms"].(time.Time).Location() != time.UTC {
			t.Errorf("timestamp-micros %v is not UTC", rec["tms"])
		}
		for _, f := range []string{"tm", "tmu"} {
			d := rec[f].(time.Duration)
			if d < 0 || d >= 24*time.Hour {
				t.Errorf("%s %v is outside [0, 24h)", f, d)
			}
		}
		rat := rec["amt"].(*big.Rat)
		if rat == nil || rat.Sign() != 1 {
			t.Fatalf("decimal %v is not a positive rational", rec["amt"])
		}
		// value = mantissa / 10^scale, mantissa within precision digits
		denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(2), nil)
		mant := new(big.Int).Mul(rat.Num(), denom)
		mant.Div(mant, rat.Denom())
		if mant.Sign() <= 0 || len(mant.String()) > 10 {
			t.Errorf("decimal mantissa %s exceeds precision 10 or is non-positive", mant)
		}
	}
}

func TestGenerateRecordsArraysMapsEnumsFixed(t *testing.T) {
	avsc := `{"type":"record","name":"O","namespace":"com.acme","fields":[
		{"name":"items","type":{"type":"array","items":"int"}},
		{"name":"attrs","type":{"type":"map","values":"long"}},
		{"name":"state","type":{"type":"enum","name":"State","symbols":["NEW","DONE"]}},
		{"name":"code","type":{"type":"fixed","name":"Code","size":4}},
		{"name":"child","type":{"type":"record","name":"Child","fields":[{"name":"x","type":"long"}]}}
	]}`
	root := mustParse(t, avsc).Root
	g := NewGenerator(synth.New(7, time.Now()))
	m, err := g.Value(root)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}
	rec := m.(map[string]any)

	items := rec["items"].([]any)
	if len(items) < 1 || len(items) > 5 {
		t.Errorf("array length %d outside [1,5]", len(items))
	}
	for _, it := range items {
		if reflect.TypeOf(it).String() != "int32" {
			t.Errorf("array item has Go type %T, want int32", it)
		}
	}

	attrs := rec["attrs"].(map[string]any)
	if len(attrs) < 1 || len(attrs) > 4 {
		t.Errorf("map length %d outside [1,4]", len(attrs))
	}
	for _, v := range attrs {
		if reflect.TypeOf(v).String() != "int64" {
			t.Errorf("map value has Go type %T, want int64", v)
		}
	}

	state := rec["state"].(string)
	if state != "NEW" && state != "DONE" {
		t.Errorf("enum symbol %q is not a declared symbol", state)
	}

	code := rec["code"]
	if ct := reflect.TypeOf(code); ct.Kind() != reflect.Array || ct.Elem().Kind() != reflect.Uint8 || ct.Len() != 4 {
		t.Errorf("fixed value has Go type %T, want [4]byte", code)
	}

	child := rec["child"].(map[string]any)
	if _, ok := child["x"]; !ok {
		t.Error("nested record missing field x")
	}
	if reflect.TypeOf(child["x"]).String() != "int64" {
		t.Errorf("nested long has Go type %T, want int64", child["x"])
	}
}

func TestGenerateUnionValues(t *testing.T) {
	avsc := `{"type":"record","name":"O","namespace":"com.acme","fields":[
		{"name":"maybe","type":["null","string"]},
		{"name":"choice","type":["string","int"]},
		{"name":"nested","type":["null",{"type":"record","name":"R","fields":[{"name":"y","type":"int"}]}]},
		{"name":"when","type":["null",{"type":"int","logicalType":"date"}]}
	]}`
	root := mustParse(t, avsc).Root
	g := NewGenerator(synth.New(7, time.Now()))
	for i := 0; i < 40; i++ {
		m, err := g.Value(root)
		if err != nil {
			t.Fatalf("generation failed: %v", err)
		}
		rec := m.(map[string]any)

		for _, f := range []string{"maybe", "nested", "when"} {
			u := rec[f].(map[string]any)
			if len(u) != 1 {
				t.Fatalf("union %s has %d entries, want exactly one", f, len(u))
			}
			for key, val := range u {
				if key == "null" && val != nil {
					t.Errorf("union %s null branch must carry nil, got %v", f, val)
				}
			}
		}

		choice := rec["choice"].(map[string]any)
		if len(choice) != 1 {
			t.Fatalf("non-null union choice has %d entries, want one", len(choice))
		}
		if _, ok := choice["string"]; !ok {
			if _, ok := choice["int"]; !ok {
				t.Fatalf("choice union branch key = %v, want string or int", choice)
			}
		}

		n := rec["nested"].(map[string]any)
		for key, val := range n {
			if key == "com.acme.R" {
				if _, ok := val.(map[string]any); !ok {
					t.Errorf("record branch value = %T, want map[string]any", val)
				}
			}
		}
	}

	// The optional idiom must eventually produce both null and value over seeds.
	nulls := 0
	vals := 0
	for seed := int64(0); seed < 200; seed++ {
		m, err := NewGenerator(synth.New(seed, time.Now())).Value(root)
		if err != nil {
			t.Fatalf("generation failed: %v", err)
		}
		u := m.(map[string]any)["maybe"].(map[string]any)
		if _, ok := u["null"]; ok {
			nulls++
		} else {
			vals++
		}
	}
	if nulls == 0 || vals == 0 {
		t.Errorf("optional union must emit both branches, got nulls=%d vals=%d", nulls, vals)
	}
}

func TestGenerateRecursionDepth(t *testing.T) {
	// A self-referential record without a null branch cannot be bounded: the
	// generator must stop with a typed error rather than loop or emit a tree
	// that violates the avsc (every Avro field is mandatory).
	cyclic := `{"type":"record","name":"Tree","fields":[
		{"name":"label","type":"string"},
		{"name":"left","type":"Tree"}
	]}`
	g := NewGenerator(synth.New(7, time.Now()))
	_, err := g.Value(mustParse(t, cyclic).Root)
	assertGenerateError(t, err)

	// The null-optional idiom (linked list) terminates: when the depth budget
	// runs out the generator emits null, so no error and no infinite loop.
	linked := `{"type":"record","name":"Node","fields":[
		{"name":"value","type":"long"},
		{"name":"next","type":["null","Node"]}
	]}`
	node := mustParse(t, linked).Root
	for seed := int64(0); seed < 20; seed++ {
		m, err := NewGenerator(synth.New(seed, time.Now())).Value(node)
		if err != nil {
			t.Fatalf("linked-list generation failed at seed %d: %v", seed, err)
		}
		if _, ok := m.(map[string]any); !ok {
			t.Fatalf("root value = %T, want map", m)
		}
	}
}

// TestGenerateNestedValuesInheritFieldName proves union branches, array items
// and map values use the enclosing field's name for heuristics (#31 decision
// 5), so a nullable email is still an email.
func TestGenerateNestedValuesInheritFieldName(t *testing.T) {
	avsc := `{"type":"record","name":"C","fields":[
		{"name":"email","type":["null","string"]},
		{"name":"emails","type":{"type":"array","items":"string"}},
		{"name":"backupEmails","type":{"type":"map","values":"string"}}
	]}`
	root := mustParse(t, avsc).Root
	emailRe := regexp.MustCompile(`^[a-z]+\.[a-z]+@[a-z]+\.[a-z]+$`)
	g := NewGenerator(synth.New(7, time.Now()))
	for i := 0; i < 40; i++ {
		m, err := g.Value(root)
		if err != nil {
			t.Fatalf("generation failed: %v", err)
		}
		rec := m.(map[string]any)
		if u := rec["email"].(map[string]any); u["string"] != nil && !emailRe.MatchString(u["string"].(string)) {
			t.Errorf("nullable email = %q, want email-shaped", u["string"])
		}
		for _, v := range rec["emails"].([]any) {
			if !emailRe.MatchString(v.(string)) {
				t.Errorf("emails item = %q, want email-shaped", v)
			}
		}
		for _, v := range rec["backupEmails"].(map[string]any) {
			if !emailRe.MatchString(v.(string)) {
				t.Errorf("backupEmails value = %q, want email-shaped", v)
			}
		}
	}
}

// TestGenerateInstantsWithinWindow proves dates and timestamps fall within the
// 365 days before now (#31 decision 7); a date may round down to midnight.
func TestGenerateInstantsWithinWindow(t *testing.T) {
	avsc := `{"type":"record","name":"W","fields":[
		{"name":"day","type":{"type":"int","logicalType":"date"}},
		{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
		{"name":"tms","type":{"type":"long","logicalType":"timestamp-micros"}}
	]}`
	root := mustParse(t, avsc).Root
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	g := NewGenerator(synth.New(7, now))
	for i := 0; i < 200; i++ {
		m, err := g.Value(root)
		if err != nil {
			t.Fatalf("generation failed: %v", err)
		}
		rec := m.(map[string]any)
		for f, slack := range map[string]time.Duration{"day": 24 * time.Hour, "ts": 0, "tms": 0} {
			v := rec[f].(time.Time)
			if v.After(now) || now.Sub(v) > 365*24*time.Hour+slack {
				t.Fatalf("%s = %v, outside the 365 days before %v", f, v, now)
			}
		}
	}
}
