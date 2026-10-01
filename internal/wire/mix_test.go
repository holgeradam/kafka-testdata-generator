package wire_test

import (
	"errors"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/generator"
	"github.com/holgeradam/kafka-testdata-generator/internal/ordered"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
)

// source is a ValueSource that logs its draw and yields a fresh value.
type source struct {
	name  string
	log   *[]string
	value func() any
	err   error
}

func (s source) Value() (any, error) {
	*s.log = append(*s.log, s.name)
	return s.value(), s.err
}

func order() map[string]any { return map[string]any{"id": "x", "region": "xx"} }

// orderPayload is the Payload schema order() conforms to.
func orderPayload() map[string]any {
	return map[string]any{"type": "object", "required": []any{"id", "region"}, "properties": map[string]any{
		"id": map[string]any{"type": "string"}, "region": map[string]any{"type": "string"},
	}}
}

func testSynth() *synth.Synthesizer { return synth.New(1, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) }

// encoding is the test's per-type encoding data: it proves Bound carries a
// format's own data typed, here the name a test encoder would write.
type encoding struct{ label string }

// bound binds a Message type whose Payload is order(), logging each draw
// under its name, checked for Plantings against orderPayload and the key.
func bound(name string, log *[]string, headers, key map[string]any) wire.Bound[encoding] {
	return wire.Bound[encoding]{
		Name:     name,
		Payload:  source{name: name, log: log, value: func() any { return order() }},
		Walk:     generator.NewWalk(orderPayload(), key),
		Headers:  headers,
		Encoding: encoding{label: "enc-" + name},
	}
}

// mustPlant gives each bound Message type its Plantings.
func mustPlant(t *testing.T, types []wire.Bound[encoding], keyPath string, params ...asyncapi.TopicParameter) {
	t.Helper()
	if err := wire.Plant(types, keyPath, params); err != nil {
		t.Fatalf("Plant: %v", err)
	}
}

// keySource generates Keys from a key schema on the run's Synthesizer.
type keySource struct {
	gen    *generator.Generator
	schema map[string]any
}

func (k keySource) Value() (any, error) { return k.gen.Value(k.schema) }

// TestMixMakesTheWholeMessage proves the Mix generates a message's Payload,
// then its Headers, then its Key - the order the seeded stream has always
// been drawn in - and plants every Planting before the Headers are encoded
// (#108).
func TestMixMakesTheWholeMessage(t *testing.T) {
	var log []string
	key := map[string]any{"type": "string", "format": "uuid"}
	types := []wire.Bound[encoding]{bound("A", &log, tenantHeaders(), key)}
	mustPlant(t, types, "id", headerParam("tenant", "acme", "tenant"),
		asyncapi.TopicParameter{Name: "region", Value: "eu", Location: "$message.payload#/region", Pointer: []string{"region"}})
	s := testSynth()
	g, err := wire.NewMix(s, types, keySource{gen: generator.New(s), schema: key}).Generate()
	if err != nil {
		t.Fatal(err)
	}

	// The same seed, drawn Headers first and Key second, must give this Key.
	replay := testSynth()
	if _, err := generator.New(replay).Value(tenantHeaders()); err != nil {
		t.Fatal(err)
	}
	wantKey, err := generator.New(replay).Value(key)
	if err != nil {
		t.Fatal(err)
	}
	if g.Key != wantKey {
		t.Errorf("Key = %v, want %v: the Key is drawn after the Headers", g.Key, wantKey)
	}
	if want := map[string]any{"id": wantKey, "region": "eu"}; !reflect.DeepEqual(g.Payload, want) {
		t.Errorf("Payload = %v, want %v: the Key and region planted", g.Payload, want)
	}
	if len(g.Headers) == 0 || !reflect.DeepEqual(g.Headers[0], pipeline.Header{Name: "tenant", Value: []byte("acme")}) {
		t.Errorf("Headers = %v, want tenant=acme planted", g.Headers)
	}
}

// TestMixPicksABoundType proves each record is of one bound Message type,
// picked from the seeded stream, and Generated.Type indexes the very slice
// the Mix was given, so a format's encoding data is found by it.
func TestMixPicksABoundType(t *testing.T) {
	var log []string
	types := []wire.Bound[encoding]{bound("A", &log, nil, nil), bound("B", &log, nil, nil), bound("C", &log, nil, nil)}
	mustPlant(t, types, "")
	m := wire.NewMix(testSynth(), types, nil)
	seen := map[string]bool{}
	for n := 0; n < 60; n++ {
		log = nil
		g, err := m.Generate()
		if err != nil {
			t.Fatal(err)
		}
		if len(log) != 1 || log[0] != types[g.Type].Name {
			t.Fatalf("record of Type %d drew %v, want the Payload of %s", g.Type, log, types[g.Type].Name)
		}
		seen[types[g.Type].Encoding.label] = true
	}
	if len(seen) != 3 {
		t.Errorf("types seen %v, want all three", seen)
	}
}

// TestMixWithoutKey proves a run without a key schema generates messages with
// a null Key and draws nothing for one, and a single Message type draws no
// pick.
func TestMixWithoutKey(t *testing.T) {
	var log []string
	types := []wire.Bound[encoding]{bound("A", &log, nil, nil)}
	mustPlant(t, types, "")
	g, err := wire.NewMix(testSynth(), types, nil).Generate()
	if err != nil || g.Key != nil || g.Headers != nil || g.Type != 0 {
		t.Errorf("Generate = %+v, %v; want Type 0, a null Key and no Headers", g, err)
	}
	if want := []string{"A"}; !reflect.DeepEqual(log, want) {
		t.Errorf("draws = %v, want %v", log, want)
	}
}

// TestMixKeyErrorAborts proves a Key the key schema cannot produce stops the
// message, with the generator's error.
func TestMixKeyErrorAborts(t *testing.T) {
	var log []string
	boom := errors.New("key schema cannot be honoured")
	types := []wire.Bound[encoding]{bound("A", &log, nil, nil)}
	mustPlant(t, types, "")
	key := source{name: "key", log: &log, value: func() any { return nil }, err: boom}
	if _, err := wire.NewMix(testSynth(), types, key).Generate(); !errors.Is(err, boom) {
		t.Errorf("Generate = %v, want the Key generator's error", err)
	}
}

// TestMixHeaders proves each record's Headers come from its own Message
// type's headers schema, none for a type that declares none, and that a
// headers schema the generator cannot honour stops the run naming the type.
func TestMixHeaders(t *testing.T) {
	var log []string
	acme := map[string]any{"type": "object", "required": []any{"tenant"}, "properties": map[string]any{"tenant": map[string]any{"type": "string", "enum": []any{"acme"}}}}
	types := []wire.Bound[encoding]{bound("A", &log, acme, nil), bound("B", &log, nil, nil)}
	mustPlant(t, types, "")
	m := wire.NewMix(testSynth(), types, nil)
	for n := 0; n < 20; n++ {
		g, err := m.Generate()
		if err != nil {
			t.Fatal(err)
		}
		want := []pipeline.Header{{Name: "tenant", Value: []byte("acme")}}
		if g.Type == 1 {
			want = nil
		}
		if !reflect.DeepEqual(g.Headers, want) {
			t.Fatalf("%s: headers %v, want %v", types[g.Type].Name, g.Headers, want)
		}
	}

	bad := []wire.Bound[encoding]{bound("A", &log, map[string]any{"type": "object", "required": []any{"n"}, "properties": map[string]any{"n": map[string]any{"type": "wat"}}}, nil)}
	mustPlant(t, bad, "")
	if _, err := wire.NewMix(testSynth(), bad, nil).Generate(); err == nil || !strings.Contains(err.Error(), "headers of A") {
		t.Errorf("err = %v, want a generation error naming the headers of A", err)
	}
}

// TestMixHeadersFollowDeclaredOrder proves Headers come in the order the
// headers schema declares its properties, an object header's JSON text in its
// own declared order (#96).
func TestMixHeadersFollowDeclaredOrder(t *testing.T) {
	var log []string
	schema := map[string]any{"type": "object", "required": []any{"zone", "origin", "attempt"}, "properties": map[string]any{
		"zone":    map[string]any{"const": "eu"},
		"origin":  map[string]any{"type": "object", "required": []any{"z", "a"}, "properties": map[string]any{"z": map[string]any{"const": 1}, "a": map[string]any{"const": 2}}, ordered.Keyword: []any{"z", "a"}},
		"attempt": map[string]any{"const": 3},
	}, ordered.Keyword: []any{"zone", "origin", "attempt"}}
	types := []wire.Bound[encoding]{bound("A", &log, schema, nil)}
	mustPlant(t, types, "")
	g, err := wire.NewMix(testSynth(), types, nil).Generate()
	if err != nil {
		t.Fatal(err)
	}
	want := []pipeline.Header{{Name: "zone", Value: []byte("eu")}, {Name: "origin", Value: []byte(`{"z":1,"a":2}`)}, {Name: "attempt", Value: []byte("3")}}
	if !reflect.DeepEqual(g.Headers, want) {
		t.Errorf("headers = %s, want %s", g.Headers, want)
	}
}

// TestMixPlantsHeaders proves a Topic parameter with a header location has its
// value planted into the Headers of every record of every Message type,
// before they are encoded (#93).
func TestMixPlantsHeaders(t *testing.T) {
	var log []string
	types := []wire.Bound[encoding]{bound("A", &log, tenantHeaders(), nil), bound("B", &log, tenantHeaders(), nil)}
	mustPlant(t, types, "", headerParam("tenant", "acme", "tenant"))
	m := wire.NewMix(testSynth(), types, nil)
	for n := 0; n < 20; n++ {
		g, err := m.Generate()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(g.Headers[0], pipeline.Header{Name: "tenant", Value: []byte("acme")}) {
			t.Fatalf("type %d: headers %v, want tenant=acme planted", g.Type, g.Headers)
		}
	}
}

// TestPlantRefusesHeaderLocations proves every header Planting the run cannot
// honour stops it before any record exists, as an Error of -topic.
func TestPlantRefusesHeaderLocations(t *testing.T) {
	var log []string
	both := func() []wire.Bound[encoding] {
		return []wire.Bound[encoding]{bound("A", &log, tenantHeaders(), nil), bound("B", &log, tenantHeaders(), nil)}
	}
	cases := map[string]struct {
		types  []wire.Bound[encoding]
		params []asyncapi.TopicParameter
		want   string
	}{
		"a Message type without headers": {[]wire.Bound[encoding]{bound("A", &log, tenantHeaders(), nil), bound("B", &log, nil, nil)},
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant")},
			"Topic parameter tenant: location $message.header#/tenant: in Message type B: B declares no headers"},
		"no Message type declares headers": {[]wire.Bound[encoding]{bound("A", &log, nil, nil)},
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant")},
			"Topic parameter tenant: location $message.header#/tenant: A declares no headers"},
		"not guaranteed": {both(),
			[]asyncapi.TopicParameter{headerParam("trace", "t1", "trace")},
			"Topic parameter trace: location $message.header#/trace: in Message type A:"},
		"value the header refuses": {both(),
			[]asyncapi.TopicParameter{headerParam("tenant", "ACME", "tenant")},
			"Topic parameter tenant: value ACME does not conform to the header at $message.header#/tenant: in Message type A:"},
		"two plantings in one header": {both(),
			[]asyncapi.TopicParameter{headerParam("tenant", "acme", "tenant"), headerParam("org", "acme", "tenant")},
			"Topic parameters tenant and org plant into the same field"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := wire.Plant(c.types, "", c.params)
			var we *wire.Error
			if !errors.As(err, &we) || we.Flag != "topic" || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want a topic error mentioning %q", err, c.want)
			}
		})
	}
}

// TestSharedKeyBinding proves the Message types of a Kafka topic must declare
// the same Key binding, or none, compared in whichever format they are read
// in, and a disagreement names them grouped by binding (#34 decision 3).
func TestSharedKeyBinding(t *testing.T) {
	uuid := func() asyncapi.JSONSchema { return asyncapi.JSONSchema{"type": "string", "format": "uuid"} }

	if k, err := wire.SharedKeyBinding([]asyncapi.MessageType{{Name: "A", Key: uuid()}, {Name: "B", Key: uuid()}}); err != nil || !reflect.DeepEqual(k, asyncapi.Schema(uuid())) {
		t.Errorf("equal JSON bindings = %v, %v; want the binding", k, err)
	}
	if k, err := wire.SharedKeyBinding([]asyncapi.MessageType{{Name: "A"}, {Name: "B"}}); err != nil || k != nil {
		t.Errorf("no bindings = %v, %v; want none", k, err)
	}
	if k, err := wire.SharedKeyBinding([]asyncapi.MessageType{{Name: "A", Key: asyncapi.Avsc(`"string"`)}, {Name: "B", Key: asyncapi.Avsc(`"string"`)}}); err != nil || !reflect.DeepEqual(k, asyncapi.Schema(asyncapi.Avsc(`"string"`))) {
		t.Errorf("equal avsc bindings = %s, %v; want the binding", k, err)
	}

	for name, c := range map[string]struct {
		err  func() error
		want string
	}{
		"JSON": {func() error {
			_, err := wire.SharedKeyBinding([]asyncapi.MessageType{{Name: "A", Key: uuid()}, {Name: "B"}, {Name: "C", Key: uuid()}})
			return err
		}, "the Message types of the Kafka topic declare different Key bindings (A, C vs B (none)); a Key identifies one Entity across them, so they must declare the same one, or none"},
		"avsc": {func() error {
			_, err := wire.SharedKeyBinding([]asyncapi.MessageType{{Name: "A", Key: asyncapi.Avsc(`"string"`)}, {Name: "B", Key: asyncapi.Avsc(`"long"`)}})
			return err
		}, "declare different Key bindings (A vs B)"},
	} {
		t.Run(name, func(t *testing.T) {
			err := c.err()
			var we *wire.Error
			if !errors.As(err, &we) || we.Flag != "topic" || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want a topic error mentioning %q", err, c.want)
			}
		})
	}
}

// tenantHeaders is a headers schema whose tenant header is required, of
// lower-case letters, and whose trace header is optional.
func tenantHeaders() map[string]any {
	return map[string]any{"type": "object", "required": []any{"tenant"}, "properties": map[string]any{
		"tenant": map[string]any{"type": "string", "pattern": "^[a-z]+$"},
		"trace":  map[string]any{"type": "string"},
	}}
}

func headerParam(name, value, pointer string) asyncapi.TopicParameter {
	return asyncapi.TopicParameter{Name: name, Value: value, Location: "$message.header#/" + pointer, Pointer: []string{pointer}, InHeaders: true}
}
