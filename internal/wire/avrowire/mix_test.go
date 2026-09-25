package avrowire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	codec "github.com/confluentinc/confluent-avro-go/v2"
	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/synth"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire"
)

const address = `{"type":"record","name":"Address","fields":[{"name":"city","type":"string"}]}`

// createdAvsc and paidAvsc are two Avro Message types of one Kafka topic,
// sharing the named type com.acme.Address.
const createdAvsc = `{"type":"record","name":"OrderCreated","namespace":"com.acme","fields":[{"name":"id","type":"string"},{"name":"region","type":"string"},{"name":"billing","type":` + address + `}]}`
const paidAvsc = `{"type":"record","name":"OrderPaid","namespace":"com.acme","fields":[{"name":"id","type":"string"},{"name":"region","type":"string"},{"name":"amount","type":"double"},{"name":"billing","type":` + address + `}]}`

// mixOptions are the options of a run whose spec declares both Message types
// in Avro, each with the given Key binding ("" for none).
func mixOptions(seed int64, keys ...string) wire.Options {
	types := []asyncapi.MessageType{{Name: "OrderCreated", Avsc: []byte(createdAvsc)}, {Name: "OrderPaid", Avsc: []byte(paidAvsc)}}
	for i, k := range keys {
		if k != "" {
			types[i].KeyAvsc = []byte(k)
		}
	}
	return wire.Options{Topic: "orders", DryRun: true, Synth: synth.New(seed, testNow()), MessageTypes: types}
}

// TestBuildMixesAvroTypes proves several Avro Message types are mixed per
// record from the seeded stream, as JSON mode mixes (#74): each appears,
// Generated names the one each Payload is of, and a seed repeats the
// sequence.
func TestBuildMixesAvroTypes(t *testing.T) {
	sequence := func(seed int64) []int {
		parts, err := Format{}.Build(mixOptions(seed))
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		var types []int
		for i := 0; i < 200; i++ {
			g, err := parts.Values.Generate()
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			_, paid := g.Payload.(map[string]any)["amount"]
			if paid != (g.Type == 1) {
				t.Fatalf("record %d: Type %d, payload %v: the Type must name the Message type generated", i, g.Type, g.Payload)
			}
			types = append(types, g.Type)
		}
		return types
	}
	first := sequence(5)
	counts := map[int]int{}
	for _, ty := range first {
		counts[ty]++
	}
	if counts[0] < 50 || counts[1] < 50 {
		t.Errorf("200 records split %v, want both Message types well represented", counts)
	}
	if !reflect.DeepEqual(first, sequence(5)) {
		t.Error("the same seed must repeat the sequence of Message types")
	}
}

// TestBuildChecksEveryAvroType proves -keyPath and a Topic parameter's
// location must hold in every Avro Message type, naming the one that fails,
// and that the Message types must agree on their Key binding.
func TestBuildChecksEveryAvroType(t *testing.T) {
	opts := mixOptions(1, `"string"`, `"string"`)
	opts.KeyPath = "id"
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if parts.KeyGen == nil || parts.Checker == nil {
		t.Fatal("an agreed Avro Key binding and -keyPath must give a Key and a checker")
	}
	if err := parts.Checker.Check(mustPath(t, "amount")); err == nil || !strings.Contains(err.Error(), "in Message type OrderCreated") {
		t.Errorf("err = %v, want the path refused in Message type OrderCreated", err)
	}

	opts = mixOptions(1)
	opts.TopicParameters = []asyncapi.TopicParameter{{Name: "tier", Value: "gold", Location: "$message.payload#/amount", Pointer: []string{"amount"}}}
	if _, err := (Format{}).Build(opts); err == nil || !strings.Contains(err.Error(), "in Message type OrderCreated") {
		t.Errorf("err = %v, want the location refused in Message type OrderCreated", err)
	}

	opts = mixOptions(1)
	opts.TopicParameters = []asyncapi.TopicParameter{{Name: "region", Value: "eu", Location: "$message.payload#/region", Pointer: []string{"region"}}}
	parts, err = Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for i := 0; i < 20; i++ {
		g, _ := parts.Values.Generate()
		if got := g.Payload.(map[string]any)["region"]; got != "eu" {
			t.Fatalf("record %d of type %d: region %v, want the Topic parameter planted", i, g.Type, got)
		}
	}

	_, err = Format{}.Build(mixOptions(1, `"string"`, `"long"`))
	var we *wire.Error
	if !errors.As(err, &we) || we.Flag != "topic" || !strings.Contains(err.Error(), "different Key bindings (OrderCreated vs OrderPaid)") {
		t.Errorf("err = %v, want differing Key bindings refused", err)
	}
	_, err = Format{}.Build(mixOptions(1, `"string"`))
	if err == nil || !strings.Contains(err.Error(), "OrderPaid (none)") {
		t.Errorf("err = %v, want a missing Key binding refused", err)
	}
}

// TestBuildRefusesAvroRedefinition proves a named type the Message types
// define differently stops the run before any record exists.
func TestBuildRefusesAvroRedefinition(t *testing.T) {
	opts := mixOptions(1)
	opts.MessageTypes[1].Avsc = []byte(strings.Replace(paidAvsc, `"city"`, `"town"`, 1))
	_, err := Format{}.Build(opts)
	var we *wire.Error
	if !errors.As(err, &we) || we.Flag != "topic" || !strings.Contains(err.Error(), "named type com.acme.Address is defined differently in Message types OrderCreated and OrderPaid") {
		t.Errorf("err = %v, want the redefinition refused", err)
	}
}

// TestDryRunRendersBranchAlone proves a Dry run shows each record in the
// Avro JSON encoding of its own Message type, without the union's wrapper.
func TestDryRunRendersBranchAlone(t *testing.T) {
	parts, err := Format{}.Build(mixOptions(1))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	enc, err := parts.Encoder(context.Background())
	if err != nil {
		t.Fatalf("Encoder: %v", err)
	}
	paid := map[string]any{"id": "o-1", "region": "eu", "amount": 9.5, "billing": map[string]any{"city": "Oslo"}}
	_, out, err := enc.Encode(nil, pipeline.Generated{Type: 1, Payload: paid})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if want := `{"id":"o-1","region":"eu","amount":9.5,"billing":{"city":"Oslo"}}`; string(out) != want {
		t.Errorf("Dry run = %s, want %s", out, want)
	}
}

// unionRegistry is a registry that stores what is registered, answers
// version lookups, and gives each subject its own ID. A subject's history
// holds an older version first, so a lookup must find the right one.
type unionRegistry struct {
	t        *testing.T
	order    []string
	schemas  map[string]string
	refs     map[string][]map[string]any
	ids      map[string]int
	versions map[string]int
}

func newUnionRegistry(t *testing.T) (*httptest.Server, *unionRegistry) {
	r := &unionRegistry{t: t, schemas: map[string]string{}, refs: map[string][]map[string]any{}, ids: map[string]int{}, versions: map[string]int{}}
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, r
}

func (r *unionRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/subjects/"), "/versions")
	subject := parts[0]
	switch {
	case req.Method == http.MethodPost:
		body, _ := io.ReadAll(req.Body)
		var payload struct {
			Schema     string
			References []map[string]any
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			r.t.Errorf("registration body %s: %v", body, err)
		}
		r.order = append(r.order, subject)
		r.schemas[subject] = payload.Schema
		r.refs[subject] = payload.References
		r.ids[subject] = 100 + len(r.order)
		r.versions[subject] = 3
		fmt.Fprintf(w, `{"id":%d}`, r.ids[subject])
	case parts[1] == "":
		fmt.Fprint(w, `[1,3]`)
	default:
		v, _ := strconv.Atoi(strings.TrimPrefix(parts[1], "/"))
		id := 1
		if v == r.versions[subject] {
			id = r.ids[subject]
		}
		body, _ := json.Marshal(map[string]any{"schema": r.schemas[subject], "id": id, "version": v})
		w.Write(body)
	}
}

// TestAvroEncoderRegistersUnion proves producing registers the shared named
// type, then each record under its full name referencing it, then the union
// under <topic>-value referencing the records, each reference at the version
// the registry holds it at; every record is framed with the union's ID and
// decodes against the union to its own Message type.
func TestAvroEncoderRegistersUnion(t *testing.T) {
	srv, reg := newUnionRegistry(t)
	opts := mixOptions(1)
	opts.DryRun = false
	opts.RegistryURL = srv.URL
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	enc, err := parts.Encoder(context.Background())
	if err != nil {
		t.Fatalf("Encoder: %v", err)
	}

	if want := []string{"com.acme.Address", "com.acme.OrderCreated", "com.acme.OrderPaid", "orders-value"}; !reflect.DeepEqual(reg.order, want) {
		t.Fatalf("registered %v, want %v", reg.order, want)
	}
	ref := func(name string) map[string]any {
		return map[string]any{"name": name, "subject": name, "version": float64(3)}
	}
	if want := []map[string]any{ref("com.acme.Address")}; !reflect.DeepEqual(reg.refs["com.acme.OrderPaid"], want) {
		t.Errorf("OrderPaid references %v, want %v", reg.refs["com.acme.OrderPaid"], want)
	}
	if want := []map[string]any{ref("com.acme.OrderCreated"), ref("com.acme.OrderPaid")}; !reflect.DeepEqual(reg.refs["orders-value"], want) {
		t.Errorf("orders-value references %v, want %v", reg.refs["orders-value"], want)
	}
	if got := reg.schemas["orders-value"]; got != `["com.acme.OrderCreated","com.acme.OrderPaid"]` {
		t.Errorf("orders-value = %s, want the union of the records' names", got)
	}

	union, err := codec.ParseBytesWithCache([]byte(`[`+createdAvsc+`,`+strings.Replace(paidAvsc, address, `"com.acme.Address"`, 1)+`]`), "", &codec.SchemaCache{})
	if err != nil {
		t.Fatal(err)
	}
	paid := map[string]any{"id": "o-1", "region": "eu", "amount": 9.5, "billing": map[string]any{"city": "Oslo"}}
	_, framed, err := enc.Encode(nil, pipeline.Generated{Type: 1, Payload: paid})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if id := int(framed[1])<<24 | int(framed[2])<<16 | int(framed[3])<<8 | int(framed[4]); id != reg.ids["orders-value"] {
		t.Errorf("framed with schema ID %d, want the union's %d", id, reg.ids["orders-value"])
	}
	var decoded any
	if err := (codec.Config{}).Freeze().Unmarshal(union, framed[5:], &decoded); err != nil {
		t.Fatalf("decoding against the union: %v", err)
	}
	if want := map[string]any{"com.acme.OrderPaid": paid}; !reflect.DeepEqual(decoded, want) {
		t.Errorf("decoded %v, want %v", decoded, want)
	}
}

func mustPath(t *testing.T, path string) []keyplan.Step {
	t.Helper()
	steps, err := keyplan.ParsePath(path)
	if err != nil {
		t.Fatal(err)
	}
	return steps
}

// TestBuildGeneratesHeadersUnderAvro proves an Avro Message type's Headers
// come from its JSON Schema headers, as in JSON mode (#85 decision 1), and
// that a spec's headers are ignored, out loud, under -avro-schema, whose
// records are of no Message type in the spec.
func TestBuildGeneratesHeadersUnderAvro(t *testing.T) {
	opts := mixOptions(1)
	opts.MessageTypes[1].Headers = map[string]any{"type": "object", "required": []any{"tenant"}, "properties": map[string]any{"tenant": map[string]any{"const": "acme"}}}
	parts, err := Format{}.Build(opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for i := 0; i < 30; i++ {
		g, _ := parts.Values.Generate()
		if got := len(g.Headers); got != g.Type {
			t.Fatalf("record %d of type %d: %d headers, want OrderPaid's alone to have one", i, g.Type, got)
		}
	}

	file := options(t)
	file.MessageTypes[0].Headers = opts.MessageTypes[1].Headers
	parts, err = Format{}.Build(file)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if g, _ := parts.Values.Generate(); g.Headers != nil {
		t.Errorf("headers = %v, want none under -avro-schema", g.Headers)
	}
	if !strings.Contains(strings.Join(parts.Warnings, "\n"), "headers are ignored under -avro-schema") {
		t.Errorf("warnings = %v, want the ignored headers reported", parts.Warnings)
	}
}
