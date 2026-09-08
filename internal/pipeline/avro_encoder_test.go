package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	avro2 "github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
)

// Compile-time check: AvroEncoder must satisfy the Encoder interface.
var _ Encoder = (*AvroEncoder)(nil)

// fakeRegistry spins up a registry that returns a fixed schema ID and records
// the registration request body. It is the shim that keeps encoder tests off
// real registries while still exercising the real registry client.
func fakeRegistry(t *testing.T, id int) (*httptest.Server, *string) {
	t.Helper()
	var lastBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s %s", r.Method, r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		lastBody = string(b)
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		fmt.Fprintf(w, `{"id":%d}`, id)
	}))
	t.Cleanup(srv.Close)
	return srv, &lastBody
}

func TestAvroEncoderRegistersExplicitAvsc(t *testing.T) {
	avsc := `{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"string"},{"name":"qty","type":"int"}]}`
	srv, lastBody := fakeRegistry(t, 42)

	enc, err := NewAvroEncoder(context.Background(), srv.URL, "orders-value", avsc)
	if err != nil {
		t.Fatalf("NewAvroEncoder: %v", err)
	}

	// The exact avsc bytes must be what the encoder registers, not a schema
	// derived from a value type (ADR-0007 decision 3).
	var payload struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal([]byte(*lastBody), &payload); err != nil {
		t.Fatalf("registration body is not the registry JSON envelope: %v (%s)", err, *lastBody)
	}
	if payload.Schema != avsc {
		t.Errorf("registered schema != explicit avsc:\n  got  %s\n  want %s", payload.Schema, avsc)
	}

	_, payloadBytes, err := enc.Encode(nil, map[string]any{"id": "abc", "qty": int32(1)})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// The registry-assigned schema ID is what gets framed on the wire.
	wantFrame := []byte{0x00, 0x00, 0x00, 0x00, 0x2a}
	for i := range wantFrame {
		if payloadBytes[i] != wantFrame[i] {
			t.Fatalf("framing prefix = %v, want %v", payloadBytes[:5], wantFrame)
		}
	}
	if len(payloadBytes) <= 5 {
		t.Fatalf("payload too short: %d bytes", len(payloadBytes))
	}
}

func TestAvroEncoderKeyContract(t *testing.T) {
	avsc := `{"type":"record","name":"O","fields":[{"name":"id","type":"string"}]}`
	srv, _ := fakeRegistry(t, 1)
	enc, err := NewAvroEncoder(context.Background(), srv.URL, "t-value", avsc)
	if err != nil {
		t.Fatalf("NewAvroEncoder: %v", err)
	}

	ken, kpayload, err := enc.Encode("cust-1", map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if string(ken) != "cust-1" {
		t.Errorf("string key = %q, want cust-1", ken)
	}
	if len(kpayload) == 0 {
		t.Error("expected a payload")
	}

	// Under AVRO the extracted key can be a non-float64 scalar (longs come out
	// as int64): the plain-scalar contract renders it as decimal text.
	keyBytes, _, err := enc.Encode(int64(42), map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Encode int64 key: %v", err)
	}
	if string(keyBytes) != "42" {
		t.Errorf("int64 key = %q, want 42", keyBytes)
	}

	nilKey, _, err := enc.Encode(nil, map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Encode nil key: %v", err)
	}
	if nilKey != nil {
		t.Errorf("nil key must yield nil keyBytes, got %q", nilKey)
	}
}

func TestAvroEncoderRegistryUnreachableTypedError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // registry gone

	_, err := NewAvroEncoder(context.Background(), url, "t-value", `{"type":"string"}`)
	if err == nil {
		t.Fatal("expected an error for an unreachable registry")
	}
	var re *RegistryError
	if !errors.As(err, &re) {
		t.Fatalf("expected *RegistryError, got %T (%v)", err, err)
	}
	if re.Error() == "" {
		t.Error("RegistryError must carry a message")
	}
	// A transport failure is not a registry rejection: it must NOT masquerade
	// as the typed registry rejection.
	var cre registry.Error
	if errors.As(err, &cre) {
		t.Errorf("unreachable registry must not unwrap to a registry.Error, got %v", err)
	}
}

func TestAvroEncoderRegistryRejectsSchemaTypedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error_code":42201,"message":"Schema being registered is incompatible with an earlier schema"}`)
	}))
	defer srv.Close()

	_, err := NewAvroEncoder(context.Background(), srv.URL, "t-value", `{"type":"string"}`)
	if err == nil {
		t.Fatal("expected an error when the registry rejects the schema")
	}
	var re *RegistryError
	if !errors.As(err, &re) {
		t.Fatalf("expected *RegistryError, got %T (%v)", err, err)
	}
	var cre registry.Error
	if !errors.As(err, &cre) {
		t.Fatalf("expected the rejection to unwrap to a registry.Error, got %v", err)
	}
	if cre.StatusCode != http.StatusBadRequest {
		t.Errorf("rejection status = %d, want 400", cre.StatusCode)
	}
}

func TestAvroEncoderConformanceProperty(t *testing.T) {
	api := avro2.Config{}.Freeze()

	// Fixtures cover every construct the generator supports, including the
	// logical types whose wire conventions are the easy place to go wrong.
	fixtures := []struct {
		name string
		avsc string
	}{
		{"primitives", `{"type":"record","name":"O","fields":[
			{"name":"b","type":"boolean"},{"name":"i","type":"int"},{"name":"l","type":"long"},
			{"name":"f","type":"float"},{"name":"d","type":"double"},{"name":"by","type":"bytes"},
			{"name":"s","type":"string"},{"name":"n","type":"null"}]}`},
		{"logical", `{"type":"record","name":"O","fields":[
			{"name":"day","type":{"type":"int","logicalType":"date"}},
			{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
			{"name":"tms","type":{"type":"long","logicalType":"timestamp-micros"}},
			{"name":"tm","type":{"type":"int","logicalType":"time-millis"}},
			{"name":"tmu","type":{"type":"long","logicalType":"time-micros"}},
			{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
			{"name":"price","type":{"type":"fixed","name":"Price","size":8,"logicalType":"decimal","precision":18,"scale":4}}]}`},
		{"collections", `{"type":"record","name":"O","fields":[
			{"name":"items","type":{"type":"array","items":"int"}},
			{"name":"attrs","type":{"type":"map","values":"long"}},
			{"name":"state","type":{"type":"enum","name":"State","symbols":["NEW","DONE"]}},
			{"name":"code","type":{"type":"fixed","name":"Code","size":4}},
			{"name":"child","type":{"type":"record","name":"Child","fields":[{"name":"x","type":"long"}]}}]}`},
		{"unions", `{"type":"record","name":"O","namespace":"com.acme","fields":[
			{"name":"maybe","type":["null","string"]},
			{"name":"choice","type":["string","int"]},
			{"name":"optionalDate","type":["null",{"type":"int","logicalType":"date"}]},
			{"name":"optDecimal","type":["null",{"type":"bytes","logicalType":"decimal","precision":6,"scale":3}]},
			{"name":"nested","type":["null",{"type":"record","name":"R","fields":[{"name":"y","type":"int"}]}]}]}`},
		{"nested-collections", `{"type":"record","name":"O","fields":[
			{"name":"matrix","type":{"type":"array","items":{"type":"array","items":"long"}}},
			{"name":"dict","type":{"type":"map","values":{"type":"record","name":"V","fields":[{"name":"z","type":"double"}]}}},
			{"name":"tags","type":{"type":"array","items":{"type":"map","values":"boolean"}}}]}`},
		{"recursive-linked", `{"type":"record","name":"Node","fields":[
			{"name":"value","type":"long"},
			{"name":"next","type":["null","Node"]}]}`},
	}

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			model, err := avro.Parse([]byte(fx.avsc))
			if err != nil {
				t.Fatalf("model parse failed: %v", err)
			}
			cfSchema, err := avro2.Parse(fx.avsc)
			if err != nil {
				t.Fatalf("confluent parse failed: %v", err)
			}
			srv, _ := fakeRegistry(t, 9)
			enc, err := NewAvroEncoder(context.Background(), srv.URL, "t-value", fx.avsc)
			if err != nil {
				t.Fatalf("NewAvroEncoder: %v", err)
			}

			for seed := int64(0); seed < 6; seed++ {
				gen := avro.NewGenerator(seed, now)
				value, err := gen.Value(model.Root)
				if err != nil {
					t.Fatalf("seed %d: generation failed: %v", seed, err)
				}
				_, wire, err := enc.Encode(nil, value)
				if err != nil {
					t.Fatalf("seed %d: Encode failed: %v (value %#v)", seed, err, value)
				}
				if wire[0] != 0x00 || wire[1] != 0 || wire[2] != 0 || wire[3] != 0 || wire[4] != 9 {
					t.Fatalf("seed %d: wire prefix %v, want magic 0x00 + id 9", seed, wire[:5])
				}
				var got any
				if err := api.Unmarshal(cfSchema, wire[5:], &got); err != nil {
					t.Fatalf("seed %d: a Confluent consumer could not decode the payload: %v", seed, err)
				}
				if got == nil {
					t.Fatalf("seed %d: decode returned nil", seed)
				}
			}
		})
	}
}
