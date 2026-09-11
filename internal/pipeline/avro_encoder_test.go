package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	avro2 "github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
)

// Compile-time check: AvroEncoder must satisfy the Encoder interface.
var _ Encoder = (*AvroEncoder)(nil)

// registryCall records one schema-registration request: the subject the schema
// was registered under and the raw request body.
type registryCall struct {
	subject, body string
}

// fakeRegistry spins up a registry that returns the configured schema ID for
// each subject (defaulting to 1) and records every registration request. It is
// the shim that keeps encoder tests off real registries while still exercising
// the real registry client.
func fakeRegistry(t *testing.T, ids map[string]int) (*httptest.Server, *[]registryCall) {
	t.Helper()
	var calls []registryCall
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s %s", r.Method, r.URL.Path)
		}
		subject := strings.TrimSuffix(r.URL.Path, "/versions")
		subject = strings.TrimPrefix(subject, "/subjects/")
		b, _ := io.ReadAll(r.Body)
		calls = append(calls, registryCall{subject: subject, body: string(b)})
		id, ok := ids[subject]
		if !ok {
			id = 1
		}
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		fmt.Fprintf(w, `{"id":%d}`, id)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestAvroEncoderRegistersExplicitAvsc(t *testing.T) {
	avsc := `{"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"string"},{"name":"qty","type":"int"}]}`
	srv, calls := fakeRegistry(t, map[string]int{"orders-value": 42})

	enc, err := NewAvroEncoder(context.Background(), srv.URL, "orders", avsc, "")
	if err != nil {
		t.Fatalf("NewAvroEncoder: %v", err)
	}

	// The exact avsc bytes must be what the encoder registers, not a schema
	// derived from a value type (ADR-0007 decision 3).
	var payload struct {
		Schema string `json:"schema"`
	}
	if len(*calls) != 1 {
		t.Fatalf("expected 1 registration, got %d", len(*calls))
	}
	if (*calls)[0].subject != "orders-value" {
		t.Errorf("value avsc must register under <topic>-value, got %q", (*calls)[0].subject)
	}
	if err := json.Unmarshal([]byte((*calls)[0].body), &payload); err != nil {
		t.Fatalf("registration body is not the registry JSON envelope: %v (%s)", err, (*calls)[0].body)
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

// TestAvroEncoderKeyContract locks the AVRO key contract (issue #24): a key
// value is only encoded when a key avsc is registered, and then it is framed
// under the key schema's own registry ID, never silently dropped.
func TestAvroEncoderKeyContract(t *testing.T) {
	valueAvsc := `{"type":"record","name":"O","fields":[{"name":"id","type":"string"}]}`
	keyAvsc := `{"type":"string"}`
	api := avro2.Config{}.Freeze()
	srv, _ := fakeRegistry(t, map[string]int{"t-value": 1, "t-key": 2})

	// No key avsc: the run produces payload-only records (null key).
	enc, err := NewAvroEncoder(context.Background(), srv.URL, "t", valueAvsc, "")
	if err != nil {
		t.Fatalf("NewAvroEncoder: %v", err)
	}
	nilKey, _, err := enc.Encode(nil, map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Encode nil key: %v", err)
	}
	if nilKey != nil {
		t.Errorf("payload-only encoding must yield nil key bytes, got %q", nilKey)
	}

	// No key avsc but a key value would silently lose data: reject it.
	if _, _, err := enc.Encode("cust-1", map[string]any{"id": "a"}); err == nil {
		t.Fatal("expected an error when a key is given but no key schema is registered")
	}

	// With a key avsc the key is framed magic byte + key-schema ID + Avro, and
	// a standard consumer decodes it against the key avsc.
	keyAvscParsed, err := avro2.Parse(keyAvsc)
	if err != nil {
		t.Fatalf("parse key avsc: %v", err)
	}
	keyed, err := NewAvroEncoder(context.Background(), srv.URL, "t", valueAvsc, keyAvsc)
	if err != nil {
		t.Fatalf("NewAvroEncoder with key avsc: %v", err)
	}
	kbytes, _, err := keyed.Encode("cust-1", map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Encode keyed: %v", err)
	}
	wantKeyFrame := []byte{0x00, 0x00, 0x00, 0x00, 0x02}
	for i := range wantKeyFrame {
		if kbytes[i] != wantKeyFrame[i] {
			t.Fatalf("key framing prefix = %v, want %v", kbytes[:5], wantKeyFrame)
		}
	}
	var got any
	if err := api.Unmarshal(keyAvscParsed, kbytes[5:], &got); err != nil {
		t.Fatalf("a Confluent consumer could not decode the key: %v", err)
	}
	if got != "cust-1" {
		t.Errorf("decoded key = %v, want cust-1", got)
	}

	// A key schema configured but no key value: reject, never emit a null key
	// where the schema contract promises a generated key.
	if _, _, err := keyed.Encode(nil, map[string]any{"id": "a"}); err == nil {
		t.Fatal("expected an error when a key schema is configured but no key value arrives")
	}
}

func TestAvroEncoderRegistryUnreachableTypedError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // registry gone

	_, err := NewAvroEncoder(context.Background(), url, "t", `{"type":"string"}`, "")
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

	_, err := NewAvroEncoder(context.Background(), srv.URL, "t", `{"type":"string"}`, "")
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
			srv, _ := fakeRegistry(t, map[string]int{"t-value": 9})
			enc, err := NewAvroEncoder(context.Background(), srv.URL, "t", fx.avsc, "")
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

// TestAvroEncoderRegistersKeySubject proves the key avsc registers under
// <topic>-key, symmetric with the value schema, and that the run frames keys
// with the ID that registration assigned (issue #24 AC5).
func TestAvroEncoderRegistersKeySubject(t *testing.T) {
	valueAvsc := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	keyAvsc := `{"type":"record","name":"OrderKey","fields":[{"name":"id","type":"string"},{"name":"seq","type":"long"}]}`
	srv, calls := fakeRegistry(t, map[string]int{"orders-value": 7, "orders-key": 42})

	enc, err := NewAvroEncoder(context.Background(), srv.URL, "orders", valueAvsc, keyAvsc)
	if err != nil {
		t.Fatalf("NewAvroEncoder: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected 2 registrations (value + key), got %d", len(*calls))
	}
	if (*calls)[0].subject != "orders-value" {
		t.Errorf("value avsc registered under %q, want orders-value", (*calls)[0].subject)
	}
	if (*calls)[1].subject != "orders-key" {
		t.Errorf("key avsc registered under %q, want orders-key", (*calls)[1].subject)
	}
	var body struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal([]byte((*calls)[1].body), &body); err != nil {
		t.Fatalf("key registration body not a registry envelope: %v (%s)", err, (*calls)[1].body)
	}
	if body.Schema != keyAvsc {
		t.Errorf("registered key schema != explicit key avsc:\n  got  %s\n  want %s", body.Schema, keyAvsc)
	}

	kbytes, pbytes, err := enc.Encode(map[string]any{"id": "k", "seq": int64(1)}, map[string]any{"id": "v"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if wantKey := []byte{0x00, 0x00, 0x00, 0x00, 0x2a}; kbytes[0] != wantKey[0] || kbytes[4] != wantKey[4] {
		t.Fatalf("key must frame the key-schema ID 42, got prefix %v", kbytes[:5])
	}
	if wantPayload := []byte{0x00, 0x00, 0x00, 0x00, 0x07}; pbytes[0] != wantPayload[0] || pbytes[4] != wantPayload[4] {
		t.Fatalf("payload must frame the value-schema ID 7, got prefix %v", pbytes[:5])
	}
}

// TestAvroEncoderKeyConformanceProperty is the key counterpart of the value
// conformance property (issue #24 AC1): every key avsc the generator supports
// must produce keys a standard Confluent consumer decodes against that avsc.
func TestAvroEncoderKeyConformanceProperty(t *testing.T) {
	api := avro2.Config{}.Freeze()
	valueAvsc := `{"type":"record","name":"V","fields":[{"name":"id","type":"string"}]}`
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

	fixtures := []struct {
		name string
		avsc string
	}{
		{"string", `{"type":"string"}`},
		{"long", `{"type":"long"}`},
		{"record", `{"type":"record","name":"OrderKey","fields":[{"name":"id","type":"string"},{"name":"seq","type":"long"}]}`},
		{"union", `["string","long"]`},
		{"fixed", `{"type":"fixed","name":"K","size":4}`},
		{"logical", `{"type":"record","name":"K","fields":[
			{"name":"day","type":{"type":"int","logicalType":"date"}},
			{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}`},
	}

	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			model, err := avro.Parse([]byte(fx.avsc))
			if err != nil {
				t.Fatalf("model parse failed: %v", err)
			}
			cfKeySchema, err := avro2.Parse(fx.avsc)
			if err != nil {
				t.Fatalf("confluent parse failed: %v", err)
			}
			srv, _ := fakeRegistry(t, map[string]int{"t-value": 8, "t-key": 42})
			enc, err := NewAvroEncoder(context.Background(), srv.URL, "t", valueAvsc, fx.avsc)
			if err != nil {
				t.Fatalf("NewAvroEncoder: %v", err)
			}

			for seed := int64(0); seed < 6; seed++ {
				keyGen := avro.NewGenerator(seed, now)
				key, err := keyGen.Value(model.Root)
				if err != nil {
					t.Fatalf("seed %d: key generation failed: %v", seed, err)
				}
				kbytes, _, err := enc.Encode(key, map[string]any{"id": "v"})
				if err != nil {
					t.Fatalf("seed %d: Encode failed: %v (key %#v)", seed, err, key)
				}
				if kbytes[0] != 0x00 || kbytes[4] != 42 {
					t.Fatalf("seed %d: key wire prefix %v, want magic 0x00 + id 42", seed, kbytes[:5])
				}
				var got any
				if err := api.Unmarshal(cfKeySchema, kbytes[5:], &got); err != nil {
					t.Fatalf("seed %d: a Confluent consumer could not decode the key: %v", seed, err)
				}
				if got == nil {
					t.Fatalf("seed %d: decoded key is nil", seed)
				}
			}
		})
	}
}
