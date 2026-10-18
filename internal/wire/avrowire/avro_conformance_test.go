package avrowire_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/avro"
	"github.com/holgeradam/kafka-testdata-generator/internal/pipeline"
	"github.com/holgeradam/kafka-testdata-generator/internal/wire/avrowire"
)

// TestAvroEncodersReportConformanceErrors is #120 decision 3: a value the
// avsc rejects is the generator breaking Conformance, a tool bug that must
// stop a run even when it continues past failed records, so both Avro
// encoders report it as a *pipeline.ConformanceError.
func TestAvroEncodersReportConformanceErrors(t *testing.T) {
	const avsc = `{"type":"record","name":"O","fields":[{"name":"id","type":"string"}]}`
	model, err := avro.Parse([]byte(avsc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	bad := pipeline.Generated{Payload: map[string]any{"id": 42}}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		fmt.Fprint(w, `{"id":1}`)
	}))
	t.Cleanup(srv.Close)
	registry, err := avrowire.NewAvroEncoder(context.Background(), srv.URL, "t", model, nil)
	if err != nil {
		t.Fatalf("NewAvroEncoder: %v", err)
	}

	for name, enc := range map[string]pipeline.Encoder{
		"display":  avrowire.NewAvroDisplayEncoder(model, nil),
		"registry": registry,
	} {
		_, _, err := enc.Encode(bad)
		var ce *pipeline.ConformanceError
		if !errors.As(err, &ce) {
			t.Errorf("%s: err = %v, want a *pipeline.ConformanceError", name, err)
		}
	}
}
