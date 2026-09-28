package wire

import (
	"errors"
	"strings"
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/asyncapi"
	"github.com/holgeradam/kafka-testdata-generator/internal/keyplan"
)

// TestPlantsApplyNamesTheTopicParameter proves a planting that misses its
// field names the Topic parameter and its location, the -topic flag's, and
// never -keyPath, which the run may not even have (#107).
func TestPlantsApplyNamesTheTopicParameter(t *testing.T) {
	tp := asyncapi.TopicParameter{Name: "region", Value: "eu", Location: "$message.payload#/meta/region", Pointer: []string{"meta", "region"}}
	plants, err := Plants{}.Add(tp, []keyplan.Step{{Field: "meta", Index: -1}, {Field: "region", Index: -1}}, "")
	if err != nil {
		t.Fatal(err)
	}
	err = plants.Apply(map[string]any{"meta": map[string]any{"source": "web"}})
	var we *Error
	if !errors.As(err, &we) || we.Flag != "topic" {
		t.Fatalf("Apply = %v, want a *wire.Error of the -topic flag", err)
	}
	for _, want := range []string{"Topic parameter region", "$message.payload#/meta/region", `no field "region"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Apply = %q, want it to mention %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "keyPath") || strings.Contains(err.Error(), "Payload") {
		t.Errorf("Apply = %q, want neither -keyPath nor the Payload named: a location may be in the Headers", err)
	}
}
