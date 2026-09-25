package pipeline

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/producer"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestKafkaSinkProducesHeaders proves a produced record carries its Headers,
// in order and a null header as null, read back from a Kafka cluster: an
// in-process one, which speaks the Kafka protocol to the real producer (#92).
func TestKafkaSinkProducesHeaders(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	broker := cluster.ListenAddrs()[0]

	prod, err := producer.New(broker, producer.Options{})
	if err != nil {
		t.Fatal(err)
	}
	sink := NewKafkaSink("orders", prod)
	defer sink.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	headers := []Header{{Name: "tenant", Value: []byte("acme")}, {Name: "trace", Value: nil}, {Name: "attempt", Value: []byte("3")}}
	if err := sink.Send(ctx, Outgoing{Key: []byte("k"), Headers: headers, Payload: []byte(`{"id":"a"}`)}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	fetches := consumer.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("fetching: %v", errs)
	}
	records := fetches.Records()
	if len(records) != 1 {
		t.Fatalf("read %d records, want 1", len(records))
	}
	var got []Header
	for _, h := range records[0].Headers {
		got = append(got, Header{Name: h.Key, Value: h.Value})
	}
	if !reflect.DeepEqual(got, headers) {
		t.Errorf("headers read back = %v, want %v", got, headers)
	}
}
