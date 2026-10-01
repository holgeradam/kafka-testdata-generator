package producer_test

import (
	"errors"
	"testing"
	"time"

	"github.com/holgeradam/kafka-testdata-generator/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestParseAcks locks the case-insensitive CLI spellings for both ack levels.
func TestParseAcks(t *testing.T) {
	cases := []struct {
		in   string
		want producer.Acks
		err  bool
	}{
		{in: "1", want: producer.AcksLeader},
		{in: "all", want: producer.AcksAll},
		{in: "ALL", want: producer.AcksAll},
		{in: "All", want: producer.AcksAll},
		{in: "aLl", want: producer.AcksAll},
		{in: "aLL", want: producer.AcksAll},
		{in: "garbage", err: true},
	}

	for _, tt := range cases {
		got, err := producer.ParseAcks(tt.in)
		if tt.err {
			if err == nil {
				t.Errorf("ParseAcks(%q): expected error, got %v", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseAcks(%q): unexpected error %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseAcks(%q): expected %v, got %v", tt.in, tt.want, got)
		}
	}
}

// TestAckConfigInvariant locks the core invariant of issue #16: idempotency is
// derived from the ack level, never an independent knob. acks=all must enable
// the idempotent path (disableIdempotency=false); the default acks=1 must keep
// it disabled.
func TestAckConfigInvariant(t *testing.T) {
	cases := []struct {
		name            string
		acks            producer.Acks
		wantAcks        kgo.Acks
		wantDisableIdem bool
	}{
		{
			name:            "acks-all-enables-idempotency",
			acks:            producer.AcksAll,
			wantAcks:        kgo.AllISRAcks(),
			wantDisableIdem: false,
		},
		{
			name:            "acks-leader-disables-idempotency",
			acks:            producer.AcksLeader,
			wantAcks:        kgo.LeaderAck(),
			wantDisableIdem: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			gotAcks, gotDisable := producer.AckConfig(producer.Options{Acks: tt.acks})
			if gotAcks != tt.wantAcks {
				t.Errorf("expected acks %v, got %v", tt.wantAcks, gotAcks)
			}
			if gotDisable != tt.wantDisableIdem {
				t.Errorf("expected disableIdempotency %v, got %v", tt.wantDisableIdem, gotDisable)
			}
		})
	}
}

// TestDefaultOptionsMatchToday ensures the defaults preserve the pre-#16
// hardcoded behavior: acks=1, linger 10ms, max buffered records 1000.
func TestDefaultOptionsMatchToday(t *testing.T) {
	o := producer.DefaultOptions()
	if o.Acks != producer.AcksLeader {
		t.Errorf("expected default acks AcksLeader, got %v", o.Acks)
	}
	if o.Linger != 10*time.Millisecond {
		t.Errorf("expected default linger 10ms, got %v", o.Linger)
	}
	if o.MaxBufferedRecords != 1000 {
		t.Errorf("expected default max buffered records 1000, got %d", o.MaxBufferedRecords)
	}
}

// TestNewBuildsWithoutDialing proves buildOpts is a valid option set: kgo.NewClient
// configures eagerly without connecting, so a non-routable broker still yields a
// usable Producer (proving the generated opts are wire-compatible).
func TestNewBuildsWithoutDialing(t *testing.T) {
	p, err := producer.New("localhost:1", producer.DefaultOptions())
	if err != nil {
		t.Fatalf("New with non-dialing broker should succeed: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil Producer")
	}
	p.Close()
}

// TestNewRoutesOptionsThroughBuildOpts uses the injected client constructor to
// confirm New forwards Options into the kgo option set it builds.
func TestNewRoutesOptionsThroughBuildOpts(t *testing.T) {
	var gotOpts []kgo.Opt
	defer producer.SetNewClient(func(opts ...kgo.Opt) (*kgo.Client, error) {
		gotOpts = opts
		return nil, nil
	})()

	p, err := producer.New("localhost:1", producer.Options{Acks: producer.AcksLeader, Linger: time.Second, MaxBufferedRecords: 7})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil Producer from injected constructor")
	}
	if len(gotOpts) == 0 {
		t.Error("expected New to pass buildOpts options to the constructor")
	}
	// New prepends the seed-broker opt, so the constructor sees buildOpts + 1.
	want := len(producer.BuildOpts(producer.Options{Acks: producer.AcksLeader, Linger: time.Second, MaxBufferedRecords: 7})) + 1
	if len(gotOpts) != want {
		t.Errorf("constructor received %d opts, want %d (buildOpts + seed broker)", len(gotOpts), want)
	}

	// The stored Options must reflect what was passed in.
	if p.Opts().Acks != producer.AcksLeader || p.Opts().Linger != time.Second || p.Opts().MaxBufferedRecords != 7 {
		t.Errorf("Producer did not store the provided Options: %+v", p.Opts())
	}
}

// TestNewRejectsInjectedConstructorError makes New propagate a construction
// failure from the constructor.
func TestNewRejectsInjectedConstructorError(t *testing.T) {
	defer producer.SetNewClient(func(_ ...kgo.Opt) (*kgo.Client, error) {
		return nil, errors.New("boom")
	})()

	_, err := producer.New("localhost:1", producer.DefaultOptions())
	if err == nil {
		t.Fatal("expected New to propagate constructor error")
	}
}
