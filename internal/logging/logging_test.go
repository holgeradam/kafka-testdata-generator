package logging_test

import (
	"testing"

	"github.com/holgeradam/kafka-testdata-generator/internal/logging"
)

func TestParseLevel(t *testing.T) {
	for name, want := range map[string]logging.Level{
		"debug": logging.Debug,
		"info":  logging.Info,
		"warn":  logging.Warn,
		"error": logging.Error,
	} {
		got, err := logging.ParseLevel(name)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v, nil", name, got, err, want)
		}
		if got.String() != name {
			t.Errorf("Level.String() = %q, want %q", got.String(), name)
		}
	}
	for _, bad := range []string{"", "trace", "WARN", "warning", "fatal"} {
		if _, err := logging.ParseLevel(bad); err == nil {
			t.Errorf("ParseLevel(%q) = nil error, want the accepted values named", bad)
		}
	}
}

func TestRecorderKeepsEntriesInOrder(t *testing.T) {
	r := &logging.Recorder{}
	var l logging.Logger = r
	l.Info("one", "k", 1)
	l.Warn("two")
	l.Error("three", "error", "boom")

	got := r.Entries()
	if len(got) != 3 || got[0].Level != logging.Info || got[0].Msg != "one" || got[0].Attrs[0] != "k" ||
		got[1].Level != logging.Warn || got[2].Level != logging.Error || got[2].Msg != "three" {
		t.Errorf("entries = %+v", got)
	}
}

func TestNopDiscards(t *testing.T) {
	logging.Nop().Error("nothing happens", "k", "v")
}
