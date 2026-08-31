package generator

import (
	"testing"
	"time"
)

func fixedNow() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
}

// dateSchema builds an object with date and date-time formatted fields plus a
// plain string field, so tests can isolate clock-driven output from
// seed-driven output.
func dateSchema() map[string]any {
	return map[string]any{
		"type":     "object",
		"required": []any{"created", "createdAt", "name"},
		"properties": map[string]any{
			"created":   map[string]any{"type": "string", "format": "date"},
			"createdAt": map[string]any{"type": "string", "format": "date-time"},
			"name":      map[string]any{"type": "string"},
		},
	}
}

// anchored asserts a date or date-time formatted value lies within the window
// [now-365d, now] implied by the current date synthesis.
func anchored(t *testing.T, v string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, v)
	if err != nil {
		parsed, err = time.Parse("2006-01-02", v)
		if err != nil {
			t.Fatalf("value %q is neither RFC3339 nor a date: %v", v, err)
		}
	}
	return parsed
}

func TestNewAnchorsDateToNow(t *testing.T) {
	now := fixedNow()
	gen := New(42, now)

	result, err := gen.Value(dateSchema(), RootField)
	if err != nil {
		t.Fatalf("Value error: %v", err)
	}
	obj := result.(map[string]any)

	for _, f := range []string{"created", "createdAt"} {
		parsed := anchored(t, obj[f].(string))
		if parsed.After(now) {
			t.Errorf("%s %q is after injected now %v", f, obj[f], now)
		}
		if now.Sub(parsed) > 366*24*time.Hour {
			t.Errorf("%s %q is more than a year before now %v", f, obj[f], now)
		}
	}
}

func TestNewSameSeedSameNowDeterministicDates(t *testing.T) {
	now := fixedNow()
	gen1 := New(7, now)
	gen2 := New(7, now)

	for i := 0; i < 5; i++ {
		r1, err := gen1.Value(dateSchema(), RootField)
		if err != nil {
			t.Fatalf("gen1.Value error: %v", err)
		}
		r2, err := gen2.Value(dateSchema(), RootField)
		if err != nil {
			t.Fatalf("gen2.Value error: %v", err)
		}
		for _, f := range []string{"created", "createdAt"} {
			if r1.(map[string]any)[f] != r2.(map[string]any)[f] {
				t.Errorf("iteration %d: %s differs for same (seed, now): %v vs %v",
					i, f, r1.(map[string]any)[f], r2.(map[string]any)[f])
			}
		}
	}
}

func TestNewDifferentNowChangesDatesOnly(t *testing.T) {
	genA := New(42, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	genB := New(42, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))

	rA, err := genA.Value(dateSchema(), RootField)
	if err != nil {
		t.Fatalf("genA.Value error: %v", err)
	}
	rB, err := genB.Value(dateSchema(), RootField)
	if err != nil {
		t.Fatalf("genB.Value error: %v", err)
	}

	objA := rA.(map[string]any)
	objB := rB.(map[string]any)

	// The non-date field is seed-driven and must match.
	if objA["name"] != objB["name"] {
		t.Errorf("non-date field differs: %v vs %v", objA["name"], objB["name"])
	}
	// The clock fields are now-driven and the two nows are far apart, so they
	// must diverge with overwhelming probability.
	for _, f := range []string{"created", "createdAt"} {
		if objA[f] == objB[f] {
			t.Errorf("%s should differ across different nows: %v", f, objA[f])
		}
	}
}
