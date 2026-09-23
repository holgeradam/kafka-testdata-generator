package keyplan

import (
	"fmt"
	"math/rand"
	"testing"
)

// counter is a Key Generator producing a fresh, numbered Key per call, so a
// test can tell a new Entity from a reused one.
type counter struct{ n int }

func (c *counter) Value() (any, error) {
	c.n++
	return fmt.Sprintf("k%d", c.n), nil
}

// picker is a seeded Picker, standing in for the run's Synthesizer.
type picker struct{ rng *rand.Rand }

func newPicker(seed int64) *picker { return &picker{rng: rand.New(rand.NewSource(seed))} }

func (p *picker) Pick(n int) int { return p.rng.Intn(n) }

// countingPicker records how many draws were made.
type countingPicker struct{ draws int }

func (p *countingPicker) Pick(int) int { p.draws++; return 0 }

func keys(t *testing.T, g Generator, n int) []string {
	t.Helper()
	out := make([]string, n)
	for i := range out {
		k, err := g.Value()
		if err != nil {
			t.Fatalf("Value: %v", err)
		}
		out[i] = k.(string)
	}
	return out
}

// TestReuseOfOneIsTheGenerator proves -records-per-key 1 is today's plan
// exactly: the Generator itself, and not a single draw from the stream.
func TestReuseOfOneIsTheGenerator(t *testing.T) {
	g := &counter{}
	p := &countingPicker{}
	if got := Reuse(g, 1, p); got != Generator(g) {
		t.Errorf("Reuse(g, 1) = %v, want g itself", got)
	}
	if p.draws != 0 {
		t.Errorf("draws = %d, want none", p.draws)
	}
}

// TestReuseAveragesRecordsPerKey proves an Entity carries N records on average:
// each record starts a new Entity with probability 1/N.
func TestReuseAveragesRecordsPerKey(t *testing.T) {
	const records, perKey = 20000, 4
	distinct := map[string]bool{}
	for _, k := range keys(t, Reuse(&counter{}, perKey, newPicker(1)), records) {
		distinct[k] = true
	}
	avg := float64(records) / float64(len(distinct))
	if avg < 3.7 || avg > 4.3 {
		t.Errorf("%d records over %d Keys = %.2f per Key, want about %d", records, len(distinct), avg, perKey)
	}
}

// TestReuseIsDeterministic proves the same stream gives the same Key sequence.
func TestReuseIsDeterministic(t *testing.T) {
	a := keys(t, Reuse(&counter{}, 3, newPicker(9)), 500)
	b := keys(t, Reuse(&counter{}, 3, newPicker(9)), 500)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("record %d: %s vs %s, want the same sequence", i, a[i], b[i])
		}
	}
}

// TestReuseKeepsRecentEntities proves memory stays bounded for endless runs:
// a reused Key always belongs to one of the most recent maxEntities Entities.
func TestReuseKeepsRecentEntities(t *testing.T) {
	created := map[string]int{} // Key -> how many Entities existed when it was made
	entities := 0
	for i, k := range keys(t, Reuse(&counter{}, 2, newPicker(5)), 30000) {
		at, seen := created[k]
		if !seen {
			created[k] = entities
			entities++
			continue
		}
		if entities-at > maxEntities {
			t.Fatalf("record %d reuses %s, created %d Entities ago; want at most %d", i, k, entities-at, maxEntities)
		}
	}
	if entities <= maxEntities {
		t.Fatalf("only %d Entities created; the test must exceed the bound of %d", entities, maxEntities)
	}
}
