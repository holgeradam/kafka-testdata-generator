// Package synth is the Synthesizer: the seeded, clock-aware source of every
// leaf value the schema walkers emit (ADR-0008). The JSON Schema walker
// (internal/generator) and the avsc walker (internal/avro) own structure -
// traversal, optionality, sizes, depth budgets and typed schema errors - and
// ask the Synthesizer for each value and random decision. It is the single
// owner of randomness in a run, so one seed and one clock govern all output.
//
// Determinism holds within one build: the same seed and now replay the same
// sequence of draws. Values for a seed may change between releases.
package synth

import (
	"math/rand"
	"time"
)

// instantWindow bounds how far before now an Instant may fall.
const instantWindow = 365 * 24 * time.Hour

// Synthesizer draws leaf values and random decisions from one seeded stream.
// It is not safe for concurrent use.
type Synthesizer struct {
	rng *rand.Rand
	now time.Time
}

// New creates a Synthesizer with an explicit seed and clock. Identical output
// requires fixing both (ADR-0006 decision 4); there is no hidden clock.
func New(seed int64, now time.Time) *Synthesizer {
	return &Synthesizer{rng: rand.New(rand.NewSource(seed)), now: now}
}

// Instant returns a moment within the 365 days before now. Walkers render it in
// their wire convention: a date, a timestamp, text or time.Time.
func (s *Synthesizer) Instant() time.Time {
	return s.now.Add(-time.Duration(s.rng.Int63n(int64(instantWindow/time.Millisecond)+1)) * time.Millisecond)
}

// Int returns a uniform integer in [min, max]. An empty range yields min.
func (s *Synthesizer) Int(min, max int64) int64 {
	if max <= min {
		return min
	}
	span := uint64(max) - uint64(min) + 1
	if span == 0 { // the full int64 range
		return int64(s.rng.Uint64())
	}
	return int64(uint64(min) + s.rng.Uint64()%span)
}

// Float returns a uniform float in [min, max). An empty range yields min.
func (s *Synthesizer) Float(min, max float64) float64 {
	if max <= min {
		return min
	}
	return min + s.rng.Float64()*(max-min)
}

// Chance reports true with the given probability in percent.
func (s *Synthesizer) Chance(percent int) bool {
	return s.rng.Intn(100) < percent
}

// Pick returns a uniform index in [0, n). n must be positive.
func (s *Synthesizer) Pick(n int) int {
	return s.rng.Intn(n)
}

// Bytes returns n random bytes.
func (s *Synthesizer) Bytes(n int) []byte {
	b := make([]byte, n)
	s.rng.Read(b)
	return b
}

func (s *Synthesizer) pick(items []string) string {
	return items[s.rng.Intn(len(items))]
}

func (s *Synthesizer) random(pool string, length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = pool[s.rng.Intn(len(pool))]
	}
	return string(b)
}
