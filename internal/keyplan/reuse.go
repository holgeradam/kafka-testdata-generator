package keyplan

// maxEntities bounds the pool of Entities whose Keys can be reused. Older
// Entities retire as new ones arrive, so an endless run (-count 0) holds
// constant memory and new Entities keep appearing, as they do on a real Kafka
// topic.
const maxEntities = 1000

// Picker draws a uniform index in [0, n). The run's Synthesizer satisfies it,
// so every reuse decision comes from the seeded stream.
type Picker interface {
	Pick(n int) int
}

// Reuse returns a Generator whose Keys identify Entities that recur across
// records (-records-per-key): each record starts a new Entity, with a fresh
// Key from g, with probability 1/perKey, and otherwise reuses the Key of one
// of the most recent Entities, picked uniformly. An Entity therefore carries
// perKey records on average. With perKey 1 it returns g itself, drawing
// nothing, so the run is exactly what it was without reuse.
func Reuse(g Generator, perKey int, p Picker) Generator {
	if perKey <= 1 {
		return g
	}
	return &pool{gen: g, perKey: perKey, picker: p}
}

// pool is the reusing Generator: a ring of the most recent Entities' Keys.
type pool struct {
	gen    Generator
	perKey int
	picker Picker
	keys   []any
	oldest int
}

func (p *pool) Value() (any, error) {
	if len(p.keys) > 0 && p.picker.Pick(p.perKey) != 0 {
		return p.keys[p.picker.Pick(len(p.keys))], nil
	}
	key, err := p.gen.Value()
	if err != nil {
		return nil, err
	}
	if len(p.keys) < maxEntities {
		p.keys = append(p.keys, key)
	} else {
		p.keys[p.oldest] = key
		p.oldest = (p.oldest + 1) % maxEntities
	}
	return key, nil
}
