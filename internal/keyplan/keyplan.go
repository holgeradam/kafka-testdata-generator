// Package keyplan owns the Key of a run: the key schema produces the value,
// and -keyPath says where that value is mirrored into the Payload, so the two
// can never disagree (ADR-0009). The plan is built at the process edge, where
// its Checker validates the path against the Payload schema before any record
// is generated; from then on each record is one Apply call.
//
// Path parsing and planting are format-blind: both schema walkers emit plain
// maps and slices along a validated path, so nothing here knows JSON Schema
// from avsc. What a path may legally traverse is the Checker's business.
package keyplan

import (
	"fmt"
	"strconv"
	"strings"
)

// Step is one step of a key path: a field name, or an array index when Index is
// not negative.
type Step struct {
	// Field is the object field name; used when Index is -1.
	Field string
	// Index is the array index; -1 means this step is a field access.
	Index int
}

// String renders a step the way it is written in -keyPath.
func (s Step) String() string {
	if s.Index >= 0 {
		return "[" + strconv.Itoa(s.Index) + "]"
	}
	return s.Field
}

// PathString renders a parsed path the way it is written in -keyPath.
func PathString(path []Step) string {
	var b strings.Builder
	for i, s := range path {
		if s.Index < 0 && i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(s.String())
	}
	return b.String()
}

// PathError reports a -keyPath the plan cannot use: malformed syntax, or a path
// the Payload does not carry at generation time.
type PathError struct {
	// Path is the key path as written, or as far as it parsed.
	Path string
	// Detail explains what is wrong with it.
	Detail string
}

func (e *PathError) Error() string {
	return fmt.Sprintf("keyplan: -keyPath %q: %s", e.Path, e.Detail)
}

// Checker validates a parsed key path against the schema that governs the
// Payload: that every step is guaranteed to exist in every generated record,
// and that the type at the path can hold the Key. One adapter exists per schema
// language, each comparing within its own language.
type Checker interface {
	Check(path []Step) error
}

// Generator produces one Key value from the key schema.
type Generator interface {
	Value() (any, error)
}

// Plan generates the Key of each record and plants it into the Payload.
type Plan struct {
	gen  Generator
	path []Step
	raw  string
}

// New builds a plan from the key Generator and the key path, running the
// Checker's startup validation before the plan exists. An empty path means the
// Key is generated but not planted, and needs no Checker.
func New(g Generator, c Checker, path string) (*Plan, error) {
	if path == "" {
		return &Plan{gen: g}, nil
	}
	steps, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, &PathError{Path: path, Detail: "a key path needs a schema to validate it against"}
	}
	if err := c.Check(steps); err != nil {
		return nil, err
	}
	return &Plan{gen: g, path: steps, raw: path}, nil
}

// Apply generates the Key for one record and plants it into payload at the key
// path, returning the value both now hold. Without a path the Payload is left
// untouched.
func (p *Plan) Apply(payload any) (any, error) {
	key, err := p.gen.Value()
	if err != nil {
		return nil, err
	}
	if len(p.path) == 0 {
		return key, nil
	}
	if err := plant(payload, p.path, key); err != nil {
		return nil, err
	}
	return key, nil
}

// plant writes value at the end of path. Every step must already exist: the
// Checker rejects a path that is not guaranteed, so a miss here is a defect
// rather than an expected outcome, and it surfaces as a typed error.
func plant(payload any, path []Step, value any) error {
	current := payload
	for i, step := range path[:len(path)-1] {
		next, err := child(current, step)
		if err != nil {
			return pathErrorAt(path, i, err)
		}
		current = next
	}

	last := path[len(path)-1]
	if last.Index >= 0 {
		arr, ok := current.([]any)
		if !ok || last.Index >= len(arr) {
			return pathErrorAt(path, len(path)-1, fmt.Errorf("the Payload holds no item at this index"))
		}
		arr[last.Index] = value
		return nil
	}
	m, ok := current.(map[string]any)
	if !ok {
		return pathErrorAt(path, len(path)-1, fmt.Errorf("the Payload holds no object here"))
	}
	if _, ok := m[last.Field]; !ok {
		return pathErrorAt(path, len(path)-1, fmt.Errorf("the Payload has no field %q", last.Field))
	}
	m[last.Field] = value
	return nil
}

// child descends one step into the generated value.
func child(current any, step Step) (any, error) {
	if step.Index >= 0 {
		arr, ok := current.([]any)
		if !ok {
			return nil, fmt.Errorf("the Payload holds no array here")
		}
		if step.Index >= len(arr) {
			return nil, fmt.Errorf("the Payload holds no item at this index")
		}
		return arr[step.Index], nil
	}
	m, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the Payload holds no object here")
	}
	v, ok := m[step.Field]
	if !ok {
		return nil, fmt.Errorf("the Payload has no field %q", step.Field)
	}
	return v, nil
}

// pathErrorAt names the step that failed, so the message points at the part of
// the path that is wrong rather than the whole of it.
func pathErrorAt(path []Step, i int, err error) error {
	return &PathError{
		Path:   PathString(path),
		Detail: fmt.Sprintf("at %q: %v", PathString(path[:i+1]), err),
	}
}

// ParsePath splits a key path into steps: dotted field names with optional [n]
// array indexing, e.g. customer.id or items[0].sku.
func ParsePath(path string) ([]Step, error) {
	if path == "" {
		return nil, &PathError{Path: path, Detail: "the path is empty"}
	}
	var (
		steps []Step
		buf   []byte
	)
	flushField := func() {
		if len(buf) > 0 {
			steps = append(steps, Step{Field: string(buf), Index: -1})
			buf = buf[:0]
		}
	}
	for i := 0; i < len(path); {
		switch c := path[i]; c {
		case '.':
			flushField()
			i++
		case '[':
			flushField()
			end := i + 1
			for end < len(path) && path[end] != ']' {
				end++
			}
			if end >= len(path) {
				return nil, &PathError{Path: path, Detail: "an array index is missing its closing bracket"}
			}
			n, err := strconv.Atoi(path[i+1 : end])
			if err != nil || n < 0 {
				return nil, &PathError{Path: path, Detail: fmt.Sprintf("array index %q is not a non-negative number", path[i+1:end])}
			}
			steps = append(steps, Step{Index: n})
			i = end + 1
		default:
			buf = append(buf, c)
			i++
		}
	}
	flushField()
	if len(steps) == 0 {
		return nil, &PathError{Path: path, Detail: "the path names no field"}
	}
	return steps, nil
}

// Put plants value into payload at path, as Apply plants the Key: for a value
// planted beside the Key, such as a Topic parameter's (#83). The path must
// have been checked against the Payload schema, so a miss is a defect.
func Put(payload any, path []Step, value any) error {
	return plant(payload, path, value)
}

// Overlap reports whether one path leads to or into the other, so planting
// both would have one overwrite the other.
func Overlap(a, b []Step) bool {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
