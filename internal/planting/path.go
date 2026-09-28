package planting

import (
	"fmt"
	"strconv"
	"strings"
)

// Step is one step of a Planting's path: a field name, or an array index when
// Index is not negative. A step read from a JSON Pointer is a Token: its Field
// is the pointer token, which a Walk reads as an index where the schema has
// an array and as a field name elsewhere.
type Step struct {
	// Field is the object field name, or the token when Token is set; used
	// when Index is -1.
	Field string
	// Index is the array index; -1 means this step is a field access.
	Index int
	// Token marks a step a Walk still has to resolve.
	Token bool
}

// String renders a step the way it is written in -keyPath.
func (s Step) String() string {
	if s.Index >= 0 {
		return "[" + strconv.Itoa(s.Index) + "]"
	}
	return s.Field
}

// PathString renders a path the way it is written in -keyPath.
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

// PathError reports a -keyPath the run cannot use: malformed syntax, a path
// generation does not guarantee, a field that cannot hold the Key, or a
// Payload that does not carry the path at generation time.
type PathError struct {
	// Path is the key path as written, or as far as it parsed.
	Path string
	// Detail explains what is wrong with it.
	Detail string
}

func (e *PathError) Error() string {
	return fmt.Sprintf("-keyPath %q: %s", e.Path, e.Detail)
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

// tokens turns JSON Pointer tokens into steps for a Walk to resolve.
func tokens(pointer []string) []Step {
	steps := make([]Step, len(pointer))
	for i, t := range pointer {
		steps[i] = Step{Field: t, Index: -1, Token: true}
	}
	return steps
}

// pointerString renders pointer tokens as a JSON Pointer, escaping ~ and / as
// RFC 6901 does.
func pointerString(pointer []string) string {
	escaped := make([]string, len(pointer))
	for i, token := range pointer {
		escaped[i] = strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
	}
	return "/" + strings.Join(escaped, "/")
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

// plant writes value at the end of path. Every step must already exist: New
// refuses a path that is not guaranteed, so a miss here is a defect rather
// than an expected outcome. It reports the step that missed and what the
// value there lacks, for the caller to phrase.
func plant(into any, path []Step, value any) (int, error) {
	current := into
	for i, step := range path[:len(path)-1] {
		next, err := child(current, step)
		if err != nil {
			return i, err
		}
		current = next
	}

	i, last := len(path)-1, path[len(path)-1]
	if last.Index >= 0 {
		arr, ok := current.([]any)
		if !ok || last.Index >= len(arr) {
			return i, fmt.Errorf("holds no item at this index")
		}
		arr[last.Index] = value
		return 0, nil
	}
	m, ok := current.(map[string]any)
	if !ok {
		return i, fmt.Errorf("holds no object here")
	}
	if _, ok := m[last.Field]; !ok {
		return i, fmt.Errorf("has no field %q", last.Field)
	}
	m[last.Field] = value
	return 0, nil
}

// child descends one step into the generated value.
func child(current any, step Step) (any, error) {
	if step.Index >= 0 {
		arr, ok := current.([]any)
		if !ok {
			return nil, fmt.Errorf("holds no array here")
		}
		if step.Index >= len(arr) {
			return nil, fmt.Errorf("holds no item at this index")
		}
		return arr[step.Index], nil
	}
	m, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("holds no object here")
	}
	v, ok := m[step.Field]
	if !ok {
		return nil, fmt.Errorf("has no field %q", step.Field)
	}
	return v, nil
}
