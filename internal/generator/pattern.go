package generator

import (
	"strconv"
	"strings"
)

// synthesizePattern builds a value that conforms to the documented regex
// subset (ADR-0006 Decision 2): literals, character classes [A-Z] [a-z] [0-9],
// escapes \d \w \s, quantifiers {n} {n,m} * + ?, groups, alternation, and
// anchors (ignored for synthesis). Anything outside the subset yields a
// UnsupportedPatternError naming the construct, never a silently-nonconforming
// string.
func (g *Generator) synthesizePattern(pattern, path string) (string, error) {
	p := &patternParser{pattern: pattern, path: path}
	node, err := p.parseAlternation()
	if err != nil {
		return "", err
	}
	if p.i < len(pattern) {
		return "", p.errConstruct(pattern[p.i:])
	}
	var sb strings.Builder
	if err := g.genNode(node, &sb); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// reNode is one parsed piece of the documented regex subset.
type reNode interface{ isReNode() }

type reEmpty struct{}        // anchor ^ $ , ignored for synthesis
type reLit struct{ ch rune } // literal character
type reClass struct {
	chars []rune // pool of characters, one drawn independently per occurrence
}
type reSeq struct {
	items []reNode
}
type reAlt struct {
	branches []reSeq
}
type reQuant struct {
	node reNode
	min  int
	max  int
}

func (reEmpty) isReNode() {}
func (reLit) isReNode()   {}
func (reClass) isReNode() {}
func (reSeq) isReNode()   {}
func (reAlt) isReNode()   {}
func (reQuant) isReNode() {}

// maxRepeat caps how many times an unbounded quantifier ( * or + ) repeats a
// node, keeping generated strings short while still conforming.
const maxRepeat = 6

var (
	digitsPool = []rune("0123456789")
	wordPool   = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
	spacePool  = []rune(" \t\n\r\f")
)

// patternParser is a recursive-descent parser for the documented regex subset.
type patternParser struct {
	pattern string
	path    string
	i       int
}

// errConstruct reports that a construct outside the documented subset was hit.
func (p *patternParser) errConstruct(construct string) error {
	return &UnsupportedPatternError{Pattern: p.pattern, Construct: construct, Path: p.path}
}

// alternation ::= concat ( '|' concat )*
func (p *patternParser) parseAlternation() (reNode, error) {
	var branches []reSeq
	first, err := p.parseConcat()
	if err != nil {
		return nil, err
	}
	branches = append(branches, reSeq{items: first})
	for p.i < len(p.pattern) && p.pattern[p.i] == '|' {
		p.i++
		seq, err := p.parseConcat()
		if err != nil {
			return nil, err
		}
		branches = append(branches, reSeq{items: seq})
	}
	if len(branches) == 1 {
		return reSeq{items: branches[0].items}, nil
	}
	return reAlt{branches: branches}, nil
}

// concat ::= piece*   (stops at '|' or ')')
func (p *patternParser) parseConcat() ([]reNode, error) {
	var items []reNode
	for p.i < len(p.pattern) && p.pattern[p.i] != '|' && p.pattern[p.i] != ')' {
		atom, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		if p.i < len(p.pattern) {
			switch p.pattern[p.i] {
			case '{':
				q, err := p.parseQuant()
				if err != nil {
					return nil, err
				}
				atom = reQuant{node: atom, min: q[0], max: q[1]}
			case '*':
				p.i++
				atom = reQuant{node: atom, min: 0, max: maxRepeat}
			case '+':
				p.i++
				atom = reQuant{node: atom, min: 1, max: maxRepeat}
			case '?':
				p.i++
				atom = reQuant{node: atom, min: 0, max: 1}
			}
		}
		items = append(items, atom)
	}
	return items, nil
}

// atom ::= anchor | literal | escape | class | group | (unsupported -> error)
func (p *patternParser) parseAtom() (reNode, error) {
	if p.i >= len(p.pattern) {
		return nil, p.errConstruct("")
	}
	ch := p.pattern[p.i]
	switch {
	case ch == '^' || ch == '$':
		p.i++
		return reEmpty{}, nil
	case ch == '.':
		return nil, p.errConstruct(".")
	case ch == '\\':
		return p.parseEscape()
	case ch == '[':
		return p.parseClass()
	case ch == '(':
		p.i++
		inner, err := p.parseAlternation()
		if err != nil {
			return nil, err
		}
		if p.i >= len(p.pattern) || p.pattern[p.i] != ')' {
			return nil, p.errConstruct("(")
		}
		p.i++
		return inner, nil
	default:
		p.i++
		return reLit{ch: rune(ch)}, nil
	}
}

// escape ::= '\' ( d | w | s | escaped literal ) | (unsupported -> error)
func (p *patternParser) parseEscape() (reNode, error) {
	p.i++ // consume '\'
	if p.i >= len(p.pattern) {
		return nil, p.errConstruct(`\`)
	}
	c := p.pattern[p.i]
	switch c {
	case 'd':
		p.i++
		return reClass{chars: digitsPool}, nil
	case 'w':
		p.i++
		return reClass{chars: wordPool}, nil
	case 's':
		p.i++
		return reClass{chars: spacePool}, nil
	default:
		if strings.ContainsRune(`.^$[]()|*+?{}\\-`, rune(c)) {
			p.i++
			return reLit{ch: rune(c)}, nil
		}
		return nil, p.errConstruct(`\` + string(c))
	}
}

// class ::= '[' range* ']'   (a leading '^' is outside the documented subset)
func (p *patternParser) parseClass() (reNode, error) {
	start := p.i
	p.i++ // consume '['
	if p.i < len(p.pattern) && p.pattern[p.i] == '^' {
		for p.i < len(p.pattern) && p.pattern[p.i] != ']' {
			p.i++
		}
		if p.i < len(p.pattern) {
			p.i++
		}
		return nil, p.errConstruct(p.pattern[start:p.i])
	}
	var pool []rune
	for p.i < len(p.pattern) && p.pattern[p.i] != ']' {
		lo := p.pattern[p.i]
		p.i++
		if p.i+1 < len(p.pattern) && p.pattern[p.i] == '-' && p.pattern[p.i+1] != ']' {
			hi := p.pattern[p.i+1]
			p.i += 2
			if hi < lo {
				return nil, p.errConstruct(p.pattern[start:p.i])
			}
			for c := lo; c <= hi; c++ {
				pool = append(pool, rune(c))
			}
		} else {
			pool = append(pool, rune(lo))
		}
	}
	if p.i >= len(p.pattern) || p.pattern[p.i] != ']' {
		return nil, p.errConstruct(p.pattern[start:p.i])
	}
	p.i++ // consume ']'
	if len(pool) == 0 {
		return nil, p.errConstruct(p.pattern[start:p.i])
	}
	return reClass{chars: pool}, nil
}

// quant ::= '{' digits '}' | '{' digits ',' digits '}' | (outside subset -> error)
func (p *patternParser) parseQuant() ([2]int, error) {
	start := p.i
	p.i++ // consume '{'
	i := p.i
	for p.i < len(p.pattern) && p.pattern[p.i] >= '0' && p.pattern[p.i] <= '9' {
		p.i++
	}
	nStr := p.pattern[i:p.i]
	if nStr == "" {
		p.skipToCloseBrace()
		return [2]int{}, p.errConstruct(p.pattern[start:p.i])
	}
	n, _ := strconv.Atoi(nStr)
	if p.i < len(p.pattern) && p.pattern[p.i] == ',' {
		p.i++
		i = p.i
		for p.i < len(p.pattern) && p.pattern[p.i] >= '0' && p.pattern[p.i] <= '9' {
			p.i++
		}
		mStr := p.pattern[i:p.i]
		if p.i < len(p.pattern) && p.pattern[p.i] == '}' && mStr != "" {
			m, _ := strconv.Atoi(mStr)
			if m < n {
				p.i++ // consume '}'
				return [2]int{}, p.errConstruct(p.pattern[start:p.i])
			}
			p.i++
			return [2]int{n, m}, nil
		}
		p.skipToCloseBrace()
		return [2]int{}, p.errConstruct(p.pattern[start:p.i])
	}
	if p.i < len(p.pattern) && p.pattern[p.i] == '}' {
		p.i++
		return [2]int{n, n}, nil
	}
	p.skipToCloseBrace()
	return [2]int{}, p.errConstruct(p.pattern[start:p.i])
}

// skipToCloseBrace advances the parser to just past the next '}' (or the end)
// so an error Construct captures the whole offending {...} token.
func (p *patternParser) skipToCloseBrace() {
	for p.i < len(p.pattern) && p.pattern[p.i] != '}' {
		p.i++
	}
	if p.i < len(p.pattern) {
		p.i++
	}
}

// genNode writes a synthesized occurrence of node into sb.
func (g *Generator) genNode(node reNode, sb *strings.Builder) error {
	switch n := node.(type) {
	case reEmpty:
	case reLit:
		sb.WriteRune(n.ch)
	case reClass:
		sb.WriteRune(n.chars[g.rng.Intn(len(n.chars))])
	case reSeq:
		for _, it := range n.items {
			if err := g.genNode(it, sb); err != nil {
				return err
			}
		}
	case reAlt:
		return g.genNode(n.branches[g.rng.Intn(len(n.branches))], sb)
	case reQuant:
		count := n.min
		if n.max > n.min {
			count += g.rng.Intn(n.max - n.min + 1)
		}
		for i := 0; i < count; i++ {
			if err := g.genNode(n.node, sb); err != nil {
				return err
			}
		}
	}
	return nil
}
