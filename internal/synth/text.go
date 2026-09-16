package synth

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
)

// Kind names a semantic string shape the Synthesizer can produce on request,
// independent of any field name (e.g. for a JSON Schema format).
type Kind int

const (
	UUID Kind = iota
	Email
	URL
)

// Semantic returns a string of the requested shape.
func (s *Synthesizer) Semantic(k Kind) string {
	switch k {
	case UUID:
		b := s.Bytes(16)
		b[6] = (b[6] & 0x0f) | 0x40
		b[8] = (b[8] & 0x3f) | 0x80
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
	case Email:
		first := strings.ToLower(s.pick(firstNames))
		last := strings.ToLower(s.pick(surnames))
		return fmt.Sprintf("%s.%s@%s", first, last, s.pick(emailDomains))
	case URL:
		return fmt.Sprintf("https://%s.example.com/%s/%d", s.pick(urlHosts), s.pick(urlPaths), s.rng.Intn(10000))
	default:
		panic(fmt.Sprintf("synth: unknown Kind %d", k))
	}
}

// Text returns a readable value for a field of the given name, or random text
// when no heuristic applies. The name is split into words (see words) and rules
// match whole words or their regular plurals, so orderId, customer_id and emails
// hit their rules while width or capacity do not.
func (s *Synthesizer) Text(field string) string {
	w := words(field)
	has := func(names ...string) bool {
		return slices.ContainsFunc(w, func(word string) bool {
			return slices.ContainsFunc(names, func(name string) bool { return matchesWord(word, name) })
		})
	}
	switch {
	case has("email"):
		return s.Semantic(Email)
	case has("id", "uuid", "guid"):
		return s.Semantic(UUID)
	case has("firstname") || has("first") && has("name"):
		return s.pick(firstNames)
	case has("lastname", "surname") || has("last") && has("name"):
		return s.pick(surnames)
	case has("name", "fullname"):
		return s.pick(firstNames) + " " + s.pick(surnames)
	case has("phone", "telephone"):
		return fmt.Sprintf("+1-%03d-%03d-%04d", s.rng.Intn(900)+100, s.rng.Intn(900)+100, s.rng.Intn(10000))
	case has("city"):
		return s.pick(cities)
	case has("country"):
		return s.pick(countries)
	case has("street"):
		return fmt.Sprintf("%d %s", s.rng.Intn(9999)+1, s.pick(streets))
	case has("status"):
		return s.pick(statuses)
	case has("description"):
		return s.pick(descriptions)
	case has("currency"):
		return s.pick(currencies)
	case has("url", "uri"):
		return s.Semantic(URL)
	case has("sku"):
		return fmt.Sprintf("%s-%s-%04d", s.random(upper, 3), s.random(upper, 2), s.rng.Intn(10000))
	default:
		return s.random(lowerAlnum, 8)
	}
}

// matchesWord reports whether word is name or one of its regular plurals
// (emails, statuses, cities).
func matchesWord(word, name string) bool {
	switch word {
	case name, name + "s", name + "es":
		return true
	}
	stem, ok := strings.CutSuffix(name, "y")
	return ok && word == stem+"ies"
}

// words splits a field name into lower-case words at non-alphanumeric
// separators, lower-to-upper case changes, the end of an acronym (URLPath ->
// url, path) and letter-digit changes.
func words(field string) []string {
	var out []string
	var cur []rune
	flush := func() {
		if len(cur) > 0 {
			out = append(out, strings.ToLower(string(cur)))
			cur = cur[:0]
		}
	}
	rs := []rune(field)
	for i, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if len(cur) > 0 {
			prev := cur[len(cur)-1]
			switch {
			case unicode.IsDigit(r) != unicode.IsDigit(prev):
				flush()
			case unicode.IsUpper(r) && unicode.IsLower(prev):
				flush()
			case unicode.IsUpper(r) && unicode.IsUpper(prev) && i+1 < len(rs) && unicode.IsLower(rs[i+1]):
				flush()
			}
		}
		cur = append(cur, r)
	}
	flush()
	return out
}

const (
	upper      = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	lowerAlnum = "abcdefghijklmnopqrstuvwxyz0123456789"
)

var (
	emailDomains = []string{"example.com", "test.org", "demo.net"}
	urlHosts     = []string{"app", "api", "service"}
	urlPaths     = []string{"api", "users", "products", "orders", "docs"}
)

var firstNames = []string{
	"Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Hannah",
	"Ivan", "Julia", "Kevin", "Laura", "Michael", "Nancy", "Oscar", "Patricia",
	"Quentin", "Rachel", "Steven", "Tina", "Uma", "Victor", "Wendy", "Xavier",
	"Yvonne", "Zachary",
}

var surnames = []string{
	"Anderson", "Brown", "Clark", "Davis", "Evans", "Fisher", "Garcia", "Harris",
	"Irwin", "Johnson", "King", "Lee", "Miller", "Nelson", "Ortiz", "Park",
	"Quinn", "Roberts", "Smith", "Taylor", "Upton", "Vargas", "Wilson", "Young",
}

var cities = []string{
	"New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
	"Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
	"Seattle", "Denver", "Boston", "Nashville", "Portland",
}

var countries = []string{
	"United States", "Canada", "United Kingdom", "Germany", "France",
	"Japan", "Australia", "Brazil", "India", "Mexico",
}

var streets = []string{
	"Main St", "Oak Ave", "Pine Rd", "Maple Dr", "Cedar Ln",
	"Elm St", "Walnut Ave", "Spruce Rd", "Birch Dr", "Willow Ln",
}

var statuses = []string{
	"pending", "active", "completed", "cancelled", "processing",
	"delivered", "shipped", "returned", "refunded", "on-hold",
}

var descriptions = []string{
	"Standard order", "Express delivery", "Bulk purchase", "Trial subscription",
	"Premium service", "Seasonal offer", "Corporate account", "New customer",
}

var currencies = []string{
	"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY",
}
