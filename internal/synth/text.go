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
	// Specific categories first: a name word only means a person when no more
	// specific category claims the field (companyName, fileName, cityName).
	case has("email"):
		return s.Semantic(Email)
	case has("id", "uuid", "guid"):
		return s.Semantic(UUID)
	case has("ip", "ipv"):
		// RFC 5737 documentation ranges: generated data never names a real host.
		return fmt.Sprintf("%s.%d", s.pick(ipv4DocPrefixes), s.rng.Intn(256))
	case has("username") || has("login", "handle") || has("user") && has("name"):
		return fmt.Sprintf("%s.%s%d", strings.ToLower(s.pick(firstNames)), strings.ToLower(s.pick(surnames)), s.rng.Intn(100))
	case has("filename") || has("file") && has("name"):
		return fmt.Sprintf("%s-%04d.%s", s.pick(fileStems), s.rng.Intn(10000), s.pick(fileExtensions))
	case has("firstname") || has("first") && has("name"):
		return s.pick(firstNames)
	case has("lastname", "surname") || has("last") && has("name"):
		return s.pick(surnames)
	case has("phone", "telephone"):
		return fmt.Sprintf("+1-%03d-%03d-%04d", s.rng.Intn(900)+100, s.rng.Intn(900)+100, s.rng.Intn(10000))
	case has("city"):
		return s.pick(cities)
	case has("countrycode") || has("country") && has("code"):
		return s.pick(countryCodes)
	case has("country"):
		return s.pick(countries)
	case has("street"):
		return s.streetAddress()
	case has("zip", "postcode") || has("postal") && has("code"):
		return fmt.Sprintf("%05d", s.rng.Intn(100000))
	case has("state", "region", "province"):
		return s.pick(regions)
	case has("address", "addressline"):
		return fmt.Sprintf("%s, %s", s.streetAddress(), s.pick(cities))
	case has("status"):
		return s.pick(statuses)
	case has("description"):
		return s.pick(descriptions)
	case has("currency"):
		return s.pick(currencies)
	case has("company", "organization", "employer"):
		return s.pick(companies)
	case has("title", "jobtitle"):
		return s.pick(jobTitles)
	case has("hostname", "host", "domain"):
		return fmt.Sprintf("%s.%s", s.pick(hostLabels), s.pick(exampleDomains))
	case has("language", "locale"):
		return s.pick(languages)
	case has("timezone", "tz"):
		return s.pick(timezones)
	case has("iban"):
		return s.iban()
	case has("url", "uri"):
		return s.Semantic(URL)
	case has("sku"):
		return fmt.Sprintf("%s-%s-%04d", s.random(upper, 3), s.random(upper, 2), s.rng.Intn(10000))
	// Only now does a bare name word mean a person.
	case has("name", "fullname"):
		return s.pick(firstNames) + " " + s.pick(surnames)
	default:
		return s.random(lowerAlnum, 8)
	}
}

// streetAddress is a house number plus a street name, shared by the street and
// address rules.
func (s *Synthesizer) streetAddress() string {
	return fmt.Sprintf("%d %s", s.rng.Intn(9999)+1, s.pick(streets))
}

// iban builds an IBAN whose ISO 13616 check digits validate: the BBAN is drawn
// from the picked country's layout, then the two check digits are computed so
// that the rearranged number is 1 mod 97.
func (s *Synthesizer) iban() string {
	spec := ibanLayouts[s.rng.Intn(len(ibanLayouts))]
	var bban strings.Builder
	for _, c := range spec.bban {
		if c == 'a' {
			bban.WriteByte(upper[s.rng.Intn(len(upper))])
		} else {
			bban.WriteByte(byte('0' + s.rng.Intn(10)))
		}
	}
	check := 98 - mod97(bban.String()+spec.country+"00")
	return fmt.Sprintf("%s%02d%s", spec.country, check, bban.String())
}

// mod97 folds an IBAN's rearranged digits and letters (A=10 ... Z=35) into its
// remainder modulo 97, digit group by digit group so no big integer is needed.
func mod97(s string) int {
	rem := 0
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			rem = (rem*10 + int(r-'0')) % 97
		case r >= 'A' && r <= 'Z':
			rem = (rem*100 + int(r-'A') + 10) % 97
		}
	}
	return rem
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

var countryCodes = []string{
	"US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "MX",
}

var regions = []string{
	"California", "Texas", "New York", "Florida", "Washington",
	"Bavaria", "Catalonia", "Ontario", "Queensland", "Hokkaido",
}

var companies = []string{
	"Acme Corp", "Globex", "Initech", "Vandelay Industries",
	"Contoso", "Fabrikam", "Northwind Traders", "Umbrella Ltd",
}

var jobTitles = []string{
	"Software Engineer", "Product Manager", "Data Analyst", "Sales Director",
	"Support Specialist", "Account Executive", "QA Engineer", "Operations Lead",
}

var languages = []string{
	"en-US", "en-GB", "de-DE", "fr-FR", "es-ES", "ja-JP", "pt-BR", "nl-NL",
}

var timezones = []string{
	"UTC", "Europe/Berlin", "Europe/London", "America/New_York",
	"America/Los_Angeles", "America/Sao_Paulo", "Asia/Tokyo", "Australia/Sydney",
}

// ipv4DocPrefixes are the RFC 5737 documentation ranges.
var ipv4DocPrefixes = []string{"192.0.2", "198.51.100", "203.0.113"}

// exampleDomains are the RFC 2606 reserved example domains.
var exampleDomains = []string{"example.com", "example.net", "example.org"}

var hostLabels = []string{"api", "www", "mail", "app", "files"}

var fileStems = []string{"report", "invoice", "export", "summary", "backup"}

var fileExtensions = []string{"pdf", "csv", "xlsx", "png", "json"}

// ibanLayouts pairs a country code with its BBAN layout: 'n' a digit, 'a' an
// upper-case letter.
var ibanLayouts = []struct {
	country string
	bban    string
}{
	{"DE", "nnnnnnnnnnnnnnnnnn"},
	{"ES", "nnnnnnnnnnnnnnnnnnnn"},
	{"NL", "aaaannnnnnnnnn"},
	{"GB", "aaaannnnnnnnnnnnnn"},
}
