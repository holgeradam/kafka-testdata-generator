package synth

import (
	"errors"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

func fixedNow() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
}

var (
	uuidRe  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	emailRe = regexp.MustCompile(`^[a-z]+\.[a-z]+@(example\.com|test\.org|demo\.net)$`)
	urlRe   = regexp.MustCompile(`^https://(app|api|service)\.example\.com/[a-z]+/\d+$`)
	phoneRe = regexp.MustCompile(`^\+1-\d{3}-\d{3}-\d{4}$`)
	skuRe   = regexp.MustCompile(`^[A-Z]{3}-[A-Z]{2}-\d{4}$`)
	fallRe  = regexp.MustCompile(`^[a-z0-9]{8}$`)
)

// TestDeterminism proves a fixed (seed, now) replays the identical sequence of
// draws across every method, and a different seed diverges.
func TestDeterminism(t *testing.T) {
	draw := func(s *Synthesizer) []any {
		p, err := s.Pattern(`^[A-Z]{3}-\d{4}$`)
		if err != nil {
			t.Fatalf("Pattern error: %v", err)
		}
		return []any{
			s.Text("orderId"), s.Text("city"), s.Text("width"),
			s.Semantic(Email), s.Instant(), p,
			s.Int(-5, 5), s.Float(0, 1), s.Chance(50), s.Pick(7), string(s.Bytes(4)),
		}
	}
	a := draw(New(42, fixedNow()))
	b := draw(New(42, fixedNow()))
	if !slices.Equal(a, b) {
		t.Errorf("same (seed, now) diverged:\n%v\n%v", a, b)
	}
	if c := draw(New(43, fixedNow())); slices.Equal(a, c) {
		t.Errorf("different seeds produced identical draws: %v", a)
	}
}

func TestWords(t *testing.T) {
	cases := map[string][]string{
		"customerEmailAddress": {"customer", "email", "address"},
		"customer_id":          {"customer", "id"},
		"order-id":             {"order", "id"},
		"imageURL":             {"image", "url"},
		"URLPath":              {"url", "path"},
		"address2Line":         {"address", "2", "line"},
		"ID":                   {"id"},
		"":                     nil,
	}
	for in, want := range cases {
		if got := words(in); !slices.Equal(got, want) {
			t.Errorf("words(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestTextHeuristics covers every category through Text, matching on whole
// words: names that merely contain a category as a substring fall back to
// random text.
func TestTextHeuristics(t *testing.T) {
	inPool := func(pool []string) func(string) bool {
		return func(v string) bool { return slices.Contains(pool, v) }
	}
	fullName := func(v string) bool {
		first, last, ok := strings.Cut(v, " ")
		return ok && slices.Contains(firstNames, first) && slices.Contains(surnames, last)
	}
	street := regexp.MustCompile(`^\d{1,4} (.+)$`)
	isStreet := func(v string) bool {
		m := street.FindStringSubmatch(v)
		return m != nil && slices.Contains(streets, m[1])
	}
	address := regexp.MustCompile(`^\d{1,4} (.+), (.+)$`)
	isAddress := func(v string) bool {
		m := address.FindStringSubmatch(v)
		return m != nil && slices.Contains(streets, m[1]) && slices.Contains(cities, m[2])
	}

	cases := []struct {
		category string
		fields   []string
		ok       func(string) bool
	}{
		{"uuid", []string{"id", "orderId", "customer_id", "order-id", "UUID", "orderIds"}, uuidRe.MatchString},
		{"email", []string{"email", "emailAddress", "customer_email", "emails"}, emailRe.MatchString},
		{"first name", []string{"firstName", "first_name", "firstname"}, inPool(firstNames)},
		{"last name", []string{"lastName", "last_name", "lastname", "surname"}, inPool(surnames)},
		{"full name", []string{"name", "customerName", "fullName"}, fullName},
		{"phone", []string{"phone", "phoneNumber", "telephone"}, phoneRe.MatchString},
		{"city", []string{"city", "billingCity", "cities", "cityName"}, inPool(cities)},
		{"country", []string{"country", "shippingCountry"}, inPool(countries)},
		{"street", []string{"street", "streetAddress", "streetName"}, isStreet},
		{"status", []string{"status", "orderStatus", "statuses", "statusName"}, inPool(statuses)},
		{"description", []string{"description", "itemDescription"}, inPool(descriptions)},
		{"currency", []string{"currency", "priceCurrency", "currencies"}, inPool(currencies)},
		{"url", []string{"url", "websiteUrl", "imageURL", "callbackUri"}, urlRe.MatchString},
		{"sku", []string{"sku", "productSku"}, skuRe.MatchString},
		{"zip", []string{"zip", "zipCode", "postcode", "postalCode"}, regexp.MustCompile(`^\d{5}$`).MatchString},
		{"region", []string{"state", "region", "province", "billingState"}, inPool(regions)},
		{"country code", []string{"countryCode", "country_code"}, regexp.MustCompile(`^[A-Z]{2}$`).MatchString},
		{"address line", []string{"address", "addressLine", "billing_address"}, isAddress},
		{"username", []string{"username", "userName", "login", "handle"}, regexp.MustCompile(`^[a-z]+\.[a-z]+\d{1,2}$`).MatchString},
		{"company", []string{"company", "organization", "employer", "companyName"}, inPool(companies)},
		{"job title", []string{"title", "jobTitle"}, inPool(jobTitles)},
		{"ipv4", []string{"ip", "ipAddress", "ipv4", "clientIp"}, isDocIPv4},
		{"hostname", []string{"hostname", "host", "domain", "targetHost"}, isHostname},
		{"file name", []string{"fileName", "filename", "attachmentFileName"}, regexp.MustCompile(`^[a-z]+-\d{4}\.(pdf|csv|xlsx|png|json)$`).MatchString},
		{"language", []string{"language", "locale", "contentLanguage"}, inPool(languages)},
		{"timezone", []string{"timezone", "tz", "userTimezone"}, isTimezone},
		{"iban", []string{"iban", "creditorIban"}, isIBAN},
		{"fallback", []string{"", "width", "capacity", "security", "valid", "provider", "video", "during", "husky", "notes"}, fallRe.MatchString},
	}

	s := New(42, fixedNow())
	for _, c := range cases {
		for _, f := range c.fields {
			for i := 0; i < 20; i++ {
				if v := s.Text(f); !c.ok(v) {
					t.Errorf("Text(%q) = %q, want %s", f, v, c.category)
					break
				}
			}
		}
	}
}

func TestSemantic(t *testing.T) {
	s := New(42, fixedNow())
	for i := 0; i < 50; i++ {
		if v := s.Semantic(UUID); !uuidRe.MatchString(v) {
			t.Errorf("Semantic(UUID) = %q", v)
		}
		if v := s.Semantic(Email); !emailRe.MatchString(v) {
			t.Errorf("Semantic(Email) = %q", v)
		}
		if v := s.Semantic(URL); !urlRe.MatchString(v) {
			t.Errorf("Semantic(URL) = %q", v)
		}
	}
}

// TestInstantWindow proves every Instant falls within the 365 days before now.
func TestInstantWindow(t *testing.T) {
	now := fixedNow()
	s := New(7, now)
	for i := 0; i < 2000; i++ {
		v := s.Instant()
		if v.After(now) || now.Sub(v) > 365*24*time.Hour {
			t.Fatalf("Instant() = %v, outside [now-365d, now] for now %v", v, now)
		}
	}
}

func TestNumericDraws(t *testing.T) {
	s := New(1, fixedNow())
	for i := 0; i < 2000; i++ {
		if v := s.Int(-3, 3); v < -3 || v > 3 {
			t.Fatalf("Int(-3, 3) = %d", v)
		}
		if v := s.Float(1.5, 2.5); v < 1.5 || v >= 2.5 {
			t.Fatalf("Float(1.5, 2.5) = %v", v)
		}
		if v := s.Pick(4); v < 0 || v >= 4 {
			t.Fatalf("Pick(4) = %d", v)
		}
		if s.Chance(0) {
			t.Fatal("Chance(0) returned true")
		}
		if !s.Chance(100) {
			t.Fatal("Chance(100) returned false")
		}
	}
	if v := s.Int(9, 9); v != 9 {
		t.Errorf("Int(9, 9) = %d, want 9", v)
	}
	if v := s.Int(9, 2); v != 9 {
		t.Errorf("Int(9, 2) = %d, want min 9 for an empty range", v)
	}
	if v := s.Float(4, 4); v != 4 {
		t.Errorf("Float(4, 4) = %v, want 4", v)
	}
	// The full int64 span must not overflow.
	for i := 0; i < 100; i++ {
		s.Int(-1<<63, 1<<63-1)
	}
	if n := len(s.Bytes(6)); n != 6 {
		t.Errorf("len(Bytes(6)) = %d", n)
	}
}

// TestPatternSupported drives each construct of the documented subset
// (ADR-0006 decision 2) and checks the output against the same regex.
func TestPatternSupported(t *testing.T) {
	patterns := []string{
		`^AB-C$`, `[A-Z]{4}`, `[a-z]{5}`, `[0-9]{3}`, `[A-Za-z0-9]{3}`,
		`\d{4}`, `\w{5}`, `a\s+b`, `[0-9]{2,5}`, `ab*c`, `a+`, `colou?r`,
		`(ab)+c`, `^(cat|dog)$`, `^[A-Z]{3}-[A-Z]{2}-\d{4}$`, `a\.b`,
	}
	s := New(42, fixedNow())
	for _, p := range patterns {
		re := regexp.MustCompile(`^(?:` + p + `)$`)
		for i := 0; i < 50; i++ {
			v, err := s.Pattern(p)
			if err != nil {
				t.Fatalf("Pattern(%q) error: %v", p, err)
			}
			if !re.MatchString(v) {
				t.Errorf("Pattern(%q) = %q does not match", p, v)
				break
			}
		}
	}
}

// TestPatternUnsupported proves constructs outside the subset return a
// *PatternError naming the construct, with no location.
func TestPatternUnsupported(t *testing.T) {
	cases := map[string]string{
		`a.c`:        `.`,
		`[^a-z]{2}`:  `[^a-z]`,
		`\D{2}`:      `\D`,
		`\bword\b`:   `\b`,
		`[0-9]{2,}`:  `{2,}`,
		`[0-9]{3,1}`: `{3,1}`,
	}
	s := New(42, fixedNow())
	for p, construct := range cases {
		_, err := s.Pattern(p)
		var pe *PatternError
		if !errors.As(err, &pe) {
			t.Errorf("Pattern(%q) error = %v, want *PatternError", p, err)
			continue
		}
		if pe.Pattern != p || pe.Construct != construct {
			t.Errorf("Pattern(%q) = {Pattern %q, Construct %q}, want construct %q", p, pe.Pattern, pe.Construct, construct)
		}
	}
}

// isDocIPv4 reports whether v is an IPv4 address from the documentation ranges
// reserved by RFC 5737, so generated data can never name a real host.
func isDocIPv4(v string) bool {
	ip, err := netip.ParseAddr(v)
	if err != nil || !ip.Is4() {
		return false
	}
	for _, block := range []string{"192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24"} {
		if netip.MustParsePrefix(block).Contains(ip) {
			return true
		}
	}
	return false
}

// isHostname reports whether v is a host under an RFC 2606 example domain.
func isHostname(v string) bool {
	host, domain, ok := strings.Cut(v, ".")
	return ok && host != "" && slices.Contains([]string{"example.com", "example.net", "example.org"}, domain)
}

// isTimezone reports whether v names a zone the tz database knows.
func isTimezone(v string) bool {
	_, err := time.LoadLocation(v)
	return err == nil
}

// isIBAN reports whether v is a well-formed IBAN whose ISO 13616 check digits
// validate: move the first four characters to the end, map letters to numbers
// (A=10) and require the result mod 97 to be 1.
func isIBAN(v string) bool {
	if len(v) < 15 || len(v) > 34 {
		return false
	}
	rearranged := v[4:] + v[:4]
	rem := 0
	for _, r := range rearranged {
		var n int
		switch {
		case r >= '0' && r <= '9':
			n = int(r - '0')
		case r >= 'A' && r <= 'Z':
			n = int(r-'A') + 10
		default:
			return false
		}
		if n > 9 {
			rem = (rem*100 + n) % 97
		} else {
			rem = (rem*10 + n) % 97
		}
	}
	return rem == 1
}
