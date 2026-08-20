package generator

import (
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

type Generator struct {
	rng *rand.Rand
	now time.Time
}

func New(rng *rand.Rand) *Generator {
	return NewWithBaseTime(rng, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
}

func NewWithBaseTime(rng *rand.Rand, baseTime time.Time) *Generator {
	return &Generator{rng: rng, now: baseTime.UTC()}
}

func (g *Generator) Value(schema map[string]any, fieldName string) (any, error) {
	if ref, ok := schema["$ref"].(string); ok {
		return nil, fmt.Errorf("unresolved reference %q", ref)
	}

	if example, ok := pickExample(schema); ok && g.rng.Intn(100) < 20 {
		return example, nil
	}

	if enum, ok := schema["enum"].([]any); ok && len(enum) > 0 {
		return enum[g.rng.Intn(len(enum))], nil
	}
	if value, ok := schema["const"]; ok {
		return value, nil
	}

	if merged, ok := mergeComposition(schema, "allOf"); ok {
		return g.Value(merged, fieldName)
	}
	if choice, ok := pickComposition(schema, "oneOf", g.rng); ok {
		return g.Value(choice, fieldName)
	}
	if choice, ok := pickComposition(schema, "anyOf", g.rng); ok {
		return g.Value(choice, fieldName)
	}

	switch schemaType(schema) {
	case "object":
		return g.object(schema)
	case "array":
		return g.array(schema, fieldName)
	case "integer":
		return g.integer(schema, fieldName), nil
	case "number":
		return g.number(schema, fieldName), nil
	case "boolean":
		return g.rng.Intn(2) == 0, nil
	case "string":
		return g.string(schema, fieldName), nil
	default:
		if _, ok := schema["properties"]; ok {
			return g.object(schema)
		}
		return g.string(schema, fieldName), nil
	}
}

func (g *Generator) object(schema map[string]any) (map[string]any, error) {
	properties, _ := schema["properties"].(map[string]any)
	required := requiredSet(schema)
	result := make(map[string]any, len(properties))

	for _, name := range sortedKeys(properties) {
		rawProperty := properties[name]
		property, ok := rawProperty.(map[string]any)
		if !ok {
			continue
		}

		_, isRequired := required[name]
		if !isRequired && g.rng.Intn(100) < 15 {
			continue
		}

		value, err := g.Value(property, name)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", name, err)
		}
		result[name] = value
	}

	return result, nil
}

func (g *Generator) array(schema map[string]any, fieldName string) ([]any, error) {
	minItems := intFrom(schema, "minItems", 1)
	maxItems := intFrom(schema, "maxItems", max(minItems, 5))
	if maxItems < minItems {
		maxItems = minItems
	}
	length := minItems
	if maxItems > minItems {
		length += g.rng.Intn(maxItems - minItems + 1)
	}

	itemSchema, _ := schema["items"].(map[string]any)
	if itemSchema == nil {
		itemSchema = map[string]any{"type": "string"}
	}

	result := make([]any, 0, length)
	for i := 0; i < length; i++ {
		value, err := g.Value(itemSchema, singular(fieldName))
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (g *Generator) integer(schema map[string]any, fieldName string) int64 {
	minimum, maximum := numberBounds(schema, 0, 9999)

	if _, hasMinimum := schema["minimum"]; !hasMinimum {
		if _, hasMaximum := schema["maximum"]; !hasMaximum {
			switch classify(fieldName) {
			case "age":
				minimum, maximum = 18, 85
			case "quantity":
				minimum, maximum = 1, 20
			case "price":
				minimum, maximum = 5, 500
			}
		}
	}

	if maximum < minimum {
		maximum = minimum
	}
	return int64(minimum) + int64(g.rng.Intn(int(maximum-minimum+1)))
}

func (g *Generator) number(schema map[string]any, fieldName string) float64 {
	minimum, maximum := numberBounds(schema, 0, 1000)

	if _, hasMinimum := schema["minimum"]; !hasMinimum {
		_, hasMaximum := schema["maximum"]
		if !hasMaximum && classify(fieldName) == "price" {
			minimum, maximum = 5, 500
		}
	}

	if maximum < minimum {
		maximum = minimum
	}

	value := minimum + g.rng.Float64()*(maximum-minimum)
	return math.Round(value*100) / 100
}

func (g *Generator) string(schema map[string]any, fieldName string) string {
	if pattern, ok := schema["pattern"].(string); ok {
		if generated, ok := simplePattern(pattern, g.rng); ok {
			return generated
		}
	}

	format, _ := schema["format"].(string)
	switch format {
	case "date-time":
		return g.now.Add(-time.Duration(g.rng.Intn(60*24*90)) * time.Minute).Format(time.RFC3339)
	case "date":
		return g.now.AddDate(0, 0, -g.rng.Intn(365)).Format(time.DateOnly)
	case "email":
		return strings.ToLower(fmt.Sprintf("%s.%s@example.com", g.firstName(), g.lastName()))
	case "uuid":
		return g.uuid()
	case "uri", "url":
		return (&url.URL{Scheme: "https", Host: "example.com", Path: "/" + g.slug()}).String()
	}

	switch classify(fieldName) {
	case "id":
		return g.uuid()
	case "first_name":
		return g.firstName()
	case "last_name":
		return g.lastName()
	case "name":
		return g.firstName() + " " + g.lastName()
	case "email":
		return strings.ToLower(fmt.Sprintf("%s.%s@example.com", g.firstName(), g.lastName()))
	case "phone":
		return fmt.Sprintf("+1-555-%03d-%04d", g.rng.Intn(900)+100, g.rng.Intn(10000))
	case "city":
		return pick(g.rng, cities)
	case "country":
		return pick(g.rng, countries)
	case "street":
		return fmt.Sprintf("%d %s %s", g.rng.Intn(8999)+100, pick(g.rng, surnames), pick(g.rng, streetTypes))
	case "status":
		return pick(g.rng, statuses)
	case "currency":
		return pick(g.rng, []string{"USD", "EUR", "GBP", "CHF"})
	case "description":
		return pick(g.rng, descriptions)
	default:
		return fmt.Sprintf("%s-%d", kebab(fieldName, "value"), g.rng.Intn(9000)+1000)
	}
}

func (g *Generator) firstName() string {
	return pick(g.rng, firstNames)
}

func (g *Generator) lastName() string {
	return pick(g.rng, surnames)
}

func (g *Generator) slug() string {
	return pick(g.rng, []string{"orders", "customers", "shipments", "invoices"}) + "/" + fmt.Sprint(g.rng.Intn(9000)+1000)
}

func (g *Generator) uuid() string {
	bytes := make([]byte, 16)
	for i := range bytes {
		bytes[i] = byte(g.rng.Intn(256))
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:])
}

func schemaType(schema map[string]any) string {
	value, _ := schema["type"].(string)
	if value != "" {
		return value
	}
	if values, ok := schema["type"].([]any); ok {
		for _, value := range values {
			if value != "null" {
				return fmt.Sprint(value)
			}
		}
	}
	return ""
}

func requiredSet(schema map[string]any) map[string]struct{} {
	result := map[string]struct{}{}
	values, _ := schema["required"].([]any)
	for _, value := range values {
		result[fmt.Sprint(value)] = struct{}{}
	}
	return result
}

func pickExample(schema map[string]any) (any, bool) {
	if value, ok := schema["example"]; ok {
		return value, true
	}
	examples, ok := schema["examples"].([]any)
	if !ok || len(examples) == 0 {
		return nil, false
	}
	return examples[0], true
}

func numberBounds(schema map[string]any, defaultMinimum, defaultMaximum float64) (float64, float64) {
	minimum := floatFrom(schema, "minimum", defaultMinimum)
	maximum := floatFrom(schema, "maximum", defaultMaximum)
	if exclusiveMinimum, ok := schema["exclusiveMinimum"].(float64); ok && minimum <= exclusiveMinimum {
		minimum = exclusiveMinimum + 1
	}
	if exclusiveMaximum, ok := schema["exclusiveMaximum"].(float64); ok && maximum >= exclusiveMaximum {
		maximum = exclusiveMaximum - 1
	}
	return minimum, maximum
}

func intFrom(schema map[string]any, key string, fallback int) int {
	value, ok := schema[key].(float64)
	if !ok {
		return fallback
	}
	return int(value)
}

func floatFrom(schema map[string]any, key string, fallback float64) float64 {
	value, ok := schema[key].(float64)
	if !ok {
		return fallback
	}
	return value
}

func mergeComposition(schema map[string]any, key string) (map[string]any, bool) {
	parts, ok := schema[key].([]any)
	if !ok || len(parts) == 0 {
		return nil, false
	}

	merged := map[string]any{}
	properties := map[string]any{}
	required := []any{}

	for _, part := range parts {
		object, ok := part.(map[string]any)
		if !ok {
			continue
		}
		for key, value := range object {
			switch key {
			case "properties":
				propertyMap, ok := value.(map[string]any)
				if !ok {
					continue
				}
				for propertyName, propertySchema := range propertyMap {
					properties[propertyName] = propertySchema
				}
			case "required":
				requiredValues, ok := value.([]any)
				if !ok {
					continue
				}
				required = append(required, requiredValues...)
			default:
				merged[key] = value
			}
		}
	}

	if len(properties) > 0 {
		merged["type"] = "object"
		merged["properties"] = properties
	}
	if len(required) > 0 {
		merged["required"] = required
	}

	return merged, true
}

func pickComposition(schema map[string]any, key string, rng *rand.Rand) (map[string]any, bool) {
	values, ok := schema[key].([]any)
	if !ok || len(values) == 0 {
		return nil, false
	}

	selected, ok := values[rng.Intn(len(values))].(map[string]any)
	return selected, ok
}

func classify(fieldName string) string {
	field := strings.ToLower(fieldName)
	field = strings.ReplaceAll(field, "-", "_")

	switch {
	case field == "id" || strings.HasSuffix(field, "_id") || strings.Contains(field, "uuid"):
		return "id"
	case strings.Contains(field, "firstname") || strings.Contains(field, "first_name"):
		return "first_name"
	case strings.Contains(field, "lastname") || strings.Contains(field, "last_name") || strings.Contains(field, "surname"):
		return "last_name"
	case strings.Contains(field, "email"):
		return "email"
	case strings.Contains(field, "phone") || strings.Contains(field, "mobile"):
		return "phone"
	case strings.Contains(field, "city"):
		return "city"
	case strings.Contains(field, "country"):
		return "country"
	case strings.Contains(field, "street") || strings.Contains(field, "address"):
		return "street"
	case strings.Contains(field, "status") || strings.Contains(field, "state"):
		return "status"
	case strings.Contains(field, "currency"):
		return "currency"
	case strings.Contains(field, "description") || strings.Contains(field, "message") || strings.Contains(field, "note"):
		return "description"
	case strings.Contains(field, "age"):
		return "age"
	case strings.Contains(field, "quantity") || strings.Contains(field, "count"):
		return "quantity"
	case strings.Contains(field, "price") || strings.Contains(field, "amount") || strings.Contains(field, "total"):
		return "price"
	case strings.Contains(field, "name"):
		return "name"
	default:
		return ""
	}
}

func simplePattern(pattern string, rng *rand.Rand) (string, bool) {
	re := regexp.MustCompile(`^\^\[A-Z\]\{(\d+)\}-\\d\{(\d+)\}\$$`)
	matches := re.FindStringSubmatch(pattern)
	if len(matches) != 3 {
		return "", false
	}
	letters := atoi(matches[1], 3)
	digits := atoi(matches[2], 4)
	var b strings.Builder
	for i := 0; i < letters; i++ {
		b.WriteByte(byte('A' + rng.Intn(26)))
	}
	b.WriteByte('-')
	for i := 0; i < digits; i++ {
		b.WriteByte(byte('0' + rng.Intn(10)))
	}
	return b.String(), true
}

func singular(value string) string {
	return strings.TrimSuffix(value, "s")
}

func kebab(value, fallback string) string {
	if value == "" {
		return fallback
	}
	value = strings.ReplaceAll(value, "_", "-")
	value = strings.ToLower(value)
	return value
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func pick[T any](rng *rand.Rand, values []T) T {
	return values[rng.Intn(len(values))]
}

func atoi(value string, fallback int) int {
	var result int
	if _, err := fmt.Sscanf(value, "%d", &result); err != nil {
		return fallback
	}
	return result
}

var firstNames = []string{"Avery", "Jordan", "Maya", "Noah", "Sofia", "Leo", "Nora", "Elias", "Mila", "Theo"}
var surnames = []string{"Miller", "Schneider", "Patel", "Garcia", "Nguyen", "Johnson", "Kowalski", "Rossi", "Smith", "Bauer"}
var cities = []string{"Berlin", "Hamburg", "Munich", "Paris", "Amsterdam", "Zurich", "London", "New York", "Austin", "Seattle"}
var countries = []string{"Germany", "France", "Netherlands", "Switzerland", "United Kingdom", "United States"}
var statuses = []string{"pending", "confirmed", "processing", "completed", "cancelled"}
var descriptions = []string{
	"Customer requested priority handling.",
	"Generated from the standard fulfillment flow.",
	"Address verified during checkout.",
	"Payment authorized successfully.",
}
var streetTypes = []string{"Street", "Avenue", "Road", "Lane", "Boulevard"}
