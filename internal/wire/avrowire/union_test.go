package avrowire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	codec "github.com/confluentinc/confluent-avro-go/v2"
)

// member builds a union member from an avsc literal.
func member(name, avsc string) unionMember {
	return unionMember{name: name, avsc: []byte(avsc)}
}

// jsonOf decodes a schema string so tests compare structure, not text.
func jsonOf(t *testing.T, s string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatalf("%s is not JSON: %v", s, err)
	}
	return v
}

// TestPlanUnionIndependentRecords proves two records with nothing shared
// register each under its full name, and <topic>-value as the union of
// their names, referencing both (#84 decision 4).
func TestPlanUnionIndependentRecords(t *testing.T) {
	plan, err := planUnion([]unionMember{
		member("OrderCreated", `{"type":"record","name":"OrderCreated","namespace":"com.acme","fields":[{"name":"id","type":"string"}]}`),
		member("OrderPaid", `{"type":"record","name":"OrderPaid","namespace":"com.acme","fields":[{"name":"amount","type":"double"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"com.acme.OrderCreated", "com.acme.OrderPaid"}; !reflect.DeepEqual(plan.Branches, want) {
		t.Errorf("branches = %v, want %v", plan.Branches, want)
	}
	var subjects []string
	for _, s := range plan.Subjects {
		subjects = append(subjects, s.Name)
		if len(s.References) != 0 {
			t.Errorf("%s references %v, want none", s.Name, s.References)
		}
	}
	if want := []string{"com.acme.OrderCreated", "com.acme.OrderPaid"}; !reflect.DeepEqual(subjects, want) {
		t.Errorf("subjects = %v, want %v", subjects, want)
	}
	if got, want := jsonOf(t, plan.Subjects[0].Schema), jsonOf(t, `{"type":"record","name":"OrderCreated","namespace":"com.acme","fields":[{"name":"id","type":"string"}]}`); !reflect.DeepEqual(got, want) {
		t.Errorf("OrderCreated registers %v, want %v", got, want)
	}
	if got, want := jsonOf(t, plan.Value), jsonOf(t, `["com.acme.OrderCreated","com.acme.OrderPaid"]`); !reflect.DeepEqual(got, want) {
		t.Errorf("<topic>-value = %v, want %v", got, want)
	}
	if _, err := codec.ParseBytesWithCache(plan.Union, "", &codec.SchemaCache{}); err != nil {
		t.Errorf("the inline union %s does not parse: %v", plan.Union, err)
	}
}

// TestPlanUnionSharedNamedType proves a named type both Message types define
// identically registers once, first, under its full name, and the records
// that use it reference it by name (#84 decision 5).
func TestPlanUnionSharedNamedType(t *testing.T) {
	const address = `{"type":"record","name":"Address","fields":[{"name":"city","type":"string"}]}`
	plan, err := planUnion([]unionMember{
		member("OrderCreated", `{"type":"record","name":"OrderCreated","namespace":"com.acme","fields":[{"name":"billing","type":`+address+`},{"name":"shipping","type":["null","Address"]}]}`),
		member("OrderPaid", `{"type":"record","name":"OrderPaid","namespace":"com.acme","fields":[{"name":"billing","type":`+address+`}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]unionSubject{}
	var order []string
	for _, s := range plan.Subjects {
		byName[s.Name] = s
		order = append(order, s.Name)
	}
	if want := []string{"com.acme.Address", "com.acme.OrderCreated", "com.acme.OrderPaid"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("subjects = %v, want %v: a referenced subject registers first", order, want)
	}
	if got, want := jsonOf(t, byName["com.acme.Address"].Schema), jsonOf(t, `{"type":"record","name":"Address","namespace":"com.acme","fields":[{"name":"city","type":"string"}]}`); !reflect.DeepEqual(got, want) {
		t.Errorf("Address registers %v, want %v", got, want)
	}
	created := byName["com.acme.OrderCreated"]
	if got, want := jsonOf(t, created.Schema), jsonOf(t, `{"type":"record","name":"OrderCreated","namespace":"com.acme","fields":[{"name":"billing","type":"com.acme.Address"},{"name":"shipping","type":["null","com.acme.Address"]}]}`); !reflect.DeepEqual(got, want) {
		t.Errorf("OrderCreated registers %v, want %v", got, want)
	}
	if want := []string{"com.acme.Address"}; !reflect.DeepEqual(created.References, want) {
		t.Errorf("OrderCreated references %v, want %v", created.References, want)
	}
	union, err := codec.ParseBytesWithCache(plan.Union, "", &codec.SchemaCache{})
	if err != nil {
		t.Fatalf("the inline union %s does not parse: %v", plan.Union, err)
	}
	if n := strings.Count(string(plan.Union), `"name":"Address"`); n != 1 {
		t.Errorf("the inline union defines Address %d times, want once: %s", n, plan.Union)
	}
	_ = union
}

// TestPlanUnionNestedTypesStayInline proves a named type only one Message
// type uses is not a subject of its own: it stays inline in its record.
func TestPlanUnionNestedTypesStayInline(t *testing.T) {
	plan, err := planUnion([]unionMember{
		member("A", `{"type":"record","name":"A","fields":[{"name":"s","type":{"type":"enum","name":"Status","symbols":["NEW"]}},{"name":"t","type":"Status"}]}`),
		member("B", `{"type":"record","name":"B","fields":[{"name":"n","type":"int"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Subjects) != 2 {
		t.Fatalf("subjects = %+v, want A and B only", plan.Subjects)
	}
	if got, want := jsonOf(t, plan.Subjects[0].Schema), jsonOf(t, `{"type":"record","name":"A","fields":[{"name":"s","type":{"type":"enum","name":"Status","symbols":["NEW"]}},{"name":"t","type":"Status"}]}`); !reflect.DeepEqual(got, want) {
		t.Errorf("A registers %v, want %v", got, want)
	}
}

// TestPlanUnionRefusals proves each composition the registry cannot hold
// stops the run naming the Message types involved.
func TestPlanUnionRefusals(t *testing.T) {
	cases := map[string]struct {
		members []unionMember
		want    string
	}{
		"redefinition": {[]unionMember{
			member("OrderCreated", `{"type":"record","name":"OrderCreated","fields":[{"name":"a","type":{"type":"record","name":"Address","fields":[{"name":"city","type":"string"}]}}]}`),
			member("OrderPaid", `{"type":"record","name":"OrderPaid","fields":[{"name":"a","type":{"type":"record","name":"Address","fields":[{"name":"zip","type":"string"}]}}]}`),
		}, "named type Address is defined differently in Message types OrderCreated and OrderPaid"},
		"not a record": {[]unionMember{
			member("OrderCreated", `{"type":"record","name":"OrderCreated","fields":[]}`),
			member("Ping", `"string"`),
		}, "the payload of Ping is not an Avro record"},
		"same record twice": {[]unionMember{
			member("OrderCreated", `{"type":"record","name":"Order","fields":[]}`),
			member("OrderPaid", `{"type":"record","name":"Order","fields":[]}`),
		}, "Message types OrderCreated and OrderPaid both have the record Order as payload"},
		"shared types refer to each other": {[]unionMember{
			member("A", `{"type":"record","name":"A","fields":[{"name":"x","type":{"type":"record","name":"X","fields":[{"name":"y","type":["null",{"type":"record","name":"Y","fields":[{"name":"x","type":["null","X"]}]}]}]}}]}`),
			member("B", `{"type":"record","name":"B","fields":[{"name":"y","type":{"type":"record","name":"Y","fields":[{"name":"x","type":["null",{"type":"record","name":"X","fields":[{"name":"y","type":["null","Y"]}]}]}]}}]}`),
		}, "named types X and Y refer to each other"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := planUnion(c.members)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want it to mention %q", err, c.want)
			}
		})
	}
}
