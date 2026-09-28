package asyncapi

import (
	"fmt"
	"mime"
	"regexp"
	"strings"
)

// asyncAPIVersion matches the version of an AsyncAPI Schema format, per major
// version of the spec that declares it.
var asyncAPIVersion = map[int]*regexp.Regexp{
	2: regexp.MustCompile(`^2\.\d+\.\d+$`),
	3: regexp.MustCompile(`^3\.\d+\.\d+$`),
}

// PayloadFormat is the schema language a Message schema is written in.
type PayloadFormat int

const (
	// JSONSchemaFormat is JSON Schema, including the AsyncAPI Schema.
	JSONSchemaFormat PayloadFormat = iota
	// AvroFormat is Avro, an avsc (#84).
	AvroFormat
)

func (f PayloadFormat) String() string {
	if f == AvroFormat {
		return "Avro"
	}
	return "JSON Schema"
}

// Schema is a schema the spec declares for a message - its payload or its
// Key binding - in one of the formats the tool reads. It is sealed: a
// JSONSchema or an Avsc, so a caller tells the formats apart by the case it
// holds, never by which of several fields is set (#111).
type Schema interface {
	// Format is the schema language the schema is written in.
	Format() PayloadFormat
	sealed()
}

// JSONSchema is a self-contained JSON Schema: every non-cyclic $ref expanded
// in place, and every cycle kept as a $ref into its own $defs (ADR-0005,
// #73), with the order each object declares its properties in recorded
// (#96).
type JSONSchema map[string]any

// Avsc is an Avro schema as JSON, with its $refs expanded (#84).
type Avsc []byte

func (JSONSchema) Format() PayloadFormat { return JSONSchemaFormat }
func (Avsc) Format() PayloadFormat       { return AvroFormat }
func (JSONSchema) sealed()               {}
func (Avsc) sealed()                     {}

// MixedFormatsError reports a Kafka topic whose Message types declare their
// payloads in different formats: a Kafka topic is produced in one Wire
// format, so they must share one.
type MixedFormatsError struct {
	Topic string
	// Avro and JSONSchema name the Message types in each format.
	Avro, JSONSchema []string
}

func (e *MixedFormatsError) Error() string {
	return fmt.Sprintf("Kafka topic %q mixes payload formats: Avro (%s) and JSON Schema (%s); a Kafka topic is produced in one Wire format", e.Topic, strings.Join(e.Avro, ", "), strings.Join(e.JSONSchema, ", "))
}

// avroVersion matches the Avro 1.x versions an Avro schemaFormat may declare.
var avroVersion = regexp.MustCompile(`^1\.\d+\.\d+$`)

// payloadFormatOf reads a message's schemaFormat. The tool reads JSON Schema,
// which is a payload with no schemaFormat or one of the formats AsyncAPI 2.6.0
// and 3.0.0 require every implementation to support - the AsyncAPI Schema of
// the spec's own major version, and JSON Schema draft-07 - and Avro 1.x (#84).
// Any other format, Protobuf included, stops the run naming it (#76 decision
// 5).
func (d *Document) payloadFormatOf(node any, name string) (PayloadFormat, error) {
	if node == nil {
		return JSONSchemaFormat, nil
	}
	format, ok := node.(string)
	if !ok {
		return 0, fmt.Errorf("message %s: schemaFormat must be a string", name)
	}
	pf, ok := readsSchemaFormat(format, d.major)
	if !ok {
		return 0, fmt.Errorf("payload of %s is %s, which the tool does not read", name, format)
	}
	return pf, nil
}

// readsSchemaFormat reports the schema language of a format the tool reads in
// a spec of the major version. Media types compare case-insensitively, per
// RFC 6838.
func readsSchemaFormat(format string, major int) (PayloadFormat, bool) {
	mediaType, params, err := mime.ParseMediaType(format)
	if err != nil {
		return 0, false
	}
	switch mediaType {
	case "application/vnd.aai.asyncapi", "application/vnd.aai.asyncapi+json", "application/vnd.aai.asyncapi+yaml":
		return JSONSchemaFormat, asyncAPIVersion[major].MatchString(params["version"])
	case "application/schema+json", "application/schema+yaml":
		return JSONSchemaFormat, params["version"] == "draft-07"
	case "application/vnd.apache.avro", "application/vnd.apache.avro+json", "application/vnd.apache.avro+yaml":
		return AvroFormat, avroVersion.MatchString(params["version"])
	}
	return 0, false
}
