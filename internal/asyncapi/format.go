package asyncapi

import (
	"fmt"
	"mime"
	"regexp"
)

// asyncAPIVersion matches the version of an AsyncAPI Schema format, per major
// version of the spec that declares it.
var asyncAPIVersion = map[int]*regexp.Regexp{
	2: regexp.MustCompile(`^2\.\d+\.\d+$`),
	3: regexp.MustCompile(`^3\.\d+\.\d+$`),
}

// payloadFormat is the schema language a message's payload is written in.
type payloadFormat int

const (
	jsonSchema payloadFormat = iota
	avroSchema
)

// avroVersion matches the Avro 1.x versions an Avro schemaFormat may declare.
var avroVersion = regexp.MustCompile(`^1\.\d+\.\d+$`)

// payloadFormatOf reads a message's schemaFormat. The tool reads JSON Schema,
// which is a payload with no schemaFormat or one of the formats AsyncAPI 2.6.0
// and 3.0.0 require every implementation to support - the AsyncAPI Schema of
// the spec's own major version, and JSON Schema draft-07 - and Avro 1.x (#84).
// Any other format, Protobuf included, stops the run naming it (#76 decision
// 5).
func (d *Document) payloadFormatOf(node any, name string) (payloadFormat, error) {
	if node == nil {
		return jsonSchema, nil
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
func readsSchemaFormat(format string, major int) (payloadFormat, bool) {
	mediaType, params, err := mime.ParseMediaType(format)
	if err != nil {
		return 0, false
	}
	switch mediaType {
	case "application/vnd.aai.asyncapi", "application/vnd.aai.asyncapi+json", "application/vnd.aai.asyncapi+yaml":
		return jsonSchema, asyncAPIVersion[major].MatchString(params["version"])
	case "application/schema+json", "application/schema+yaml":
		return jsonSchema, params["version"] == "draft-07"
	case "application/vnd.apache.avro", "application/vnd.apache.avro+json", "application/vnd.apache.avro+yaml":
		return avroSchema, avroVersion.MatchString(params["version"])
	}
	return 0, false
}
