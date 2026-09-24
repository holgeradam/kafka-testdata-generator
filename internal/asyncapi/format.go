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

// checkSchemaFormat refuses a payload the tool cannot read: it reads JSON
// Schema, which is a payload with no schemaFormat or one of the formats
// AsyncAPI 2.6.0 and 3.0.0 require every implementation to support - the
// AsyncAPI Schema of the spec's own major version, and JSON Schema draft-07.
// Any other format, Avro and Protobuf included, stops the run naming it (#76
// decision 5).
func (d *Document) checkSchemaFormat(node any, name string) error {
	if node == nil {
		return nil
	}
	format, ok := node.(string)
	if !ok {
		return fmt.Errorf("message %s: schemaFormat must be a string", name)
	}
	if !readsSchemaFormat(format, d.major) {
		return fmt.Errorf("payload of %s is %s, which the tool does not read", name, format)
	}
	return nil
}

// readsSchemaFormat reports whether format is a JSON Schema format the tool
// reads in a spec of the major version. Media types compare
// case-insensitively, per RFC 6838.
func readsSchemaFormat(format string, major int) bool {
	mediaType, params, err := mime.ParseMediaType(format)
	if err != nil {
		return false
	}
	switch mediaType {
	case "application/vnd.aai.asyncapi", "application/vnd.aai.asyncapi+json", "application/vnd.aai.asyncapi+yaml":
		return asyncAPIVersion[major].MatchString(params["version"])
	case "application/schema+json", "application/schema+yaml":
		return params["version"] == "draft-07"
	}
	return false
}
