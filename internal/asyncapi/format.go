package asyncapi

import (
	"fmt"
	"mime"
	"regexp"
)

// asyncAPI2 matches the version of an AsyncAPI 2.x Schema format.
var asyncAPI2 = regexp.MustCompile(`^2\.\d+\.\d+$`)

// checkSchemaFormat refuses a payload the tool cannot read: it reads JSON
// Schema, which is a payload with no schemaFormat or one of the formats
// AsyncAPI 2.6.0 requires every implementation to support - the AsyncAPI
// Schema of a 2.x version, and JSON Schema draft-07. Any other format, Avro
// and Protobuf included, stops the run naming it (#76 decision 5).
func checkSchemaFormat(msg map[string]any, name string) error {
	if msg["schemaFormat"] == nil {
		return nil
	}
	format, ok := msg["schemaFormat"].(string)
	if !ok {
		return fmt.Errorf("message %s: schemaFormat must be a string", name)
	}
	if !readsSchemaFormat(format) {
		return fmt.Errorf("payload of %s is %s, which the tool does not read", name, format)
	}
	return nil
}

// readsSchemaFormat reports whether format is a JSON Schema format the tool
// reads. Media types compare case-insensitively, per RFC 6838.
func readsSchemaFormat(format string) bool {
	mediaType, params, err := mime.ParseMediaType(format)
	if err != nil {
		return false
	}
	switch mediaType {
	case "application/vnd.aai.asyncapi", "application/vnd.aai.asyncapi+json", "application/vnd.aai.asyncapi+yaml":
		return asyncAPI2.MatchString(params["version"])
	case "application/schema+json", "application/schema+yaml":
		return params["version"] == "draft-07"
	}
	return false
}
