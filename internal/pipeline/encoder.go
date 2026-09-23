package pipeline

// Encoder is the Wire format seam at the Pipeline's single marshal call site.
// The adapters live with their formats in internal/wire: JsonEncoder for JSON,
// AvroEncoder and AvroDisplayEncoder for AVRO. The Pipeline never knows which
// adapter it holds; it calls Encode for every record and the adapter owns both
// Key and Payload byte encoding. See ADR-0007.
type Encoder interface {
	// Encode turns a generated record into wire-format bytes. key is the
	// record's Key (nil when the run has no key schema); payload is the
	// generated record as an in-memory value. Returns the encoded Key bytes
	// (nil when key is nil) and Payload bytes.
	Encode(key any, payload any) (keyBytes []byte, payloadBytes []byte, err error)
}
