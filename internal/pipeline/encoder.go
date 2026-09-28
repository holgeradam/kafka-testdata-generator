package pipeline

// Encoder is the Wire format seam at the Pipeline's single marshal call site.
// The adapters live with their formats in internal/wire: JsonEncoder for JSON,
// AvroEncoder and AvroDisplayEncoder for AVRO. The Pipeline never knows which
// adapter it holds; it calls Encode for every record and the adapter owns both
// Key and Payload byte encoding. See ADR-0007.
type Encoder interface {
	// Encode turns a generated message into wire-format bytes: its Key (nil
	// when the run has no key schema) and its Payload, as in-memory values,
	// with the Message type it is of. Returns the encoded Key bytes (nil when
	// the Key is nil) and Payload bytes.
	Encode(generated Generated) (keyBytes []byte, payloadBytes []byte, err error)
}
