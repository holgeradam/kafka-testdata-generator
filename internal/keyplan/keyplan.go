// Package keyplan owns how a run generates its Keys: the key schema produces
// each value (ADR-0009), and with -records-per-key above 1 Keys recur, so an
// Entity carries several records (Reuse, #75). Where a Key is planted into the
// Payload is the Planting module's business (internal/planting, #108).
package keyplan

// Generator produces one Key value from the key schema.
type Generator interface {
	Value() (any, error)
}
