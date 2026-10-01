package generator

// MaxRecursionDepth exposes the $ref expansion bound to the external tests.
const MaxRecursionDepth = maxRecursionDepth

// SelfContained exposes the walked field's self-contained schema, given the
// Holds-capable value Walk returns.
func SelfContained(f any) map[string]any { return f.(*field).selfContained() }
