package etag

// ETag is a hash representing the contents of an object.
type ETag *string

// NewObject is a sigil indicating you want to set a new (nonexistent) key.
var NewObject = ETag(nil)

// str safely converts an ETag to a printable string.
func Str(e ETag) string {
	if e == nil {
		return "<new object>"
	}
	return *e
}

// cmp compares two ETags for equality.
func Cmp(a, b ETag) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
