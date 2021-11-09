package s3kv

import (
	"fmt"
	"time"

	"github.com/mplewis/s3kv/cache"
)

// Tag is a unique identifier for an S3 object's modification time and contents.
type Tag struct {
	// ETag and ModifiedAt make up the unique identifier for a non-empty S3 object.
	// If ETag is nil, the tag represents the absence of an S3 object.
	ETag       *string
	ModifiedAt int64
}

// Value returns the string representation of this Tag.
func (t Tag) Value() *string {
	if t.ETag == nil {
		return nil
	}
	s := fmt.Sprintf("%d_%s", t.ModifiedAt, *t.ETag)
	return &s
}

// Compare returns true if the two Tags are equal and false otherwise.
func (t Tag) Equal(x cache.Taggable) bool {
	a := t.Value()
	b := x.Value()
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// String returns a human-readable representation of this tag.
func (t Tag) String() string {
	if t.ETag == nil {
		return "{Tag: ObjectMissing}"
	}
	return fmt.Sprintf("{Tag: %s}", *t.Value())
}

// newTag builds a Tag from an S3 ETag.
func newTag(etag *string) *Tag {
	return &Tag{ETag: etag, ModifiedAt: time.Now().UnixNano()}
}

// str safely converts a maybe-nil string to a printable representation.
func str(s *string) string {
	if s == nil {
		return "<object missing>"
	}
	return *s
}

// ObjectMissing represents the absence of an S3 object.
var ObjectMissing = Tag{ETag: nil}

// StaleTagError is the error returned when an operation fails because the expected ETag did not match the actual ETag.
type StaleTagError struct {
	Key string
	// These should not be accessible to users. To get a fresh ETag, call `Store.Get`.
	// Reverse-engineering the expected ETag out of the error string is a bad idea.
	expected cache.Taggable
	actual   cache.Taggable
}

// Error converts a StaleTagError error into a human-readable string.
func (e StaleTagError) Error() string {
	return fmt.Sprintf(
		"for key %s, expected tag %s but found %s",
		e.Key, str(e.expected.Value()), str(e.actual.Value()),
	)
}
