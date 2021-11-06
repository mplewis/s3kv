package s3kv

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Store is a key-value store backed by an S3 bucket.
// Its operations are atomic and require you to provide the expected state of the blob as an ETag before you mutate it.
// If the state you provide is out of date, you must fetch the value and ETag again before retrying.
//
// Get returns the value and ETag for the given key.
//
// Set sets the value for the given key, ensuring the expected ETag matches the actual ETag.
// If you are setting a value for a new key, use the `S3kv.NewObject` sigil for your expected ETag.
//
// Del deletes the value for the given key, ensuring the expected ETag matches the actual ETag.
type Store interface {
	Get(key string) ([]byte, ETag, error)
	Set(key string, value []byte, expectedETag ETag) (ETag, error)
	Del(key string, expectedETag ETag) error
}

// store is the implementation of Store.
type store struct {
	s3     *s3.S3
	bucket string
	cache  map[string]ETag
}

// StaleETagError is the error returned when an operation fails because the expected ETag did not match the actual ETag.
type StaleETagError struct {
	Key string
	// These should not be accessible to users. To get a fresh ETag, call `Store.Get`.
	// Reverse-engineering the expected ETag out of the error string is a bad idea.
	expected ETag
	actual   ETag
}

// Error converts a StaleETagError error into a human-readable string.
func (e StaleETagError) Error() string {
	return fmt.Sprintf("for key %s, expected ETag %s but found %s", e.Key, str(e.expected), str(e.actual))
}

// New creates a new key-value store backed by an S3 bucket.
func New(bucket string) Store {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	return store{svc, bucket, map[string]ETag{}}
}

func (s store) Get(key string) ([]byte, ETag, error) {
	resp, err := s.s3.GetObject(&s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if notFound(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	et := ETag(resp.ETag)
	s.cache[key] = et
	return data, et, nil
}

func (s store) Set(key string, value []byte, xETag ETag) (ETag, error) {
	err := s.check(key, xETag)
	if err != nil {
		return nil, err
	}
	resp, err := s.s3.PutObject(&s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(value)})
	if err != nil {
		return nil, err
	}
	et := ETag(resp.ETag)
	s.cache[key] = et
	return et, nil
}

func (s store) Del(key string, xETag ETag) error {
	err := s.check(key, xETag)
	if err != nil {
		return err
	}
	_, err = s.s3.DeleteObject(&s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return err
	}
	s.cache[key] = nil
	return nil
}

// check ensures the expected ETag matches the actual ETag for a key.
func (s store) check(key string, xETag ETag) error {
	aETag, ok := s.cache[key]
	if !ok {
		_, et, err := s.Get(key)
		if err != nil {
			return err
		}
		s.cache[key] = et
		aETag = et
	}
	if !cmp(xETag, aETag) {
		return StaleETagError{key, xETag, aETag}
	}
	return nil
}

// notFound checks if an error is an AWS S3 NoSuchKey error.
func notFound(err error) bool {
	aerr, ok := err.(awserr.Error)
	if !ok {
		return false
	}
	switch aerr.Code() {
	case s3.ErrCodeNoSuchKey:
		return true
	default:
		return false
	}
}
