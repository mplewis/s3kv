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
type Store interface {
	Get(key string) ([]byte, ETag, error)
	Set(key string, value []byte, xETag ETag) (ETag, error)
	Del(key string, xETag ETag) error
}

// store is the implementation of Store.
type store struct {
	s3     *s3.S3
	bucket string
	cache  map[string]ETag
}

// New creates a new key-value store backed by an S3 bucket.
func New(bucket string) Store {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	return store{svc, bucket, map[string]ETag{}}
}

// Get returns the value and ETag for the given key.
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

// Set sets the value for the given key, making sure that the expected ETag matches the actual ETag.
// If you are setting a value for a new key, use the S3kv.NewObject sigil for your expected ETag.
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

// Del deletes the value for the given key, making sure that the expected ETag matches the actual ETag.
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
		return fmt.Errorf("for key %s, expected ETag %s but found %s", key, str(xETag), str(aETag))
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
