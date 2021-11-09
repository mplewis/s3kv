package s3kv

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// FindResult represents whether a value for a given key exists or not.
type FindResult bool

const (
	Found    FindResult = true
	NotFound FindResult = false
)

// Object
type Object interface {
	Key() string
	Get() ([]byte, FindResult, error)
	Set([]byte) error
	Del() error
}

// object is the implementation of Object.
type object struct {
	client *s3.S3
	bucket string
	key    string
	stale  bool
}

func (o object) staleErr() error {
	return fmt.Errorf("object is stale and can no longer be used: %s", o.key)
}

// Key returns the object's key.
func (o object) Key() string {
	return o.key
}

// Get returns the object's value, whether the value exists, and any error that occurred.
func (o object) Get() ([]byte, FindResult, error) {
	if o.stale {
		return nil, Found, o.staleErr()
	}
	resp, err := o.client.GetObject(&s3.GetObjectInput{Bucket: &o.bucket, Key: &o.key})
	if notFound(err) {
		return nil, NotFound, nil
	}
	if err != nil {
		return nil, Found, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, Found, err
	}
	return data, Found, nil
}

// Set sets the value for a given key.
func (o object) Set(data []byte) error {
	if o.stale {
		return o.staleErr()
	}
	_, err := o.client.PutObject(&s3.PutObjectInput{Bucket: &o.bucket, Key: &o.key, Body: bytes.NewReader(data)})
	return err
}

// Del deletes a key from the store.
func (o object) Del() error {
	if o.stale {
		return o.staleErr()
	}
	_, err := o.client.DeleteObject(&s3.DeleteObjectInput{Bucket: &o.bucket, Key: &o.key})
	return err
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
