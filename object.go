package s3kv

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Object represents an S3 object in the store.
type Object interface {
	// Key returns the object's key.
	Key() string
	// Get returns the object's value, whether the value exists, and any error that occurred.
	Get() (data []byte, found bool, err error)
	// Set sets the value for a given key.
	Set(data []byte) error
	// Del deletes a key from the store.
	Del() error
}

// object is the implementation of Object.
type object struct {
	client *s3.S3
	bucket string
	key    string
	stale  bool
}

// staleErr builds an error for a stale object.
func (o object) staleErr() error {
	return fmt.Errorf("object is stale and can no longer be used: %s", o.key)
}

// Key returns the object's key.
func (o object) Key() string {
	return o.key
}

// Get returns the object's value, whether the value exists, and any error that occurred.
func (o object) Get() (data []byte, found bool, err error) {
	if o.stale {
		return nil, true, o.staleErr()
	}
	resp, err := o.client.GetObject(&s3.GetObjectInput{Bucket: &o.bucket, Key: &o.key})
	if notFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, true, err
	}
	return data, true, nil
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
