package s3kv

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// cacheLockErr builds an error message for failures to acquire the cache lock.
func cacheLockErr(action string, key string) error {
	return fmt.Errorf("could not acquire cache lock to %s %s", action, key)
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
