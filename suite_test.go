package s3kv_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestS3kv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3kv Suite")
}
