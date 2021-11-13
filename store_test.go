package s3kv_test

import (
	"time"

	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("store", func() {
	BeforeEach(emptyBucket)

	It("behaves as expected", func() {
		store := s3kv.New(s3kv.Args{
			Bucket:  bucket,
			Session: s3Session,
			Timeout: 500 * time.Millisecond,
		})
		id, sess, err := store.OpenSession("foo", "bar")
		Expect(err).ToNot(HaveOccurred())
		Expect(id).ToNot(BeEmpty())
		_, ok := sess.Get("foo") // TODO: this should just get the value
		Expect(ok).To(BeFalse())
		sess.Set("foo", "baz")

	})
})
