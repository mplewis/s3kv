package s3kv_test

import (
	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("store", func() {
	It("works with a memory backing", func() {
		s, err := s3kv.New(s3kv.Args{
			Backing:        mb,
			LockTimeout:    short,
			SessionTimeout: long,
		})
		Expect(err).ToNot(HaveOccurred())

		sess, err := s.OpenSession("key1")
		Expect(err).ToNot(HaveOccurred())
		err = s.Set(sess, "key1", []byte("val1"))
		Expect(err).ToNot(HaveOccurred())
		s.CloseSession(sess)

		val, err := s.Get("key1")
		Expect(err).ToNot(HaveOccurred())
		Expect(val).To(Equal([]byte("val1")))
	})
})
