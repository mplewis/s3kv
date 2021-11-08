package lock_test

import (
	"testing"
	"time"

	"github.com/mplewis/s3kv/lock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLock(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lock Suite")
}

const t50ms = 50 * time.Millisecond

var _ = Describe("Lock", func() {
	It("works as intended", func() {
		l := lock.New()

		r := l.Acquire(t50ms)
		Expect(r).To(BeTrue())

		r = l.Acquire(t50ms)
		Expect(r).To(BeFalse())

		go func() {
			time.Sleep(25 * time.Millisecond)
			l.Release()
		}()

		r = l.Acquire(t50ms)
		Expect(r).To(BeTrue())
		l.Release()
	})
})
