package lock_test

import (
	"fmt"
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

		s := time.Now()
		r := l.Acquire(t50ms)
		Expect(r).To(BeTrue())
		fmt.Println(time.Since(s))

		r = l.Acquire(t50ms)
		Expect(r).To(Not(BeTrue()))
		fmt.Println(time.Since(s))

		go func() {
			time.Sleep(25 * time.Millisecond)
			l.Release()
		}()

		r = l.Acquire(t50ms)
		Expect(r).To(BeTrue())
		fmt.Println(time.Since(s))
		l.Release()

		Expect(false).To(BeTrue())
	})
})
