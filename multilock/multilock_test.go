package multilock_test

import (
	"sync"
	"testing"
	"time"

	"github.com/mplewis/s3kv/multilock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLock(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lock Suite")
}

const t50ms = 50 * time.Millisecond
const key = "somekey"

var _ = Describe("Multilock", func() {
	It("works as intended", func() {
		l := multilock.New(t50ms)

		r := l.Acquire(key)
		Expect(r).To(BeTrue())

		r = l.Acquire(key)
		Expect(r).To(BeFalse())
		l.Release(key)

		r = l.Acquire(key)
		Expect(r).To(BeTrue())
		l.Release(key)

		r = l.Acquire(key)
		Expect(r).To(BeTrue())

		go func() {
			time.Sleep(25 * time.Millisecond)
			l.Release(key)
		}()

		r = l.Acquire(key)
		Expect(r).To(BeTrue())
		l.Release(key)
	})

	It("runs the lock stress test", func() {
		l := multilock.New(t50ms)
		success := true

		wg := sync.WaitGroup{}
		for i := 0; i < 1000; i++ {
			wg.Add(1)
			go func() {
				r := l.Acquire("somekey")
				l.Release("somekey")
				if !r {
					success = false
				}
				wg.Done()
			}()
		}
		wg.Wait()

		Expect(success).To(BeTrue())
	})
})
