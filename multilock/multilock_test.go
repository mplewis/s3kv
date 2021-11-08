package multilock_test

import (
	"fmt"
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
		l := multilock.New()

		r := l.Acquire(t50ms, key)
		Expect(r).To(BeTrue())

		r = l.Acquire(t50ms, key)
		Expect(r).To(BeFalse())
		l.Release(t50ms, key)

		r = l.Acquire(t50ms, key)
		Expect(r).To(BeTrue())
		l.Release(t50ms, key)

		r = l.Acquire(t50ms, key)
		Expect(r).To(BeTrue())

		fmt.Println("Lock is tied up. Waiting to release")

		go func() {
			time.Sleep(25 * time.Millisecond)
			l.Release(100*time.Millisecond, key)
			fmt.Println("Lock has been released.")
		}()

		// time.Sleep(25 * time.Millisecond)
		// l.Release(t50ms, key)
		// fmt.Println("Lock has been released.")

		fmt.Println("Trying to acquire lock")
		r = l.Acquire(2000*time.Millisecond, key)
		Expect(r).To(BeTrue())
		l.Release(t50ms, key)

		// Expect(false).To(BeTrue())

		// go func() {
		// 	fmt.Println(time.Now().UnixMicro(), "Preparing to release lock...")
		// 	time.Sleep(25 * time.Millisecond)
		// 	fmt.Println(time.Now().UnixMicro(), "Releasing lock delayed...")
		// 	l.Release(t50ms, key)
		// 	fmt.Println(time.Now().UnixMicro(), "Released lock.")
		// }()

		// fmt.Println(time.Now().UnixMicro(), "Acquisition start...")
		// r = l.Acquire(500*time.Millisecond, key)
		// fmt.Println(time.Now().UnixMicro(), "Acquisition complete.")
		// Expect(r).To(BeTrue())
		// l.Release(t50ms, key)
	})

	// It("runs the lock stress test", func() {
	// 	l := lock.New()
	// 	var r bool
	// 	for i := 0; i < 1000; i++ {
	// 		s := time.Now()
	// 		r = l.Acquire(2000 * time.Millisecond)
	// 		Expect(r).To(BeTrue())
	// 		l.Release()
	// 		fmt.Printf("Acquired and released in %d ns\n", time.Since(s).Nanoseconds())
	// 	}
	// 	Expect(false).To(BeTrue())
	// })
})
