package sloto_test

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/mplewis/s3kv/sloto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestS3kv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sloto Suite")
}

var _ = Describe("Sloto", func() {
	It("works as specified", func() {
		a := sloto.Args{
			LockAttemptInterval: 1 * time.Millisecond,
			LockAttemptTimeout:  10 * time.Millisecond,
			SessionTimeout:      100 * time.Millisecond,
		}
		s := sloto.New(a)

		sid, err := s.Lock("foo", "bar")
		Expect(err).ToNot(HaveOccurred())
		Expect(sid).ToNot(BeEmpty())

		Expect(s.Contains(sid, "foo")).To(BeTrue())
		Expect(s.Contains(sid, "bar")).To(BeTrue())
		Expect(s.Contains(sid, "baz")).To(BeFalse())

		<-time.After(a.SessionTimeout * 2)
		Expect(s.Contains(sid, "foo")).To(BeFalse())
		Expect(s.Contains(sid, "bar")).To(BeFalse())
		Expect(s.Contains(sid, "baz")).To(BeFalse())

		_, err = s.Lock("foo", "bar")
		Expect(err).ToNot(HaveOccurred())
		_, err = s.Lock("baz", "bar")
		Expect(err).To(MatchError("timed out locking key: bar"))
	})

	It("passes a stress test", func() {
		a := sloto.Args{
			LockAttemptTimeout: 15 * time.Second,
			SessionTimeout:     30 * time.Second,
		}
		s := sloto.New(a)

		// workers must atomically lock each key to end up with the correct values
		x := ""
		y := ""
		z := ""

		// 100 workers * 3 types = 300 simultaneous workers
		count := 100
		wg := sync.WaitGroup{}

		for i := 0; i < count*3; i++ {
			i := i
			var names []string
			var targets []*string

			// each worker wants to append to two of the three values concurrently
			if i%3 == 0 {
				names = []string{"x", "y"}
				targets = []*string{&x, &y}
			} else if i%3 == 1 {
				names = []string{"y", "z"}
				targets = []*string{&y, &z}
			} else {
				names = []string{"z", "x"}
				targets = []*string{&z, &x}
			}

			wg.Add(1)
			go func() {
				sid, err := s.Lock(names...)
				if err != nil {
					log.Panic(err)
				}

				// the heavy computation of an append takes 10 ms
				for _, target := range targets {
					*target += "x"
				}
				<-time.After(10 * time.Millisecond)

				s.Unlock(sid)
				wg.Done()
			}()
		}

		wg.Wait()

		Expect(len(x)).To(Equal(count * 2))
		Expect(len(y)).To(Equal(count * 2))
		Expect(len(z)).To(Equal(count * 2))
	})
})
