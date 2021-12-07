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
		s := sloto.New(sloto.Args{
			LockAttemptInterval: 100 * time.Millisecond,
			LockAttemptTimeout:  5 * time.Second,
			SessionTimeout:      15 * time.Second,
		})

		// workers must atomically lock each key to end up with the correct values
		w := ""
		x := ""
		y := ""
		z := ""

		count := 100
		kinds := 4
		wg := sync.WaitGroup{}

		for i := 0; i < count*kinds; i++ {
			i := i
			var names []string
			var targets []*string

			// each worker wants to append to two different values concurrently,
			// but only workers 0 + 2 and workers 1 + 3 are mutually compatible
			if i%kinds == 0 {
				names = []string{"w", "x"}
				targets = []*string{&w, &x}
			} else if i%kinds == 1 {
				names = []string{"x", "y"}
				targets = []*string{&x, &y}
			} else if i%kinds == 2 {
				names = []string{"y", "z"}
				targets = []*string{&y, &z}
			} else {
				names = []string{"z", "w"}
				targets = []*string{&z, &w}
			}

			wg.Add(1)
			go func() {
				sid, err := s.Lock(names...)
				if err != nil {
					log.Panic(err)
				}

				// simulate heavy computation by making an append take 1 ms
				for _, target := range targets {
					*target += "x"
				}
				<-time.After(1 * time.Millisecond)

				s.Unlock(sid)
				wg.Done()
			}()
		}

		wg.Wait()

		Expect(len(w)).To(Equal(count * 2))
		Expect(len(x)).To(Equal(count * 2))
		Expect(len(y)).To(Equal(count * 2))
		Expect(len(z)).To(Equal(count * 2))
	})
})
