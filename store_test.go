package s3kv_test

import (
	"log"
	"sync"
	"time"

	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("store", func() {
	It("works", func() {
		s, err := s3kv.New(s3kv.Args{
			Namespace: "test",
			Backing:   mb,
			Timeouts: &s3kv.Timeouts{
				LockTimeout:    short,
				SessionTimeout: long,
			},
		})
		Expect(err).NotTo(HaveOccurred())

		// We can set then get a value
		sess, err := s.Lock("key1")
		Expect(err).NotTo(HaveOccurred())
		err = s.Set(sess, "key1", []byte("val1"))
		Expect(err).NotTo(HaveOccurred())
		s.Unlock(sess)

		val, err := s.Get("key1")
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("val1")))

		// Getting a value that does not exist returns nil
		val, err = s.Get("key2")
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())

		// Setting a value with a closed session returns an error
		err = s.Set(sess, "key1", []byte("val1"))
		Expect(err.Error()).To(ContainSubstring("does not include key"))

		// Setting a value for the wrong session returns an error
		sess, err = s.Lock("key1")
		Expect(err).NotTo(HaveOccurred())
		err = s.Set(sess, "key2", []byte("val2"))
		Expect(err.Error()).To(ContainSubstring("does not include key"))
		s.Unlock(sess)

		// We can only lock a value for one session at a time
		sess, err = s.Lock("key1")
		Expect(err).NotTo(HaveOccurred())
		_, err2 := s.Lock("key1")
		Expect(err2.Error()).To(ContainSubstring("timed out locking key"))
		s.Unlock(sess)

		// Sessions auto-close if left open for too long
		sess, err = s.Lock("key1")
		Expect(err).NotTo(HaveOccurred())
		err = s.Set(sess, "key1", []byte("val1"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(long * 2)
		err = s.Set(sess, "key1", []byte("val1"))
		Expect(err.Error()).To(ContainSubstring("does not include key"))
	})

	It("passes a stress test", func() {
		s, err := s3kv.New(s3kv.Args{
			Namespace: "test",
			Backing:   mb,
			Timeouts: &s3kv.Timeouts{
				LockAttemptInterval: 100 * time.Millisecond,
				LockTimeout:         5 * time.Second,
				SessionTimeout:      15 * time.Second,
			},
		})
		Expect(err).NotTo(HaveOccurred())

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
