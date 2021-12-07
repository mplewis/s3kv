package s3kv_test

import (
	"sync"
	"time"

	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("store", func() {
	Context("with a memory backing", func() {
		var s *s3kv.Store
		BeforeEach(func() {
			var err error
			s, err = s3kv.New(s3kv.Args{
				Backing:        mb,
				LockTimeout:    short,
				SessionTimeout: long,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("works", func() {
			// We can set then get a value
			sess, err := s.Lock("key1")
			Expect(err).NotTo(HaveOccurred())
			err = s.Set(sess, "key1", []byte("val1"))
			Expect(err).NotTo(HaveOccurred())
			err = s.Unlock(sess)
			Expect(err).NotTo(HaveOccurred())

			val, err := s.Get("key1")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]byte("val1")))

			// Getting a value that does not exist returns nil
			val, err = s.Get("key2")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(BeNil())

			// Setting a value with a closed session returns an error
			err = s.Set(sess, "key1", []byte("val1"))
			Expect(err.Error()).To(ContainSubstring("session not found"))

			// Setting a value for the wrong session returns an error
			sess, err = s.Lock("key1")
			Expect(err).NotTo(HaveOccurred())
			err = s.Set(sess, "key2", []byte("val2"))
			Expect(err.Error()).To(ContainSubstring("does not have key: key2"))
			err = s.Unlock(sess)
			Expect(err).NotTo(HaveOccurred())

			// We can only lock a value for one session at a time
			sess, err = s.Lock("key1")
			Expect(err).NotTo(HaveOccurred())
			_, err2 := s.Lock("key1")
			Expect(err2).To(MatchError("could not acquire lock for key: key1"))
			err = s.Unlock(sess)
			Expect(err).NotTo(HaveOccurred())

			// Sessions auto-close if left open for too long
			sess, err = s.Lock("key1")
			Expect(err).NotTo(HaveOccurred())
			err = s.Set(sess, "key1", []byte("val1"))
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(long * 2)
			err = s.Set(sess, "key1", []byte("val1"))
			Expect(err.Error()).To(ContainSubstring("session not found"))
		})

		It("uses sessions to ensure atomicity of writes", func() {
			k := "atomicData"
			sess, err := s.Lock(k)
			Expect(err).NotTo(HaveOccurred())
			err = s.Set(sess, k, []byte(""))
			Expect(err).NotTo(HaveOccurred())
			err = s.Unlock(sess)
			Expect(err).NotTo(HaveOccurred())

			wg := sync.WaitGroup{}
			for i := 0; i < 1000; i++ {
				sym := "x"
				if i%2 == 0 {
					sym = "o"
				}
				wg.Add(1)
				go func() {
					sess, err := s.Lock(k)
					Expect(err).NotTo(HaveOccurred())

					val, err := s.Get(k)
					Expect(err).NotTo(HaveOccurred())
					val = append(val, []byte(sym)...)

					err = s.Set(sess, k, val)
					Expect(err).NotTo(HaveOccurred())

					err = s.Unlock(sess)
					Expect(err).NotTo(HaveOccurred())
					wg.Done()
				}()
			}
			wg.Wait()

			sess, err = s.Lock("unity")
			Expect(err).NotTo(HaveOccurred())
			val, err := s.Get(k)
			Expect(err).NotTo(HaveOccurred())
			err = s.Unlock(sess)
			Expect(err).NotTo(HaveOccurred())
			x := 0
			o := 0
			for _, c := range string(val) {
				if c == 'x' {
					x++
				} else if c == 'o' {
					o++
				} else {
					Fail("unexpected symbol: " + string(c))
				}
			}

			Expect(x).To(Equal(500))
			Expect(o).To(Equal(500))
		})
	})
})
