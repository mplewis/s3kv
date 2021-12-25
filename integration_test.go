package s3kv_test

// import (
// 	"sync"

// 	"github.com/mplewis/s3kv"
// 	. "github.com/onsi/ginkgo"
// 	. "github.com/onsi/gomega"
// )

// var _ = Describe("integration test", func() {
// 	Context("with an S3 backing", func() {
// 		var s *s3kv.Store
// 		BeforeEach(func() {
// 			emptyBucket()
// 			var err error
// 			s, err = s3kv.New(s3kv.Args{
// 				Backing:        mb,
// 				LockTimeout:    short,
// 				SessionTimeout: long,
// 			})
// 			Expect(err).NotTo(HaveOccurred())
// 		})

// 		It("uses sessions to ensure atomicity of writes", func() {
// 			k := "atomicData"
// 			sess, err := s.OpenSession(k)
// 			Expect(err).NotTo(HaveOccurred())
// 			err = s.Set(sess, k, []byte(""))
// 			Expect(err).NotTo(HaveOccurred())
// 			s.CloseSession(sess)

// 			wg := sync.WaitGroup{}
// 			for i := 0; i < 1000; i++ {
// 				sym := "x"
// 				if i%2 == 0 {
// 					sym = "o"
// 				}
// 				wg.Add(1)
// 				go func() {
// 					sess, err := s.OpenSession(k)
// 					Expect(err).NotTo(HaveOccurred())

// 					val, err := s.Get(k)
// 					Expect(err).NotTo(HaveOccurred())
// 					val = append(val, []byte(sym)...)

// 					err = s.Set(sess, k, val)
// 					Expect(err).NotTo(HaveOccurred())

// 					s.CloseSession(sess)
// 					wg.Done()
// 				}()
// 			}
// 			wg.Wait()

// 			sess, err = s.OpenSession("unity")
// 			Expect(err).NotTo(HaveOccurred())
// 			val, err := s.Get(k)
// 			Expect(err).NotTo(HaveOccurred())
// 			s.CloseSession(sess)
// 			x := 0
// 			o := 0
// 			for _, c := range string(val) {
// 				if c == 'x' {
// 					x++
// 				} else if c == 'o' {
// 					o++
// 				} else {
// 					Fail("unexpected symbol: " + string(c))
// 				}
// 			}

// 			Expect(x).To(Equal(500))
// 			Expect(o).To(Equal(500))
// 		})
// 	})
// })
