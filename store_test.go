package s3kv_test

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("store", func() {
	BeforeEach(emptyBucket)

	It("behaves as expected", func() {
		// connect to bucket and lock two keys for use
		kvs, done, err := s.Lock("foo", "bar")
		defer done()
		Expect(err).NotTo(HaveOccurred())

		o := kvs["foo"]
		o2 := kvs["bar"]

		// get not found
		_, found, err := o.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(Equal(false))

		// set, then get found
		err = o.Set([]byte("baz"))
		Expect(err).NotTo(HaveOccurred())

		data, found, err := o.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(Equal(true))
		Expect(data).To(Equal([]byte("baz")))

		// set, then get found for a different key
		err = o2.Set([]byte("qux"))
		Expect(err).NotTo(HaveOccurred())

		data, found, err = o2.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(Equal(true))
		Expect(data).To(Equal([]byte("qux")))

		// the original key still holds its value
		data, found, err = o.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(Equal(true))
		Expect(data).To(Equal([]byte("baz")))

		// delete, then get not found
		err = o.Del()
		Expect(err).NotTo(HaveOccurred())

		_, found, err = o.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(Equal(false))
	})

	It("passes the atomic stress test", func() {
		// Skip("this shouldn't run against the cloud")
		start := time.Now()

		key := "addsub"
		total := 100

		// set initial value to 0
		keys, done, err := s.Lock(key)
		if err != nil {
			log.Panic(err)
		}
		err = keys[key].Set([]byte("0"))
		if err != nil {
			log.Panic(err)
		}
		done()

		// odd workers add 1, even workers subtract 1
		worker := func(s s3kv.Store, id int, delta int) {
			keys, done, err := s.Lock(key)
			if err != nil {
				log.Panic(err)
			}
			defer done()
			o := keys[key]
			val, find, err := o.Get()
			if err != nil {
				log.Panic(err)
			}
			if find == false {
				log.Panicf("%s not found", key)
			}
			n, err := strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				log.Panic(err)
			}
			n2 := int(n) + delta
			err = o.Set([]byte(fmt.Sprintf("%d", n2)))
			if err != nil {
				log.Panic(err)
			}
			// fmt.Printf("%d: %d -> %d\n", id, n, n2)
		}

		// start all the workers
		wg := sync.WaitGroup{}
		for i := 0; i < total; i++ {
			i := i
			wg.Add(1)
			d := 1
			if i%2 == 0 {
				d = -1
			}
			go func() {
				worker(s, i, d)
				wg.Done()
			}()
		}
		wg.Wait()

		// verify the final value sums to 0
		keys, done, err = s.Lock(key)
		if err != nil {
			log.Panic(err)
		}
		defer done()
		o := keys[key]
		val, find, err := o.Get()
		if err != nil {
			log.Panic(err)
		}
		if find == false {
			log.Panicf("%s not found", key)
		}
		Expect(val).To(Equal([]byte("0")))

		fmt.Printf(
			"%0.1f ms per change (%d changes in %0.2f sec)\n",
			float64(time.Since(start).Milliseconds())/float64(total),
			total,
			time.Since(start).Seconds(),
		)
	})
})
