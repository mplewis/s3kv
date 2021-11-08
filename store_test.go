package s3kv_test

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mplewis/s3kv"
	"github.com/mplewis/s3kv/etag"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const bucket = "mplewis-s3kv-test"
const attemptsBeforeGiveUp = 100

func emptyBucket() {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		log.Panic(err)
	}
	if len(resp.Contents) == 0 {
		return
	}

	objects := []*s3.ObjectIdentifier{}
	for _, obj := range resp.Contents {
		objects = append(objects, &s3.ObjectIdentifier{Key: obj.Key})
	}
	_, err = svc.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{Objects: objects},
	})
	if err != nil {
		log.Panic(err)
	}
}

func add(id int, store s3kv.Store, key string, delta int) {
	s := time.Now()
	var err error
	var att int
	for att = 0; att < attemptsBeforeGiveUp; att++ {
		err = nil
		raw, etag, err := store.Get(key)
		if err != nil {
			log.Printf("%d: error getting existing value: %s\n", id, err)
			continue
		}
		v, err := strconv.ParseInt(string(raw), 10, 0)
		if err != nil {
			log.Printf("%d: error parsing int: %s\n", id, err)
			continue
		}
		val := int(v)
		nval := val + delta
		_, err = store.Set(key, []byte(fmt.Sprintf("%d", nval)), etag)
		if err != nil {
			log.Printf("%d: error setting new value: %s\n", id, err)
			continue
		}
		break
	}
	if err != nil {
		log.Printf("%d: Giving up after %d att, %0.2f s", id, att, time.Since(s).Seconds())
		return
	}
	log.Printf("%d: Updated in %d att, %0.2f s\n", id, att, time.Since(s).Seconds())
}

var _ = Describe("store", func() {
	AfterEach(emptyBucket)

	It("runs the demo", func() {
		s := s3kv.New(bucket)
		data, etg, err := s.Get("key1")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(BeNil())
		Expect(etg).To(BeNil())

		s1 := "key-already-exists"
		_, err = s.Set("key1", []byte("somedata"), etag.ETag(&s1))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("for key key1, expected ETag key-already-exists but found <new object>"))
		e, ok := err.(s3kv.StaleETagError)
		Expect(ok).To(BeTrue())
		Expect(e.Key).To(Equal("key1"))

		etg, err = s.Set("key1", []byte("somedata"), s3kv.NewObject)
		Expect(err).To(Not(HaveOccurred()))
		Expect(etg).To(Not(BeNil()))

		data, _, err = s.Get("key1")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(Equal([]byte("somedata")))

		_, err = s.Set("key1", []byte("someotherdata"), s3kv.NewObject)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("for key key1, expected ETag <new object> but found"))

		etg, err = s.Set("key1", []byte("someotherdata"), etg)
		Expect(err).To(Not(HaveOccurred()))

		data, _, err = s.Get("key1")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(Equal([]byte("someotherdata")))

		s2 := "some-outdated-etag"
		err = s.Del("key1", etag.ETag(&s2))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("for key key1, expected ETag some-outdated-etag but found"))

		err = s.Del("key1", etg)
		Expect(err).To(Not(HaveOccurred()))

		data, etg, err = s.Get("key1")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(BeNil())
		Expect(etg).To(BeNil())
	})

	It("runs the store stress test", func() {
		s := s3kv.New(bucket)
		s.Set("stresstest", []byte("0"), s3kv.NewObject)

		data, _, err := s.Get("stresstest")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(Equal([]byte("0")))

		wg := sync.WaitGroup{}
		for i := 0; i < 3; i++ {
			i := i
			wg.Add(1)
			go func() {
				log.Printf("%d: Adding 1...\n", i)
				add(i, s, "stresstest", 1)
				wg.Done()
				log.Printf("%d: Done adding 1.\n", i)
			}()
		}
		for i := 3; i < 6; i++ {
			i := i
			wg.Add(1)
			go func() {
				log.Printf("%d: Adding -1...\n", i)
				add(i, s, "stresstest", -1)
				wg.Done()
				log.Printf("%d: Done adding -1.\n", i)
			}()
		}
		log.Println("Waiting for workers to complete...")
		wg.Wait()
		log.Println("Workers are done.")

		data, _, err = s.Get("stresstest")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(Equal([]byte("0")))

		Expect(false).To(BeTrue())
	})
})
