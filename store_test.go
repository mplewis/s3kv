package s3kv_test

import (
	"log"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestS3kv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3kv Suite")
}

const bucket = "mplewis-s3kv-test"

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

var _ = Describe("store", func() {
	AfterEach(emptyBucket)

	It("runs the demo", func() {
		s := s3kv.New(bucket)
		data, etag, err := s.Get("key1")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(BeNil())
		Expect(etag).To(BeNil())

		etag, err = s.Set("key1", []byte("somedata"), s3kv.NewObject)
		Expect(err).To(Not(HaveOccurred()))
		Expect(etag).To(Not(BeNil()))

		data, _, err = s.Get("key1")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(Equal([]byte("somedata")))

		_, err = s.Set("key1", []byte("someotherdata"), s3kv.NewObject)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("for key key1, expected ETag <new object> but found"))

		etag, err = s.Set("key1", []byte("someotherdata"), etag)
		Expect(err).To(Not(HaveOccurred()))

		data, _, err = s.Get("key1")
		Expect(err).To(Not(HaveOccurred()))
		Expect(data).To(Equal([]byte("someotherdata")))

		str := "some-outdated-etag"
		err = s.Del("key1", s3kv.ETag(&str))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("for key key1, expected ETag some-outdated-etag but found"))

		err = s.Del("key1", etag)
		Expect(err).To(Not(HaveOccurred()))
	})
})
