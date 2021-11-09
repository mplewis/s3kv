package s3kv_test

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const bucket = "mplewis-s3kv-test"

var client = s3.New(sess)
var sess = session.Must(session.NewSessionWithOptions(options))
var options = session.Options{
	Profile: "localhost",
	Config: aws.Config{
		Region:                        aws.String("us-east-1"),
		Endpoint:                      aws.String("http://localhost:9999"),
		CredentialsChainVerboseErrors: aws.Bool(true),
		Credentials:                   credentials.NewStaticCredentials("<access-key>", "<secret-key>", ""),
		S3ForcePathStyle:              aws.Bool(true),
	},
}

func emptyBucket() {
	resp, err := client.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
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
	_, err = client.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{Objects: objects},
	})
	if err != nil {
		log.Panic(err)
	}
}

var _ = Describe("store", func() {
	BeforeEach(emptyBucket)

	It("runs the demo", func() {
		// connect to bucket and lock two keys for use
		s := s3kv.New(s3kv.S3kvArgs{Bucket: bucket, Session: sess})
		kvs, done, err := s.Lock("foo", "bar")
		defer done()
		Expect(err).NotTo(HaveOccurred())

		o := kvs["foo"]
		o2 := kvs["bar"]

		// get not found
		_, find, err := o.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(find).To(Equal(s3kv.NotFound))

		// set, then get found
		err = o.Set([]byte("baz"))
		Expect(err).NotTo(HaveOccurred())

		data, find, err := o.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(find).To(Equal(s3kv.Found))
		Expect(data).To(Equal([]byte("baz")))

		// set, then get found for a different key
		err = o2.Set([]byte("qux"))
		Expect(err).NotTo(HaveOccurred())

		data, find, err = o2.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(find).To(Equal(s3kv.Found))
		Expect(data).To(Equal([]byte("qux")))

		// the original key still holds its value
		data, find, err = o.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(find).To(Equal(s3kv.Found))
		Expect(data).To(Equal([]byte("baz")))

		// delete, then get not found
		err = o.Del()
		Expect(err).NotTo(HaveOccurred())

		_, find, err = o.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(find).To(Equal(s3kv.NotFound))
	})
})
