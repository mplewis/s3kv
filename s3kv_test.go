package s3kv_test

import (
	"os"
	"testing"

	"log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3kv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3kv Suite")
}

const bucket = "mplewis-s3kv-test"

var client *s3.S3
var s3Session *session.Session

func init() {
	if os.Getenv("TEST_WITH_LIVE_S3") != "" {
		s3Session = session.Must(session.NewSession())
	} else {
		s3Session = session.Must(session.NewSessionWithOptions(session.Options{
			Profile: "localhost",
			Config: aws.Config{
				Region:                        aws.String("us-east-1"),
				Endpoint:                      aws.String("http://localhost:9999"),
				CredentialsChainVerboseErrors: aws.Bool(true),
				Credentials:                   credentials.NewStaticCredentials("<access-key>", "<secret-key>", ""),
				S3ForcePathStyle:              aws.Bool(true),
			},
		}))
	}
	client = s3.New(s3Session)
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
