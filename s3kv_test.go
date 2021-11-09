package s3kv_test

import (
	"os"
	"testing"
	"time"

	"log"

	"github.com/mplewis/s3kv"
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

var s s3kv.Store
var client *s3.S3
var sess *session.Session

func init() {
	if os.Getenv("TEST_WITH_LIVE_S3") != "" {
		sess = session.Must(session.NewSession())
	} else {
		sess = session.Must(session.NewSessionWithOptions(session.Options{
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
	s = s3kv.New(s3kv.Args{Bucket: bucket, Session: sess, Timeout: 60 * time.Second})
	client = s3.New(sess)
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
