package s3kv_test

import (
	"strings"
	"testing"
	"time"

	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestS3kv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3kv Suite")
}

var short = 50 * time.Millisecond
var long = 500 * time.Millisecond

// const bucket = "mplewis-s3kv-test"
// const ns = "test-ns"

// var ctx = context.Background()
// var s3b s3kv.S3Backing

func init() {
	// var err error
	// var cfg aws.Config
	// if os.Getenv("TEST_WITH_LIVE_S3") != "" {
	// 	cfg, err = config.LoadDefaultConfig(ctx)
	// 	if err != nil {
	// 		log.Panic(err)
	// 	}
	// } else {
	// 	// TODO: Implement custom endpoint
	// 	cfg = aws.Config{
	// 		Region:                        "us-east-1",
	// 		Endpoint:                      "http://localhost:9999",
	// 		CredentialsChainVerboseErrors: aws.Bool(true),
	// 		Credentials:                   credentials.NewStaticCredentials("access-key", "secret-key", ""),
	// 		S3ForcePathStyle:              aws.Bool(true),
	// 	}
	// }
	// client := s3.NewFromConfig(cfg)
	// s3b = s3kv.NewS3Backing(s3kv.S3BackingArgs{
	// 	Bucket:    bucket,
	// 	Namespace: ns,
	// 	Client:    client,
	// 	Config:    cfg,
	// })
}

// func emptyBucket() {
// 	resp, err := client.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
// 	if err != nil {
// 		log.Panic(err)
// 	}
// 	if len(resp.Contents) == 0 {
// 		return
// 	}

// 	objects := []*s3.ObjectIdentifier{}
// 	for _, obj := range resp.Contents {
// 		objects = append(objects, &s3.ObjectIdentifier{Key: obj.Key})
// 	}
// 	_, err = client.DeleteObjects(&s3.DeleteObjectsInput{
// 		Bucket: aws.String(bucket),
// 		Delete: &s3.Delete{Objects: objects},
// 	})
// 	if err != nil {
// 		log.Panic(err)
// 	}
// }

var mb = MemoryBacking{map[string][]byte{}}

type MemoryBacking struct {
	data map[string][]byte
}

func (b MemoryBacking) List(prefix string) ([]s3kv.Key, error) {
	keys := []s3kv.Key{}
	for k := range b.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (b MemoryBacking) Get(key s3kv.Key) ([]byte, error) {
	return b.data[key], nil
}

func (b MemoryBacking) Set(key s3kv.Key, value []byte) error {
	b.data[key] = value
	return nil
}

func (b MemoryBacking) Del(key s3kv.Key) error {
	delete(b.data, key)
	return nil
}
