package s3kv_test

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mplewis/s3kv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestS3kv(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3kv Suite")
}

var short = 50 * time.Millisecond
var long = 10 * short

const bucket = "mplewis-s3kv-test"
const ns = "test-ns"

var ctx = context.Background()
var client *s3.Client
var s3b s3kv.Backing

func init() {
	var err error
	var cfg aws.Config

	if os.Getenv("TEST_WITH_LIVE_S3") != "" {
		cfg, err = config.LoadDefaultConfig(ctx)
		if err != nil {
			log.Panic(err)
		}
	} else {
		resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{URL: "http://localhost:9999"}, nil
		})
		cfg, err = config.LoadDefaultConfig(
			ctx,
			config.WithEndpointResolver(resolver),
		)
		if err != nil {
			log.Panic(err)
		}
	}

	client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	s3b, err = s3kv.NewS3Backing(s3kv.S3BackingArgs{
		Bucket:    bucket,
		Namespace: ns,
		Client:    client,
	})
	if err != nil {
		log.Panic(err)
	}
}

func emptyBucket() {
	resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		log.Panic(err)
	}
	if len(resp.Contents) == 0 {
		return
	}

	objects := []types.ObjectIdentifier{}
	for _, obj := range resp.Contents {
		objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
	}
	_, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: objects},
	})
	if err != nil {
		log.Panic(err)
	}
}

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
