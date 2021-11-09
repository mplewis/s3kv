package s3kv_test

import (
	"log"
	"testing"

	"github.com/mplewis/s3kv"
)

var s = s3kv.New(s3kv.S3kvArgs{Bucket: bucket, Session: sess})

func BenchmarkGet(b *testing.B) {
	keys, done, err := s.Lock("foo")
	if err != nil {
		log.Panic(err)
	}
	keys["foo"].Set([]byte("bar"))
	done()

	for i := 0; i < b.N; i++ {
		keys, done, err := s.Lock("foo")
		if err != nil {
			log.Panic(err)
		}
		keys["foo"].Get()
		done()
	}
}

func BenchmarkSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		keys, done, err := s.Lock("foo")
		if err != nil {
			log.Panic(err)
		}
		keys["foo"].Set([]byte("bar"))
		done()
	}
}

func BenchmarkGetSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		keys, done, err := s.Lock("foo")
		if err != nil {
			log.Panic(err)
		}
		keys["foo"].Get()
		keys["foo"].Set([]byte("bar"))
		done()
	}
}

func BenchmarkGetSetDel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		keys, done, err := s.Lock("foo")
		if err != nil {
			log.Panic(err)
		}
		keys["foo"].Get()
		keys["foo"].Set([]byte("bar"))
		keys["foo"].Del()
		done()
	}
}