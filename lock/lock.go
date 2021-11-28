package main

import (
	"context"
	"fmt"
	"time"

	goredislib "github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

func main() {
	// server, err := tempredis.Start(tempredis.Config{})
	// if err != nil {
	// 	panic(err)
	// }
	// defer server.Term()

	client := goredislib.NewClient(&goredislib.Options{
		// Network: "unix", Addr: server.Socket(),
		Network: "tcp", Addr: ":6379",
	})

	pool := goredis.NewPool(client)

	rs := redsync.New(pool)

	mutex := rs.NewMutex("test-redsync", redsync.WithTries(3), redsync.WithExpiry(1*time.Second))
	ctx := context.Background()

	fmt.Println(mutex.LockContext(ctx))
	fmt.Println(mutex.LockContext(ctx))
	<-time.After(1 * time.Second)
	fmt.Println(mutex.LockContext(ctx))
	fmt.Println(mutex.UnlockContext(ctx))
	fmt.Println(mutex.UnlockContext(ctx))
}
