package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/sbermarket-tech/work/webui"
)

var (
	redisDSN       = flag.String("redis", "redis://0.0.0.0:6379", "redis DSN")
	redisDatabase  = flag.String("database", "0", "redis database")
	redisNamespace = flag.String("ns", "work", "redis namespace")
	webHostPort    = flag.String("listen", ":5040", "hostport to listen for HTTP JSON API")
)

func main() {
	flag.Parse()

	redisURL, err := url.Parse(*redisDSN)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	database, err := strconv.Atoi(*redisDatabase)
	if err != nil {
		fmt.Printf("Error: %v is not a valid database value\n", *redisDatabase)
		os.Exit(1)
	}

	fmt.Println("Starting workwebui:")
	fmt.Println("redis = ", redisURL.Redacted())
	fmt.Println("database = ", *redisDatabase)
	fmt.Println("namespace = ", *redisNamespace)
	fmt.Println("listen = ", *webHostPort)

	pool := newPool(*redisDSN, database)

	server := webui.NewServer(*redisNamespace, pool, *webHostPort)
	server.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	<-c

	server.Stop()

	fmt.Println("\nQuitting...")
}

func newPool(addr string, database int) *redis.Pool {
	return &redis.Pool{
		MaxActive:   3,
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(addr, redis.DialDatabase(database))
		},
		Wait: true,
	}
}
