package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"go.codycody31.dev/gobullmq"
)

var ctx = context.Background()

func main() {
	var q *gobullmq.Queue
	var queueName string
	var qEvents *gobullmq.QueueEvents

	queueName = "test"
	q, _ = gobullmq.NewQueue(queueName, gobullmq.QueueOption{
		RedisIp:     "127.0.0.1:6379",
		RedisPasswd: "",
	})
	qEvents = gobullmq.NewQueueEvents(context.Background(), queueName, gobullmq.QueueEventsOptions{
		RedisClient: *redis.NewClient(&redis.Options{
			Addr:     "127.0.0.1:6379",
			Password: "",
			DB:       0,
		}),
	})

	go func() {
		err := qEvents.Run()
		if err != nil {
			fmt.Println(err)
		}
	}()

	qEvents.On("added", func(args ...interface{}) {
		fmt.Println("added")
		fmt.Println(args)
	})

	jobdata, err := json.Marshal(
		struct {
			Foo string `json:"foo"`
		}{
			Foo: "bar",
		})
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = q.Add("test", jobdata)
	if err != nil {
		println(err.Error())
	}
	_, err = q.Add("test", jobdata)
	if err != nil {
		println(err.Error())
	}

	select {}

	qEvents.Close()
}
