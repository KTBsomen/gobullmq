package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"go.codycody31.dev/gobullmq"
	"go.codycody31.dev/gobullmq/types"
)

func main() {
	var q *gobullmq.Queue
	var queueName string
	var qEvents *gobullmq.QueueEvents
	var qWorker *gobullmq.Worker
	var err error

	qWorkerProcess := func(ctx context.Context, job *types.Job) (interface{}, error) {
		fmt.Printf("Received job%s\n\tID: %s\n\tData: %s\n", job.Name, job.Id, job.Data)
		return nil, nil
	}

	queueName = "test"
	q, _ = gobullmq.NewQueue(context.Background(), queueName, gobullmq.QueueOption{
		RedisIp:     "127.0.0.1:6379",
		RedisPasswd: "",
	})
	qWorker, err = gobullmq.NewWorker(context.Background(), queueName, gobullmq.WorkerOptions{
		Concurrency:     1,
		StalledInterval: 30000,
	}, redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
	}), qWorkerProcess)
	if err != nil {
		fmt.Println(err)
		return
	}
	qEvents, err = gobullmq.NewQueueEvents(context.Background(), queueName, gobullmq.QueueEventsOptions{
		RedisClient: *redis.NewClient(&redis.Options{
			Addr:     "127.0.0.1:6379",
			Password: "",
			DB:       0,
		}),
		Autorun: true,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	qEvents.On("added", func(args ...interface{}) {
		fmt.Println("Added event")
		fmt.Println(args)
	})

	qEvents.On("error", func(args ...interface{}) {
		fmt.Println("Error event")
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

	//err = q.Remove(j.Id, true)
	//if err != nil {
	//	println(err.Error())
	//}

	_, err = q.Add("test", jobdata)
	if err != nil {
		println(err.Error())
	}

	_, err = q.Add("test", jobdata, gobullmq.QueueWithRepeat(types.JobRepeatOptions{
		Every: 1000,
	}))
	if err != nil {
		println(err.Error())
	}

	err = qWorker.Run()
	if err != nil {
		fmt.Printf("error running worker: %v\n", err)
	}
	qWorker.Wait()

	qWorker.Close()
	qEvents.Close()
}
