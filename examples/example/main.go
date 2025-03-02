package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.codycody31.dev/gobullmq"
	"go.codycody31.dev/gobullmq/types"
)

func main() {
	queueName := "test"
	ctx := context.Background()

	// Initialize the queue
	queue, err := gobullmq.NewQueue(ctx, queueName, gobullmq.QueueOption{
		RedisIp:     "127.0.0.1:6379",
		RedisPasswd: "",
	})
	if err != nil {
		fmt.Println("Error initializing queue:", err)
		return
	}

	// Define the worker process function
	workerProcess := func(ctx context.Context, job *types.Job) (interface{}, error) {
		fmt.Printf("Processing job: %s\n", job.Id)
		fmt.Printf("Data: %v\n", job.Data)
		time.Sleep(5 * time.Second)
		return nil, errors.New("job failed")
	}

	// Initialize the worker
	worker, err := gobullmq.NewWorker(ctx, queueName, gobullmq.WorkerOptions{
		Concurrency:     1,
		StalledInterval: 30000,
	}, redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
	}), workerProcess)
	if err != nil {
		fmt.Println("Error initializing worker:", err)
		return
	}

	// Initialize queue events
	events, err := gobullmq.NewQueueEvents(ctx, queueName, gobullmq.QueueEventsOptions{
		RedisClient: *redis.NewClient(&redis.Options{
			Addr:     "127.0.0.1:6379",
			Password: "",
			DB:       0,
		}),
		Autorun: true,
	})
	if err != nil {
		fmt.Println("Error initializing queue events:", err)
		return
	}

	// Set up event listeners
	events.On("completed", func(args ...interface{}) {
		fmt.Println("Job completed:", args)
	})
	events.On("active", func(args ...interface{}) {
		fmt.Println("Job active:", args)
	})
	events.On("added", func(args ...interface{}) {
		fmt.Println("Job added:", args)
	})
	events.On("error", func(args ...interface{}) {
		fmt.Println("Error event:", args)
	})

	// Create job data
	jobData, err := json.Marshal(struct {
		Foo string `json:"foo"`
	}{
		Foo: "bar",
	})
	if err != nil {
		fmt.Println("Error marshaling job data:", err)
		return
	}

	// Add jobs to the queue
	for i := 0; i < 1; i++ {
		if _, err := queue.Add("test", jobData); err != nil {
			fmt.Println("Error adding job:", err)
		}
	}

	// if _, err := queue.Add("test", jobData, gobullmq.JobOptionWithRepeat(types.JobRepeatOptions{
	// 	Every: 1000,
	// })); err != nil {
	// 	fmt.Println("Error adding repeatable job:", err)
	// }

	worker.On("error", func(args ...interface{}) {
		fmt.Println("Worker error:", args)
	})

	// Run the worker
	if err := worker.Run(); err != nil {
		fmt.Println("Error running worker:", err)
	}

	worker.Wait()

	// Clean up
	worker.Close()
	events.Close()
}
