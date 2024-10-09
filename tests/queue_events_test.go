package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"go.codycody31.dev/gobullmq"
	"go.codycody31.dev/gobullmq/internal/testutils"
	"testing"
	"time"
)

func TestQueueEvents_WaitingEvent(t *testing.T) {
	queueName := "test-" + uuid.New().String()
	q, err := gobullmq.NewQueue(queueName, gobullmq.QueueOption{
		RedisIp:     "127.0.0.1:6379",
		RedisPasswd: "",
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})

	qEvents, err := gobullmq.NewQueueEvents(context.Background(), queueName, gobullmq.QueueEventsOptions{
		RedisClient: *redisClient,
		Autorun:     false,
	})
	if err != nil {
		t.Fatalf("Failed to create queue events: %v", err)
	}
	defer qEvents.Close()

	// Set up the waiting channel
	waiting := make(chan struct{})

	// Register the 'waiting' event listener
	qEvents.On("waiting", func(args ...interface{}) {
		close(waiting)
	})

	// Start the queue events processing
	if err := qEvents.Run(); err != nil {
		t.Fatalf("Failed to run queue events: %v", err)
	}

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

	// Add a job to the queue
	_, err = q.Add("test", jobdata)
	if err != nil {
		t.Fatalf("Failed to add job to queue: %v", err)
	}

	// Wait for the 'waiting' event or timeout
	select {
	case <-waiting:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for 'waiting' event")
	}

	qEvents.Close()

	// Clean up
	if err := testutils.RemoveAllQueueData(redisClient, q.KeyPrefix); err != nil {
		t.Errorf("Failed to remove all queue data: %v", err)
	}
}
