package queue_test

import (
	"context"
	"github.com/go-redis/redis/v8"
	"testing"

	"github.com/google/uuid"
	"go.codycody31.dev/gobullmq"
)

var ctx = context.Background()

func TestPauseQueue(t *testing.T) {
	var q *gobullmq.Queue
	var queueName string
	var qEvents *gobullmq.QueueEvents

	beforeEach := func() {
		queueName = "test-" + uuid.New().String()
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
	}

	afterEach := func() {
		// q.Close()
		qEvents.Close()
		//utils.RemoveAllQueueData(connection, queueName)
	}

	//t.Run("should not process delayed jobs", func(t *testing.T) {
	//	beforeEach()
	//	defer afterEach()
	//
	//	processed := false
	//
	//	worker := queue.NewWorker(queueName, func(j *queue.Job) error {
	//		processed = true
	//		return nil
	//	}, connection)
	//	worker.WaitUntilReady()
	//
	//	q.Pause()
	//	q.Add("test", map[string]interface{}{}, queue.JobOptions{Delay: 300})
	//
	//	counts := q.GetJobCounts("waiting", "delayed")
	//	assert.Equal(t, 0, counts["waiting"])
	//	assert.Equal(t, 1, counts["delayed"])
	//
	//	time.Sleep(500 * time.Millisecond)
	//	if processed {
	//		t.Error("should not process delayed jobs in paused queue.")
	//	}
	//
	//	counts2 := q.GetJobCounts("waiting", "paused", "delayed")
	//	assert.Equal(t, 0, counts2["waiting"])
	//	assert.Equal(t, 0, counts2["paused"])
	//	assert.Equal(t, 1, counts2["delayed"])
	//
	//	worker.Close()
	//})
	//
	t.Run("should pause queue until resumed", func(t *testing.T) {
		beforeEach()
		defer afterEach()

		//isPaused := false
		//counter := 2
		//processPromise := make(chan bool)

		//worker := queue.NewWorker(queueName, func(j *queue.Job) error {
		//	assert.False(t, isPaused)
		//	assert.Equal(t, "paused", j.Data["foo"])
		//	counter--
		//	if counter == 0 {
		//		close(processPromise)
		//	}
		//	return nil
		//}, connection)
		//worker.WaitUntilReady()

		q.Pause()
		//isPaused = true
		q.Add("test", map[string]interface{}{"foo": "paused"})
		q.Add("test", map[string]interface{}{"foo": "paused"})
		//isPaused = false
		q.Resume()

		//<-processPromise
		//worker.Close()
	})
	//
	//t.Run("should emit relevant events on pause and resume", func(t *testing.T) {
	//	beforeEach()
	//	defer afterEach()
	//
	//	isPaused, isResumed := false, true
	//	first := true
	//	processPromise := make(chan bool)
	//
	//	worker := queue.NewWorker(queueName, func(j *queue.Job) error {
	//		if first {
	//			first = false
	//			isPaused = true
	//			q.Pause()
	//			return nil
	//		} else {
	//			assert.True(t, isResumed)
	//			q.Close()
	//			close(processPromise)
	//		}
	//		return nil
	//	}, connection)
	//
	//	qEvents.OnPaused(func(args interface{}) {
	//		isPaused = false
	//		q.Resume()
	//	})
	//
	//	qEvents.OnResumed(func(args interface{}) {
	//		isResumed = true
	//	})
	//
	//	q.Add("test", map[string]interface{}{"foo": "paused"})
	//	q.Add("test", map[string]interface{}{"foo": "paused"})
	//
	//	<-processPromise
	//})
	//
	//t.Run("should wait for active jobs to finish before pausing", func(t *testing.T) {
	//	beforeEach()
	//	defer afterEach()
	//
	//	startProcessing := make(chan bool)
	//
	//	worker := queue.NewWorker(queueName, func(j *queue.Job) error {
	//		startProcessing <- true
	//		time.Sleep(1 * time.Second)
	//		return nil
	//	}, connection)
	//
	//	worker.WaitUntilReady()
	//
	//	for i := 0; i < 10; i++ {
	//		q.Add("test", i)
	//	}
	//
	//	<-startProcessing
	//	q.Pause()
	//
	//	active := q.GetJobCountByTypes("active")
	//	assert.Equal(t, 0, active)
	//
	//	paused := q.GetJobCountByTypes("paused", "waiting", "delayed")
	//	assert.Equal(t, 9, paused)
	//
	//	worker.Close()
	//})
	//
	//t.Run("should pause locally with multiple workers", func(t *testing.T) {
	//	beforeEach()
	//	defer afterEach()
	//
	//	startProcessing1 := make(chan bool)
	//	startProcessing2 := make(chan bool)
	//
	//	worker1 := queue.NewWorker(queueName, func(j *queue.Job) error {
	//		startProcessing1 <- true
	//		time.Sleep(200 * time.Millisecond)
	//		return nil
	//	}, connection)
	//
	//	worker2 := queue.NewWorker(queueName, func(j *queue.Job) error {
	//		startProcessing2 <- true
	//		time.Sleep(200 * time.Millisecond)
	//		return nil
	//	}, connection)
	//
	//	worker1.WaitUntilReady()
	//	worker2.WaitUntilReady()
	//
	//	q.Add("test", 1)
	//	q.Add("test", 2)
	//	q.Add("test", 3)
	//	q.Add("test", 4)
	//
	//	<-startProcessing1
	//	<-startProcessing2
	//
	//	worker1.Pause()
	//	worker2.Pause()
	//
	//	counts := q.GetJobCounts("active", "waiting", "completed")
	//	assert.Equal(t, 0, counts["active"])
	//	assert.Equal(t, 2, counts["waiting"])
	//	assert.Equal(t, 2, counts["completed"])
	//
	//	worker1.Close()
	//	worker2.Close()
	//})
}
