package gobullmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"sync"
	"time"
)

type QueueEventsIface interface {
	On(event string, listener func(...interface{}))
	Once(event string, listener func(...interface{}))
	Emit(event string, args ...interface{})
	RemoveListener(event string, listener func(...interface{}))
	RemoveAllListeners(event string)
}

type QueueEvents struct {
	Name        string                     // Name of the queue
	Token       uuid.UUID                  // Token used to identify the queue events
	ee          *eventemitter.EventEmitter // Event emitter used to handle events occuring in worker threads/go routines/etc
	running     bool                       // Flag to indicate if the queue events is running
	closing     bool                       // Flag to indicate if the queue events is closing
	redisClient redis.Client               // Redis client used to interact with the redis server
	ctx         context.Context            // Context used to handle the queue events
	cancel      context.CancelFunc         // Cancel function used to stop the queue events
	Prefix      string
	KeyPrefix   string
	mutex       sync.Mutex     // Mutex used to lock/unlock the queue events
	wg          sync.WaitGroup // WaitGroup used to wait for the queue events to finish
	Opts        struct {
		LastEventId string // Last event id
	}
}

type QueueEventsOptions struct {
	RedisClient redis.Client // Assume we have been handled a working and valid redis con
	Autorun     bool         // If true, run the queue events immediately after creation
	Prefix      string       // Prefix for the queue events key
}

// NewQueueEvents creates a new QueueEvents instance
func NewQueueEvents(ctx context.Context, name string, opts QueueEventsOptions) *QueueEvents {
	ctx, cancel := context.WithCancel(ctx)

	qe := &QueueEvents{
		Name:        name,
		Token:       uuid.New(),
		ee:          eventemitter.NewEventEmitter(),
		running:     false,
		closing:     false,
		ctx:         ctx,
		cancel:      cancel,
		redisClient: opts.RedisClient,
	}

	if opts.Prefix == "" {
		qe.KeyPrefix = "bull"
	} else {
		qe.KeyPrefix = opts.Prefix
	}
	qe.Prefix = qe.KeyPrefix
	qe.KeyPrefix = qe.KeyPrefix + ":" + name + ":"

	// if autorun, run qe.run() and if it has any errors emit error event
	if opts.Autorun {
		err := qe.Run()
		if err != nil {
			qe.Emit("error", fmt.Sprintf("Error running queue events: %v", err))
		}
	}

	return qe
}

func (qe *QueueEvents) Emit(event string, args ...interface{}) {
	qe.ee.Emit(event, args...)
}

func (qe *QueueEvents) Off(event string, listener func(...interface{})) {
	qe.ee.RemoveListener(event, listener)
}

func (qe *QueueEvents) On(event string, listener func(...interface{})) {
	qe.ee.On(event, listener)
}

func (qe *QueueEvents) Once(event string, listener func(...interface{})) {
	qe.ee.Once(event, listener)
}

func (qe *QueueEvents) Run() error {
	qe.mutex.Lock()
	defer qe.mutex.Unlock()

	if qe.running {
		return errors.New("queue events is already running")
	}

	qe.running = true
	client := qe.redisClient

	client.Do(qe.ctx, "CLIENT", "SETNAME", fmt.Sprintf("%s:%s%s", qe.Prefix, base64.StdEncoding.EncodeToString([]byte(qe.Name)), ":qe"))

	qe.wg.Add(1)

	go func() {
		defer func() {
			qe.running = false
			qe.wg.Done()
		}()
		if err := qe.consumeEvents(client); err != nil {
			// Handle critical errors: log and possibly shut down
			qe.Emit("error", fmt.Sprintf("Critical error in consumeEvents: %v", err))
			// Optionally, you can decide to cancel the context to stop other operations
			qe.cancel()
		}
	}()

	return nil
}

func (qe *QueueEvents) consumeEvents(client redis.Client) error {
	eventKey := qe.KeyPrefix + "events"
	id := "$"
	if qe.Opts.LastEventId != "" {
		id = qe.Opts.LastEventId
	}
	for {
		select {
		case <-qe.ctx.Done():
			return nil
		default:
		}

		streams, err := client.XRead(qe.ctx, &redis.XReadArgs{
			Streams: []string{eventKey, id},
			Block:   0,
		}).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			qe.Emit("error", fmt.Sprintf("Error reading from stream: %v", err))
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				id = message.ID
				args := message.Values

				if err := qe.processEvent(args, id); err != nil {
					qe.Emit("error", fmt.Sprintf("Error processing event: %v", err))
					continue
				}
			}
		}
	}
}

func (qe *QueueEvents) processEvent(args map[string]interface{}, id string) error {
	// Extract the event name
	eventName, ok := args["event"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid 'event' field in message ID %s", id)
	}

	// Initialize event data
	var data interface{}
	var err error

	// Handle specific events that require data unmarshaling
	switch eventName {
	case "progress", "completed":
		dataKey := "data"
		if eventName == "completed" {
			dataKey = "returnvalue"
		}
		dataStr, ok := args[dataKey].(string)
		if !ok {
			return fmt.Errorf("missing or invalid '%s' field in message ID %s", dataKey, id)
		}
		// Unmarshal the JSON data
		if err = json.Unmarshal([]byte(dataStr), &data); err != nil {
			return fmt.Errorf("error unmarshaling '%s': %v", dataKey, err)
		}
		args[dataKey] = data
	}

	// Emit the event
	qe.emitEvent(eventName, args, id)
	return nil
}

func (qe *QueueEvents) emitEvent(eventName string, args map[string]interface{}, id string) {
	jobId, _ := args["jobId"].(string)

	if eventName == "drained" {
		qe.Emit(eventName, id)
	} else {
		qe.Emit(eventName, args, id)
		if jobId != "" {
			qe.Emit(fmt.Sprintf("%s:%s", eventName, jobId), args, id)
		}
	}
}

func (qe *QueueEvents) Close() {
	qe.mutex.Lock()
	defer qe.mutex.Unlock()

	if !qe.running {
		return
	}

	qe.cancel()
	qe.wg.Wait()
	qe.running = false
}
