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
)

type QueueEventsIface interface {
	On(event string, listener func(...interface{}))
	Once(event string, listener func(...interface{}))
	Emit(event string, args ...interface{})
	RemoveListener(event string, listener func(...interface{}))
	RemoveAllListeners(event string)
}

type QueueEvents struct {
	Name        string
	Token       uuid.UUID
	ee          *eventemitter.EventEmitter // Event emitter used to handle events occuring in worker threads/go routines/etc
	running     bool
	closing     bool
	redisClient redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
	Prefix      string
	KeyPrefix   string
	mutex       sync.Mutex
	wg          sync.WaitGroup
	Opts        struct {
		LastEventId string
	}
}

type QueueEventsOptions struct {
	RedisClient redis.Client // Assume we have been handled a working and valid redis con
	Autorun     bool
	Prefix      string
}

// TODO: Define a context, so if we need to shutdown the queue we can use the context to stop the queue
// And do it safely

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

	// Set the name of the client connection to the queue name
	client.Do(qe.ctx, "CLIENT", "SETNAME", fmt.Sprintf("%s:%s%s", qe.Prefix, base64.StdEncoding.EncodeToString([]byte(qe.Name)), ":qe"))

	qe.wg.Add(1) // Add to WaitGroup

	// Use a goroutine to run the async task.
	go func() {
		defer func() {
			qe.running = false
			qe.wg.Done() // Signal WaitGroup when done
		}()
		if err := qe.consumeEvents(client); err != nil {
			qe.Emit("error", fmt.Sprintf("Error consuming events: %v", err))
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
		if qe.closing {
			break
		}

		// https://www.dragonflydb.io/code-examples/golang-redis-xread
		streams, err := client.XRead(qe.ctx, &redis.XReadArgs{
			Streams: []string{eventKey, id},
			Block:   0,
		}).Result()

		// TODO: Think about how error handling needs to be done, if we need to return an error or just continue

		if errors.Is(err, redis.Nil) {
			continue
		} else if errors.Is(err, context.Canceled) {
			return nil
		} else if err != nil {
			// Log or handle error based on your needs
			return err
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				id := message.ID
				args := message.Values

				type event struct {
					Event string `json:"event"`
					JobId string `json:"jobId"`
					Name  string `json:"name"`
				}

				var e event

				for k, v := range args {
					switch k {
					case "event":
						e.Event = v.(string)
					case "jobId":
						e.JobId = v.(string)
					case "name":
						e.Name = v.(string)
					}
				}

				switch e.Event {
				case "progress":
					// Add to  map[string]interface{} key data
					err = json.Unmarshal([]byte(args["data"].(string)), args["data"])
					if err != nil {
						return err
					}
				case "completed":
					// Add to  map[string]interface{} key returnvalue
					err = json.Unmarshal([]byte(args["returnvalue"].(string)), args["returnvalue"])
					if err != nil {
						return err
					}
				}

				// restArgs, is just args but without the event key
				restArgs := args
				delete(restArgs, "event")

				if e.Event == "drained" {
					qe.Emit(e.Event, id)
				} else {
					qe.Emit(e.Event, restArgs, id)
					qe.Emit(e.Event+":"+e.JobId, restArgs, id)
				}
			}
		}
	}

	return nil
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
