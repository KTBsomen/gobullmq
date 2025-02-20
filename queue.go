package gobullmq

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v5"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/internal/utils/repeat"
	"go.codycody31.dev/gobullmq/types"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/redisAction"

	"github.com/google/uuid"
)

// QueueIface defines the interface for a job queue with various operations.
type QueueIface interface {
	// EventEmitterIface is embedded to provide event handling capabilities.
	eventemitter.EventEmitterIface
	// Init initializes a new queue with the given context, name, and options.
	Init(ctx context.Context, name string, opts QueueOption) (*Queue, error)
	// Add adds a new job to the queue with the specified name, data, and options.
	Add(jobName string, jobData types.JobData, options ...types.JobOptionFunc) (types.Job, error)
	// Pause pauses the queue, preventing new jobs from being processed.
	Pause() error
	// Resume resumes the queue, allowing jobs to be processed.
	Resume() error
	// IsPaused checks if the queue is currently paused.
	IsPaused() bool
	// Drain removes all jobs from the queue, optionally including delayed jobs.
	Drain(delayed bool) error
	// Clean removes completed jobs from the queue based on the specified criteria.
	Clean(grace int, limit int, cType types.QueueEventType) ([]string, error)
	// Obliterate completely removes the queue and its data.
	Obliterate(opts ObliterateOpts) error
	// Ping checks the connection to the Redis server.
	Ping() error
	// Remove removes a job from the queue by its ID.
	Remove(jobId string, removeChildren bool) error
	// TrimEvents trims the event stream to the specified maximum length.
	TrimEvents(max int64) (int64, error)
}

var _ QueueIface = (*Queue)(nil)

// Queue represents a job queue with various operations.
type Queue struct {
	eventemitter.EventEmitter
	Name      string
	Token     uuid.UUID
	KeyPrefix string
	Client    redis.Cmdable
	Prefix    string
	ctx       context.Context
}

type QueueOption struct {
	Mode        int
	KeyPrefix   string
	RedisIp     string
	RedisPasswd string

	// Options for the streams used internally in BullMQ.
	Streams struct {
		// Options for the events stream.
		Events struct {
			MaxLen int64 // Max approximated length for streams. Default is 10 000 events.
		}
	}
}

type QueueJob struct {
	Name string
	Data types.JobData
	Opts types.JobOptions
}

// NewQueue creates a new Queue instance with the specified context, name, and options.
func NewQueue(ctx context.Context, name string, opts QueueOption) (*Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("queue name must be provided")
	}

	q := &Queue{
		Name:  name,
		Token: uuid.New(),
		ctx:   ctx,
	}

	q.EventEmitter.Init()

	if opts.KeyPrefix == "" {
		q.KeyPrefix = "bull"
	} else {
		q.KeyPrefix = opts.KeyPrefix
	}
	q.Prefix = q.KeyPrefix
	q.KeyPrefix = q.KeyPrefix + ":" + name + ":"

	redisIp := opts.RedisIp
	redisPasswd := opts.RedisPasswd
	redisMode := opts.Mode
	var err error
	q.Client, err = redisAction.Init(redisIp, redisPasswd, redisMode)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Redis client: %w", err)
	}

	if opts.Streams.Events.MaxLen != 0 {
		q.Client.XTrim(q.ctx, q.toKey("events"), opts.Streams.Events.MaxLen)
	} else {
		q.Client.HSet(q.ctx, q.toKey("meta"), "opts.maxLenEvents", "10000")
	}

	return q, nil
}

// Init initializes a new Queue instance with the specified context, name, and options.
func (q *Queue) Init(ctx context.Context, name string, opts QueueOption) (*Queue, error) {
	return NewQueue(ctx, name, opts)
}

// Add adds a new job to the queue with the specified name, data, and options.
func (q *Queue) Add(jobName string, jobData types.JobData, options ...types.JobOptionFunc) (types.Job, error) {
	distOption := &types.JobOptions{}
	var name string

	for _, optionFunc := range options {
		optionFunc(distOption)
	}

	if distOption.JobId != "" {
		if distOption.JobId == "0" || (distOption.JobId[0] == '0' && distOption.JobId[1] != ':') {
			return types.Job{}, fmt.Errorf("jobId cannot be '0' or start with 0:")
		}
	}

	if jobName == "" {
		name = _DEFAULT_JOB_NAME
	} else {
		name = jobName
	}

	if distOption.Repeat.Every != 0 || distOption.Repeat.Pattern != "" {
		return q.addRepeatableJob(name, jobData, *distOption, true)
	}

	job, err := newJob(name, jobData, *distOption)
	if err != nil {
		return job, fmt.Errorf("failed to create new job: %w", err)
	}
	jobId, err := q.addJob(job, distOption.JobId)
	if err != nil {
		return job, fmt.Errorf("failed to add job: %w", err)
	}
	job.Id = jobId

	q.Emit("waiting", job)

	return job, nil
}

// pause pauses or resumes the queue based on the provided flag.
func (q *Queue) pause(pause bool) error {
	client := q.Client
	p := "paused"

	src := "wait"
	dst := "paused"
	if !pause {
		src = "paused"
		dst = "wait"
		p = "resumed"
	}

	exists, err := client.Exists(context.Background(), q.toKey(src)).Result()
	if err != nil {
		return fmt.Errorf("failed to check if queue exists: %w", err)
	}

	if exists == 0 {
		return fmt.Errorf("source queue does not exist, nothing to pause or resume")
	}

	keys := []string{
		q.toKey(src),
		q.toKey(dst),
		q.toKey("meta"),
		q.toKey("prioritized"),
		q.toKey("events"),
	}

	rs, err := lua.Pause(client, keys, p)
	if err != nil {
		return fmt.Errorf("failed to pause or resume queue: %w", err)
	}
	fmt.Println("Result: ", rs)

	return nil
}

// Pause pauses the queue, preventing new jobs from being processed.
func (q *Queue) Pause() error {
	if err := q.pause(true); err != nil {
		return fmt.Errorf("failed to pause queue: %w", err)
	}
	q.Emit("paused")
	return nil
}

// Resume resumes the queue, allowing jobs to be processed.
func (q *Queue) Resume() error {
	if err := q.pause(false); err != nil {
		return fmt.Errorf("failed to resume queue: %w", err)
	}
	q.Emit("resumed")
	return nil
}

// IsPaused checks if the queue is currently paused.
func (q *Queue) IsPaused() bool {
	pausedKeyExists, _ := q.Client.HExists(context.Background(), q.KeyPrefix+"meta", "paused").Result()
	return pausedKeyExists
}

// addJob adds a job to the queue with the specified job ID.
func (q *Queue) addJob(job types.Job, jobId string) (string, error) {
	rdb := q.Client

	keys := []string{
		q.KeyPrefix + "wait",
		q.KeyPrefix + "paused",
		q.KeyPrefix + "meta",
		q.KeyPrefix + "id",
		q.KeyPrefix + "delayed",
		q.KeyPrefix + "prioritized",
		q.KeyPrefix + "completed",
		q.KeyPrefix + "events",
		q.KeyPrefix + "pc",
	}

	args := []interface{}{
		q.KeyPrefix,
		jobId,
		job.Name,
		job.TimeStamp,
		nil, nil, nil, nil,
		job.Opts.RepeatJobKey,
	}

	msgPackedArgs, err := msgpack.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("failed to marshal args: %w", err)
	}

	msgPackedOpts, err := msgpack.Marshal(job.Opts)
	if err != nil {
		return "", fmt.Errorf("failed to marshal opts: %w", err)
	}

	givenJobId, err := lua.AddJob(rdb, keys, msgPackedArgs, job.Data, msgPackedOpts)
	if err != nil {
		return "", fmt.Errorf("failed to add job: %w", err)
	}

	return givenJobId.(string), nil
}

// addRepeatableJob adds a repeatable job to the queue with the specified options.
func (q *Queue) addRepeatableJob(name string, jobData types.JobData, opts types.JobOptions, skipCheckExists bool) (types.Job, error) {
	prevMillis := opts.Repeat.PrevMillis
	currentCount := opts.Repeat.Count

	if currentCount == 0 {
		currentCount = 1
	} else {
		currentCount++
	}

	if limit := opts.Repeat.Limit; limit > 0 && currentCount > limit {
		return types.Job{}, fmt.Errorf("repeatable job limit exceeded")
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)

	if opts.Repeat.EndDate != nil && now > opts.Repeat.EndDate.UnixNano()/int64(time.Millisecond) {
		return types.Job{}, fmt.Errorf("repeatable job end date exceeded")
	}

	if int64(prevMillis) > now {
		now = int64(prevMillis)
	}

	nextMillis, err := repeat.Strategy(now, opts)
	if err != nil {
		return types.Job{}, fmt.Errorf("failed to calculate nextMillis: %w", err)
	}

	pattern := opts.Repeat.Pattern
	hasImmediately := false

	if (opts.Repeat.Every != 0 || pattern != "") && opts.Repeat.Immediately {
		hasImmediately = true
	}

	var offset int64
	if hasImmediately {
		offset = now - nextMillis
	} else {
		offset = 0
	}

	if nextMillis > 0 {
		if prevMillis != 0 && opts.JobId != "" {
			opts.Repeat.JobId = opts.JobId
		}

		repeatJobKey := repeat.GetKey(name, opts.Repeat)

		repeatableExists := true

		if !skipCheckExists {
			f, err := q.Client.ZScore(q.ctx, q.toKey("repeat"), repeatJobKey).Result()
			if err != nil || f == 0 {
				repeatableExists = false
			}
		}

		if repeatableExists {
			jobId, err := repeat.GetJobId(name, nextMillis, utils.MD5Hash(repeatJobKey), opts.JobId)
			if err != nil {
				return types.Job{}, fmt.Errorf("failed to get repeatable job id: %w", err)
			}

			now := time.Now().UnixNano() / int64(time.Millisecond)
			delay := nextMillis + offset - now

			opts.JobId = jobId
			opts.Delay = int(delay)
			opts.TimeStamp = now
			opts.Repeat.PrevMillis = int(nextMillis)
			opts.RepeatJobKey = repeatJobKey
			opts.Repeat.Count = currentCount

			q.Client.ZAdd(q.ctx, q.toKey("repeat"), &redis.Z{
				Score:  float64(nextMillis),
				Member: repeatJobKey,
			})

			job, err := newJob(name, jobData, opts)
			if err != nil {
				return job, fmt.Errorf("failed to create new job: %w", err)
			}
			_, err = q.addJob(job, jobId)
			if err != nil {
				return job, fmt.Errorf("failed to add repeatable job: %w", err)
			}

			q.Emit("waiting", job)

			return job, nil
		} else {
			return types.Job{}, fmt.Errorf("repeatable job does not exist")
		}
	} else {
		return types.Job{}, fmt.Errorf("repeatable job nextMillis is invalid")
	}
}

// Drain removes all jobs from the queue, optionally including delayed jobs.
func (q *Queue) Drain(delayed bool) error {
	keys := []string{
		q.toKey("wait"),
		q.toKey("paused"),
	}

	if delayed {
		keys = append(keys, q.toKey("delayed"))
	} else {
		keys = append(keys, "")
	}
	keys = append(keys, q.toKey("prioritized"))

	_, err := lua.Drain(q.Client, keys, q.KeyPrefix)
	if err != nil {
		return fmt.Errorf("failed to drain queue: %w", err)
	}
	return nil
}

// Clean removes completed jobs from the queue based on the specified criteria.
func (q *Queue) Clean(grace int, limit int, cType types.QueueEventType) ([]string, error) {
	var jobs []string

	set := cType
	timestamp := time.Now().Unix() - int64(grace)

	keys := []string{
		q.toKey(string(set)),
		q.toKey("events"),
	}

	i, err := lua.CleanJobsInSet(q.Client, keys, q.KeyPrefix, timestamp, limit, string(set))
	if err != nil {
		return jobs, fmt.Errorf("failed to clean jobs: %w", err)
	}

	jobs = i.([]string)

	q.Emit("cleaned", jobs, string(set))
	return jobs, nil
}

type ObliterateOpts struct {
	Force bool // Use force = true to force obliteration even with active jobs in the queue (default: false)
	Count int  // Use count with the maximum number of deleted keys per iteration (default: 1000)
}

// Obliterate completely removes the queue and its data.
func (q *Queue) Obliterate(opts ObliterateOpts) error {
	if err := q.pause(true); err != nil {
		return fmt.Errorf("failed to pause queue: %w", err)
	}

	var force string
	if opts.Force {
		force = "force"
	}

	count := opts.Count
	if count == 0 {
		count = 1000
	}

	keys := []string{
		q.toKey("meta"),
		q.KeyPrefix,
	}

	for {
		i, err := lua.Obliterate(q.Client, keys, count, force)
		if err != nil {
			return fmt.Errorf("failed to obliterate queue: %w", err)
		}

		result := i.(int64)

		if result < 0 {
			switch result {
			case -1:
				return fmt.Errorf("cannot obliterate non-paused queue")
			case -2:
				return fmt.Errorf("cannot obliterate queue with active jobs")
			}
		} else if result == 0 {
			break
		}
	}

	return nil
}

// Ping checks the connection to the Redis server.
func (q *Queue) Ping() error {
	return redisAction.Ping(q.Client)
}

// toKey constructs a Redis key with the queue's prefix.
func (q *Queue) toKey(name string) string {
	return q.KeyPrefix + name
}

// Remove removes a job from the queue by its ID.
func (q *Queue) Remove(jobId string, removeChildren bool) error {
	keys := []string{
		q.KeyPrefix,
	}

	i, err := lua.RemoveJob(q.Client, keys, jobId, removeChildren)
	if err != nil {
		return fmt.Errorf("failed to remove job: %w", err)
	}

	if i.(int64) == 0 {
		return fmt.Errorf("failed to remove job: %s, the job is locked", jobId)
	}

	return nil
}

// TrimEvents trims the event stream to the specified maximum length.
func (q *Queue) TrimEvents(max int64) (int64, error) {
	return q.Client.XTrim(q.ctx, q.KeyPrefix+"events", max).Result()
}
