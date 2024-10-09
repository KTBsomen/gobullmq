package gobullmq

import (
	"context"
	"fmt"
	"time"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/redisAction"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type QueueEventType string

var (
	QueueEventCompleted   QueueEventType = "completed"
	QueueEventWait        QueueEventType = "wait"
	QueueEventActive      QueueEventType = "active"
	QueueEventPaused      QueueEventType = "paused"
	QueueEventPrioritized QueueEventType = "prioritized"
	QueueEventDelayed     QueueEventType = "delayed"
	QueueEventFailed      QueueEventType = "failed"
)

type QueueIface interface {
	eventemitter.EventEmitterIface
	Add(jobName string, jobData JobData, options ...WithOption) (Job, error)
	Pause() error
	Resume()
	IsPaused() bool
	Ping() error
}

var _ QueueIface = (*Queue)(nil)

const (
	SingleNode = 0
	Cluster    = 1
)

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
}

func NewQueue(name string, opts QueueOption) (*Queue, error) {
	q := &Queue{
		Name:  name,
		Token: uuid.New(),
		ctx:   context.Background(), // TODO: Take input of context
	}

	q.EventEmitter.Init()

	if opts.KeyPrefix == "" {
		q.KeyPrefix = "bull"
	} else {
		q.KeyPrefix = opts.KeyPrefix
	}
	q.Prefix = q.KeyPrefix
	q.KeyPrefix = q.KeyPrefix + ":" + name + ":"

	if name == "" {
		return nil, wrapError(nil, "Queue name must be provided'")
	}

	redisIp := opts.RedisIp
	redisPasswd := opts.RedisPasswd
	redisMode := opts.Mode
	var err error
	q.Client, err = redisAction.Init(redisIp, redisPasswd, redisMode)
	if err != nil {
		return nil, wrapError(err, "bull Init error")
	}

	return q, nil
}

func (q *Queue) Init(name string, opts QueueOption) (*Queue, error) {
	nq, err := NewQueue(name, opts)
	if err != nil {
		return nil, wrapError(err, "bull Init error")
	}
	return nq, nil
}

func (q *Queue) Add(jobName string, jobData JobData, options ...WithOption) (Job, error) {
	distOption := &JobOptions{}
	var name string

	for _, withOptionFunc := range options {
		withOptionFunc(distOption)
	}

	if distOption.JobId != "" {
		if distOption.JobId == "0" || (distOption.JobId[0] == '0' && distOption.JobId[1] != ':') {
			return Job{}, wrapError(nil, "JobId cannot be '0' or start with 0:")
		}
	}

	// TODO: setup this.jobsOpts for the default base options configured

	if jobName == "" {
		name = _DEFAULT_JOB_NAME
	} else {
		name = jobName
	}

	job, err := newJob(name, jobData, *distOption)
	if err != nil {
		return job, wrapError(err, "attempt to create new job failed")
	}
	jobId, err := q.addJob(job, distOption.JobId)
	if err != nil {
		return job, wrapError(err, "failed to add job")
	}
	job.Id = jobId

	q.Emit("waiting", job)

	return job, nil
}

func (q *Queue) pause(pause bool) error {
	client := q.Client
	p := "paused"

	// Determine the source and destination queues based on whether to pause or resume
	src := "wait"
	dst := "paused"
	if !pause {
		src = "paused"
		dst = "wait"
		p = "resumed"
	}

	// Check if the source queue exists
	exists, err := client.Exists(context.Background(), q.toKey(src)).Result()
	if err != nil {
		return wrapError(err, "failed to check if queue exists")
	}

	if exists == 0 {
		// If the queue doesn't exist, there's no need to rename it
		return wrapError(nil, "source queue does not exist, nothing to pause or resume")
	}

	// Define the keys to operate on
	keys := []string{
		q.toKey(src),
		q.toKey(dst),
		q.toKey("meta"),
		q.toKey("prioritized"),
		q.toKey("events"),
	}

	rs, err := lua.Pause(client, keys, p)
	if err != nil {
		return wrapError(err, "failed to pause or resume queue")
	}
	fmt.Println("Result: ", rs)

	return nil
}

func (q *Queue) Pause() error {
	err := q.pause(true)
	if err != nil {
		return wrapError(err, "failed to pause queue")
	}
	q.Emit("paused")
	return nil
}

func (q *Queue) Resume() {
	q.pause(false)
	q.Emit("resumed")
}

func (q *Queue) IsPaused() bool {
	client := q.Client
	pausedKeyExists, _ := client.HExists(context.Background(), q.KeyPrefix+"meta", "paused").Result()
	return pausedKeyExists
}

func (q *Queue) addJob(job Job, jobId string) (string, error) {
	rdb := q.Client

	keys := make([]string, 0, 8)
	keys = append(keys, q.KeyPrefix+"wait")
	keys = append(keys, q.KeyPrefix+"paused")
	keys = append(keys, q.KeyPrefix+"meta")
	keys = append(keys, q.KeyPrefix+"id")
	keys = append(keys, q.KeyPrefix+"delayed")
	keys = append(keys, q.KeyPrefix+"prioritized")
	keys = append(keys, q.KeyPrefix+"completed")
	keys = append(keys, q.KeyPrefix+"events")
	keys = append(keys, q.KeyPrefix+"pc")

	// args := q.getArgs(job)
	args := make([]interface{}, 0)
	args = append(args, q.KeyPrefix)
	args = append(args, jobId)
	args = append(args, job.Name)
	args = append(args, job.TimeStamp)
	for i := 0; i < 5; i++ {
		args = append(args, nil)
	}

	msgPackedArgs, err := msgpack.Marshal(args)
	if err != nil {
		return "nil", fmt.Errorf("failed to marshal args: %v", err)
	}

	msgPackedOpts, err := msgpack.Marshal(job.Opts)
	if err != nil {
		return "nil", fmt.Errorf("failed to marshal opts: %v", err)
	}

	givenJobId, err := lua.AddJob(rdb, keys, msgPackedArgs, job.Data, msgPackedOpts)
	if err != nil {
		return "nil", fmt.Errorf("failed to add job: %v", err)
	}

	jobIdStr := givenJobId.(string)

	return jobIdStr, nil
}

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
		return wrapError(err, "failed to drain queue")
	}
	return nil
}

// Clean cleans jobs from the queue (limit is the max number of jobs to clean, 0 is unlimited)
func (q *Queue) Clean(grace int, limit int, cType QueueEventType) ([]string, error) {
	var jobs []string

	set := cType
	timestamp := time.Now().Unix() - int64(grace)

	keys := []string{
		q.toKey(string(set)),
		q.toKey("events"),
	}

	i, err := lua.CleanJobsInSet(q.Client, keys, q.KeyPrefix, timestamp, limit, string(set))
	if err != nil {
		return jobs, wrapError(err, "failed to clean jobs")
	}

	jobs = i.([]string)

	q.Emit("cleaned", jobs, string(set))
	return jobs, nil
}

type ObliterateOpts struct {
	Force bool // Use force = true to force obliteration even with active jobs in the queue (default: false)
	Count int  // Use count with the maximum number of deleted keys per iteration (default: 1000)
}

func (q *Queue) Obliterate(opts ObliterateOpts) error {
	err := q.pause(true)
	if err != nil {
		return wrapError(err, "failed to pause queue")
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
			return wrapError(err, "failed to obliterate queue")
		}

		// -1: Queue is not paused
		// -2: Queue has active jobs
		// 0: Queue obliterated completely
		// 1: Queue obliterated partially
		result := i.(int64)

		if result < 0 {
			switch result {
			case -1:
				return wrapError(nil, "cannot obliterate non-paused queue")
			case -2:
				return wrapError(nil, "cannot obliterate queue with active jobs")
			}
		} else if result == 0 {
			break
		}
	}

	return nil
}

func (q *Queue) Ping() error {
	return redisAction.Ping(q.Client)
}

func (q *Queue) toKey(name string) string {
	return q.KeyPrefix + name
}

func (q *Queue) Remove(jobId string, removeChildren bool) error {
	keys := []string{
		q.KeyPrefix,
	}

	i, err := lua.RemoveJob(q.Client, keys, jobId, removeChildren)
	if err != nil {
		return wrapError(err, fmt.Sprintf("failed to remove job: %s", jobId))
	}

	if i.(int64) == 0 {
		return wrapError(err, fmt.Sprintf("failed to remove job: %s, the job is locked", jobId))
	}

	return nil
}

func (q *Queue) TrimEvents(max int64) (int64, error) {
	return q.Client.XTrim(q.ctx, q.KeyPrefix+"events", max).Result()
}
