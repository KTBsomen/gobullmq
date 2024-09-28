package gobullmq

import (
	"context"
	"fmt"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/redisAction"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type QueueIface interface {
	eventemitter.EventEmitterIface
	Add(jobName string, jobData JobData, options ...WithOption) (Job, error)
	Pause()
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
		return job, wrapError(err, "bull Add error")
	}

	jobId, err := q.addJob(job, distOption.JobId)
	if err != nil {
		return job, wrapError(err, "bull Add error")
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
		fmt.Println("Error: ", err)
		return wrapError(err, "failed to pause or resume queue")
	}
	fmt.Println("Result: ", rs)

	return nil
}

func (q *Queue) Pause() {
	q.pause(true)
	q.Emit("paused")
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
		return "nil", err
	}

	msgPackedOpts, err := msgpack.Marshal(job.Opts)
	if err != nil {
		return "nil", err
	}

	givenJobId, err := lua.AddJob(rdb, keys, msgPackedArgs, job.Data, msgPackedOpts)
	if err != nil {
		return "nil", err
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

func (q *Queue) Ping() error {
	return redisAction.Ping(q.Client)
}

func (q *Queue) toKey(name string) string {
	return q.KeyPrefix + name
}
