/**
 * @Description: data struct and operations with queue
 * @FilePath: /bull-golang/queue.go
 * @Author: liyibing liyibing@lixiang.com
 * @Date: 2023-07-19 15:55:49
 */
package gobullmq

import (
	"context"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/luaScripts"
	"go.codycody31.dev/gobullmq/internal/redisAction"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type QueueIface interface {
	eventemitter.EventEmitterIface
	Add(jobName string, jobData JobData, options ...withOption) (Job, error)
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
	QueueName   string
	RedisIp     string
	RedisPasswd string
}

func NewQueue(opts QueueOption) (*Queue, error) {
	q := &Queue{
		Name:  opts.QueueName,
		Token: uuid.New(),
	}

	q.EventEmitter.Init()

	// Generate go files based of the lua scripts, internal/luaScripts, rather than redisAction.ExecLua
	// As it closer to the way the original bull is implemented

	if opts.KeyPrefix == "" {
		q.KeyPrefix = "bull"
	} else {
		q.KeyPrefix = opts.KeyPrefix
	}
	q.Prefix = q.KeyPrefix
	q.KeyPrefix = q.KeyPrefix + ":" + opts.QueueName + ":"

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

func (q *Queue) Init(opts QueueOption) error {
	q.Name = opts.QueueName
	q.Token = uuid.New()

	q.EventEmitter.Init()

	if opts.KeyPrefix == "" {
		q.KeyPrefix = "bull"
	} else {
		q.KeyPrefix = opts.KeyPrefix
	}
	q.Prefix = q.KeyPrefix
	q.KeyPrefix = q.KeyPrefix + ":" + opts.QueueName + ":"

	redisIp := opts.RedisIp
	redisPasswd := opts.RedisPasswd
	redisMode := opts.Mode
	var err error
	q.Client, err = redisAction.Init(redisIp, redisPasswd, redisMode)
	if err != nil {
		return wrapError(err, "bull Init error")
	}

	return nil
}

func (q *Queue) Add(jobName string, jobData JobData, options ...withOption) (Job, error) {
	distOption := &JobOptions{}
	var name string

	for _, withOptionFunc := range options {
		withOptionFunc(distOption)
	}

	if jobName == "" {
		name = _DEFAULT_JOB_NAME
	} else {
		name = jobName
	}
	job, err := newJob(name, jobData, *distOption)
	if err != nil {
		return job, wrapError(err, "bull Add error")
	}
	err = q.addJob(job)
	if err != nil {
		return job, wrapError(err, "bull Add error")
	}

	q.Emit("waiting", job)

	return job, nil
}

func (q *Queue) Process(jobName string, handler func(Job) error) error {
	return nil
}

func (q *Queue) pause(paused bool) {
	// client := q.Client
	// TODO: pause: No where near full implementation, missing lots
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
	pausedKeyExists, err := client.HExists(context.Background(), q.KeyPrefix+"meta", "paused").Result()
	if err != nil {
		return false
	}
	if pausedKeyExists {
		return true
	}

	return false
}

func (q *Queue) addJob(job Job) error {
	// TODO: addJob: No where near full implementation, missing lots

	// also missing the return of the job id, etc

	rdb := q.Client
	keys := q.getKeys()
	args := q.getArgs(job)
	err := redisAction.ExecLua(luaScripts.AddJobLua, rdb, keys, args)
	if err != nil {
		return err
	}
	return nil
}

func (q *Queue) getKeys() []string {
	keys := make([]string, 0, 6)
	keys = append(keys, q.KeyPrefix+"wait")
	keys = append(keys, q.KeyPrefix+"paused")
	keys = append(keys, q.KeyPrefix+"meta-paused")
	keys = append(keys, q.KeyPrefix+"id")
	keys = append(keys, q.KeyPrefix+"delayed")
	keys = append(keys, q.KeyPrefix+"priority")

	return keys
}

func (q *Queue) getArgs(job Job) []interface{} {
	args := make([]interface{}, 0, 11)
	args = append(args, q.KeyPrefix)
	args = append(args, job.Id)
	args = append(args, job.Name)
	args = append(args, job.Data)
	args = append(args, job.OptsByJson)
	args = append(args, job.TimeStamp)
	args = append(args, job.Delay)
	args = append(args, job.DelayTimeStamp)
	args = append(args, job.Opts.Priority)
	if job.Opts.Lifo == "RPUSH" {
		args = append(args, "RPUSH")
	} else {
		args = append(args, "LPUSH")
	}
	args = append(args, q.Token)

	return args
}

func (q *Queue) Ping() error {
	return redisAction.Ping(q.Client)
}
