/**
 * @Description: data struct and operations with job
 * @FilePath: /bull-golang/job.go
 * @Author: liyibing liyibing@lixiang.com
 * @Date: 2023-07-19 15:59:43
 */
package gobullmq

import (
	"encoding/json"
	"time"
)

const (
	_DEFAULT_JOB_NAME = "__default__"
)

type RedisJobOptions struct {
	fpof bool // If true, moves parent to failed.
	kl   int  // Maximum amount of log entries that will be preserved
	rdof bool // If true, removes the job from its parent dependencies when it fails after all attempts.
}

type ParentOpts struct {
	waitChildrenKey       string
	parentDependenciesKey string
	parentKey             string
}

// 这边应该要求传入json数据，需要在使用接口直接保证
type JobData interface{}

// 这个结构也是需要被序列化的
type JobOptions struct {
	Priority         int    `json:"priority,omitempty" msgpack:"priority,omitempty"`
	RemoveOnComplete bool   `json:"removeOnComplete,omitempty" msgpack:"removeOnComplete,omitempty"`
	RemoveOnFail     bool   `json:"removeOnFail,omitempty" msgpack:"removeOnFail,omitempty"`
	Attempts         int    `json:"attempts,omitempty" msgpack:"attempts,omitempty"`
	Delay            int    `json:"delay,omitempty" msgpack:"delay,omitempty"`
	TimeStamp        int64  `json:"timestamp,omitempty" msgpack:"timestamp,omitempty"`
	Lifo             string `json:"lifo,omitempty" msgpack:"lifo,omitempty"`
	JobId            string `json:"jobId,omitempty" msgpack:"jobId,omitempty"`
	RepeatJobKey     string `json:"repeatJobKey,omitempty" msgpack:"repeatJobKey,omitempty"`

	Repeat JobRepeatOptions `json:"repeat,omitempty" msgpack:"repeat,omitempty"`
}

type JobRepeatOptions struct {
	// ParserOptions
	CurrentDate  *time.Time `json:"currentDate,omitempty" msgpack:"currentDate,omitempty"`
	StartDate    *time.Time `json:"startDate,omitempty" msgpack:"startDate,omitempty"`
	EndDate      *time.Time `json:"endDate,omitempty" msgpack:"endDate,omitempty"`
	UTC          bool       `json:"utc,omitempty" msgpack:"utc,omitempty"`
	TZ           string     `json:"tz,omitempty" msgpack:"tz,omitempty"`
	NthDayOfWeek int        `json:"nthDayOfWeek,omitempty" msgpack:"nthDayOfWeek,omitempty"`

	// RepeatOptions
	Pattern     string `json:"pattern,omitempty" msgpack:"pattern,omitempty"`         // A repeat pattern
	Limit       int    `json:"limit,omitempty" msgpack:"limit,omitempty"`             // Number of times the job should repeat at max.
	Every       int    `json:"every,omitempty" msgpack:"every,omitempty"`             // Repeat after this amount of milliseconds (`pattern` setting cannot be used together with this setting.)
	Immediately bool   `json:"immediately,omitempty" msgpack:"immediately,omitempty"` // Repeated job should start right now (work only with every settings)
	Count       int    `json:"count,omitempty" msgpack:"count,omitempty"`             // The start value for the repeat iteration count.
	// omit from json output always
	PrevMillis int    `json:"prevMillis,omitempty" msgpack:"prevMillis,omitempty"`
	Offset     int    `json:"offset,omitempty" msgpack:"offset,omitempty"`
	JobId      string `json:"jobId,omitempty" msgpack:"jobId,omitempty"`
}

type Job struct {
	Name           string
	Id             string
	Data           JobData
	Opts           JobOptions
	OptsByJson     []byte
	TimeStamp      int64
	Progress       int
	Delay          int
	DelayTimeStamp int64

	AttemptsMade int
}

/**
 * @description:
 * @return {*}
 */
func (job *Job) toJsonData() error {
	data, err := json.Marshal(job.Opts)
	if err != nil {
		return err
	}
	job.OptsByJson = data
	return err
}

func newJob(name string, data JobData, opts JobOptions) (Job, error) {
	op := setOpts(opts)
	if name == "" {
		name = _DEFAULT_JOB_NAME
	}

	curJob := Job{
		Opts:         op,
		Name:         name,
		Data:         data,
		Progress:     0,
		Delay:        op.Delay,
		TimeStamp:    op.TimeStamp,
		AttemptsMade: 0,
	}

	err := curJob.toJsonData()
	if err != nil {
		return curJob, err
	}

	return curJob, nil
}

func setOpts(opts JobOptions) JobOptions {
	op := opts

	if opts.Delay < 0 {
		opts.Delay = 0
	}

	if opts.Attempts == 0 {
		op.Attempts = 1
	} else {
		op.Attempts = opts.Attempts
	}

	op.Delay = opts.Delay

	if opts.TimeStamp == 0 {
		op.TimeStamp = time.Now().UnixMilli()
	} else {
		op.TimeStamp = opts.TimeStamp
	}

	return op
}
