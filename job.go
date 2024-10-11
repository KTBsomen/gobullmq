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
	Priority         int    `json:"priority"`
	RemoveOnComplete bool   `json:"removeOnComplete"`
	RemoveOnFail     bool   `json:"removeOnFail"`
	Attempts         int    `json:"attempts"`
	Delay            int    `json:"delay"`
	TimeStamp        int64  `json:"timestamp"`
	Lifo             string `json:"lifo"`
	JobId            string `json:"jobId"`
	RepeatJobKey     string `json:"repeatJobKey"`

	Repeat JobRepeatOptions `json:"repeat"`
}

type JobRepeatOptions struct {
	// ParserOptions
	CurrentDate  time.Time `json:"currentDate"`
	StartDate    time.Time `json:"startDate"`
	EndDate      time.Time `json:"endDate"`
	UTC          bool      `json:"utc"`
	TZ           string    `json:"tz"`
	NthDayOfWeek int       `json:"nthDayOfWeek"`

	// RepeatOptions
	Pattern     string `json:"pattern"`     // A repeat pattern
	Limit       int    `json:"limit"`       // Number of times the job should repeat at max.
	Every       int    `json:"every"`       // Repeat after this amount of milliseconds (`pattern` setting cannot be used together with this setting.)
	Immediately bool   `json:"immediately"` // Repeated job should start right now (work only with every settings)
	Count       int    `json:"count"`       // The start value for the repeat iteration count.
	PrevMillis  int    `json:"prevMillis"`
	Offset      int    `json:"offset"`
	JobId       string `json:"jobId"`
}

type JobFilteredRepeatOptions struct {
	// ParserOptions
	CurrentDate  time.Time `json:"currentDate"`
	StartDate    time.Time `json:"startDate"`
	EndDate      time.Time `json:"endDate"`
	UTC          bool      `json:"utc"`
	TZ           string    `json:"tz"`
	NthDayOfWeek int       `json:"nthDayOfWeek"`

	// RepeatOptions
	Pattern    string `json:"pattern"` // A repeat pattern
	Limit      int    `json:"limit"`   // Number of times the job should repeat at max.
	Every      int    `json:"every"`   // Repeat after this amount of milliseconds (`pattern` setting cannot be used together with this setting.)
	Count      int    `json:"count"`   // The start value for the repeat iteration count.
	PrevMillis int    `json:"prevMillis"`
	Offset     int    `json:"offset"`
	JobId      string `json:"jobId"`
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
