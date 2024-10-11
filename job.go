package gobullmq

import (
	"go.codycody31.dev/gobullmq/types"
	"time"
)

const (
	_DEFAULT_JOB_NAME = "__default__"
)

func newJob(name string, data types.JobData, opts types.JobOptions) (types.Job, error) {
	op := setOpts(opts)
	if name == "" {
		name = _DEFAULT_JOB_NAME
	}

	curJob := types.Job{
		Opts:         op,
		Name:         name,
		Data:         data,
		Progress:     0,
		Delay:        op.Delay,
		TimeStamp:    op.TimeStamp,
		AttemptsMade: 0,
	}

	err := curJob.ToJsonData()
	if err != nil {
		return curJob, err
	}

	return curJob, nil
}

func setOpts(opts types.JobOptions) types.JobOptions {
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
