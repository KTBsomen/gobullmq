package gobullmq

import "go.codycody31.dev/gobullmq/types"

func JobOptionWithPriority(priority int) types.JobOptionFunc {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.Priority = priority
	}
}

func JobOptionRemoveOnComplete(flag bool) types.JobOptionFunc {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.RemoveOnComplete = flag
	}
}

func JobOptionRemoveOnFail(flag bool) types.JobOptionFunc {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.RemoveOnFail = flag
	}
}

func JobOptionWithAttempts(times int) types.JobOptionFunc {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.Attempts = times
	}
}

func JobOptionWithDelay(delayTime int) types.JobOptionFunc {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.Delay = delayTime
	}
}

// JobOptionWithTimeStamp sets the timestamp using time.Now().UnixMilli()
func JobOptionWithTimeStamp(timeStamp int64) types.JobOptionFunc {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.TimeStamp = timeStamp
	}
}

func JobOptionWithJobId(id string) types.JobOptionFunc {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.JobId = id
	}
}

func JobOptionWithRepeat(repeat types.JobRepeatOptions) types.JobOptionFunc {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.Repeat = repeat
	}
}
