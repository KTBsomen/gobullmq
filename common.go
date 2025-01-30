package gobullmq

import "go.codycody31.dev/gobullmq/types"

func QueueWithPriorityOp(priority int) types.QueueWithOption {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.Priority = priority
	}
}

func QueueWithRemoveOnCompleteOp(flag bool) types.QueueWithOption {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.RemoveOnComplete = flag
	}
}

func QueueWithRemoveOnFailOp(flag bool) types.QueueWithOption {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.RemoveOnFail = flag
	}
}

func QueueWithAttemptsOp(times int) types.QueueWithOption {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.Attempts = times
	}
}

func QueueWithDelayOp(delayTime int) types.QueueWithOption {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.Delay = delayTime
	}
}

// WithTimeStamp timeStamp by time.Now().UnixMilli()
func QueueWithTimeStamp(timeStamp int64) types.QueueWithOption {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.TimeStamp = timeStamp
	}
}

func QueueWithJobId(id string) types.QueueWithOption {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.JobId = id
	}
}

func QueueWithRepeat(repeat types.JobRepeatOptions) types.QueueWithOption {
	return func(o *types.JobOptions) {
		if o == nil {
			return
		}
		o.Repeat = repeat
	}
}
