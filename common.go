package gobullmq

type WithOption func(o *JobOptions)

func WithPriorityOp(priority int) WithOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.Priority = priority
	}
}

func WithRemoveOnCompleteOp(flag bool) WithOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.RemoveOnComplete = flag
	}
}

func WithRemoveOnFailOp(flag bool) WithOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.RemoveOnFail = flag
	}
}

func WithAttemptsOp(times int) WithOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.Attempts = times
	}
}

func WithDelayOp(delayTime int) WithOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.Delay = delayTime
	}
}

// WithTimeStamp timeStamp by time.Now().UnixMilli()
func WithTimeStamp(timeStamp int64) WithOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.TimeStamp = timeStamp
	}
}

func WithJobId(id string) WithOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.JobId = id
	}
}

func WithRepeat(repeat JobRepeatOptions) WithOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.Repeat = repeat
	}
}
