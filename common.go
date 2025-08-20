package gobullmq

import (
	"github.com/ktbsomen/gobullmq/types"
)

// AddOption defines the functional option type for Queue.Add
type AddOption func(*types.JobOptions)

// AddWithPriority sets the priority for the job.
// Lower number means higher priority.
func AddWithPriority(priority int) AddOption {
	return func(o *types.JobOptions) {
		o.Priority = priority
	}
}

// AddWithRemoveOnComplete configures job removal upon successful completion.
// If keep is provided, it specifies the number/age criteria.
// If keep is omitted, it defaults to removing the job immediately (Count: 0).
func AddWithRemoveOnComplete(keep ...types.KeepJobs) AddOption {
	return func(o *types.JobOptions) {
		setting := types.KeepJobs{Count: 0} // Default: remove immediately
		if len(keep) > 0 {
			setting = keep[0]
		}
		o.RemoveOnComplete = &setting // Use pointer
	}
}

// AddWithRemoveOnFail configures job removal upon failure.
// If keep is provided, it specifies the number/age criteria.
// If keep is omitted, it defaults to keeping the job (Count: -1).
func AddWithRemoveOnFail(keep ...types.KeepJobs) AddOption {
	return func(o *types.JobOptions) {
		setting := types.KeepJobs{Count: -1} // Default: keep forever
		if len(keep) > 0 {
			setting = keep[0]
		}
		o.RemoveOnFail = &setting // Use pointer
	}
}

// AddWithAttempts sets the maximum number of attempts for the job.
func AddWithAttempts(times int) AddOption {
	return func(o *types.JobOptions) {
		if times > 0 {
			o.Attempts = times
		}
	}
}

// AddWithDelay sets an initial delay (in milliseconds) before the job can be processed.
func AddWithDelay(delayMillis int) AddOption {
	return func(o *types.JobOptions) {
		if delayMillis > 0 {
			o.Delay = delayMillis
		}
	}
}

// AddWithTimestamp sets a custom timestamp for the job.
// Defaults to time.Now().UnixMilli() if not set.
func AddWithTimestamp(tsMillis int64) AddOption {
	return func(o *types.JobOptions) {
		o.TimeStamp = tsMillis
	}
}

// AddWithJobID sets a specific ID for the job.
// Use with caution, as IDs must be unique.
func AddWithJobID(id string) AddOption {
	return func(o *types.JobOptions) {
		o.JobId = id
	}
}

// AddWithRepeat configures the job to repeat based on the provided options.
func AddWithRepeat(repeatOpts types.JobRepeatOptions) AddOption {
	return func(o *types.JobOptions) {
		o.Repeat = &repeatOpts // Use pointer
	}
}

// AddWithLifo adds the job using LIFO (Last In, First Out) order.
func AddWithLifo() AddOption {
	return func(o *types.JobOptions) {
		o.Lifo = true
	}
}

// AddWithFailParentOnFailure marks the job to fail its parent job if this job fails.
func AddWithFailParentOnFailure(fail bool) AddOption {
	return func(o *types.JobOptions) {
		o.FailParentOnFailure = fail
	}
}

// AddWithParent sets the parent job information for this job.
func AddWithParent(parentOpts types.ParentOpts) AddOption {
	return func(o *types.JobOptions) {
		o.Parent = &parentOpts
	}
}

// AddWithRemoveDependencyOnFailure marks the job's dependency to be removed from its parent even if this job fails.
func AddWithRemoveDependencyOnFailure(remove bool) AddOption {
	return func(o *types.JobOptions) {
		o.RemoveDependencyOnFailure = remove
	}
}
