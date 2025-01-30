package gobullmq

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"go.codycody31.dev/gobullmq/types"
	"strconv"
	"time"
)

const (
	_DEFAULT_JOB_NAME = "__default__"
)

func JobFromId(ctx context.Context, client redis.Cmdable, queueKey string, jobId string) (types.Job, error) {
	jobData, err := client.HGetAll(ctx, queueKey+jobId).Result()
	if err != nil {
		return types.Job{}, err
	}

	if len(jobData) == 0 {
		return types.Job{}, nil
	}

	//	return isEmpty(jobData)
	//	? undefined
	//	: this.fromJSON<T, R, N>(
	//		queue,
	//		(<unknown>jobData) as JobJsonRaw,
	//		jobId,
	//);

	job, err := JobFromJson(jobData)
	if err != nil {
		return types.Job{}, err
	}

	return job, nil
}

func JobFromJson(jobData map[string]string) (types.Job, error) {
	data := jobData["data"]
	opts, err := JobOptsFromJson(jobData["opts"])
	if err != nil {
		return types.Job{}, err
	}

	job := types.Job{
		Name: jobData["name"],
		Data: data,
		Opts: opts,
		Id:   jobData["id"],
	}

	// Parse int64 timestamp
	if timestampStr, ok := jobData["timestamp"]; ok {
		if timestamp, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
			job.TimeStamp = timestamp
		}
	}

	// Parse progress
	if progressStr, ok := jobData["progress"]; ok {
		if progress, err := strconv.Atoi(progressStr); err == nil {
			job.Progress = progress
		}
	}

	// Parse delay
	if delayStr, ok := jobData["delay"]; ok {
		if delay, err := strconv.Atoi(delayStr); err == nil {
			job.Delay = delay
		}
	}

	// Parse finishedOn
	if finishedOnStr, ok := jobData["finishedOn"]; ok {
		if finishedOn, err := strconv.ParseInt(finishedOnStr, 10, 64); err == nil {
			job.FinishedOn = time.Unix(finishedOn, 0)
		}
	}

	// Parse processedOn
	if processedOnStr, ok := jobData["processedOn"]; ok {
		if processedOn, err := strconv.ParseInt(processedOnStr, 10, 64); err == nil {
			job.ProcessedOn = time.Unix(processedOn, 0)
		}
	}

	if repeatJobKey, ok := jobData["rjk"]; ok {
		job.RepeatJobKey = repeatJobKey
	}

	if failedReason, ok := jobData["failedReason"]; ok {
		job.FailedReason = failedReason
	}

	if attemptsMadeStr, ok := jobData["attemptsMade"]; ok {
		if attemptsMade, err := strconv.Atoi(attemptsMadeStr); err == nil {
			job.AttemptsMade = attemptsMade
		}
	}

	if returnvalue, ok := jobData["returnvalue"]; ok {
		var returnVal interface{}
		if err := json.Unmarshal([]byte(returnvalue), &returnVal); err == nil {
			job.Returnvalue = returnVal
		}
	}

	//// Parse parentKey
	//if parentKey, ok := jobData["parentKey"]; ok {
	//	job.ParentKey = parentKey
	//}

	//// Parse parent (as a JSON object)
	//if parent, ok := jobData["parent"]; ok {
	//	var parentData map[string]interface{}
	//	if err := json.Unmarshal([]byte(parent), &parentData); err == nil {
	//		job.Parent = parentData
	//	}
	//}

	return job, nil
}

// jobOptsDecodeMap maps JSON keys to struct field names.
var jobOptsDecodeMap = map[string]string{
	"priority":         "Priority",
	"removeOnComplete": "RemoveOnComplete",
	"removeOnFail":     "RemoveOnFail",
	"attempts":         "Attempts",
	"delay":            "Delay",
	"timestamp":        "TimeStamp",
	"lifo":             "Lifo",
	"jobId":            "JobId",
	"repeatJobKey":     "RepeatJobKey",
	"token":            "Token",
	"currentDate":      "CurrentDate",
	"startDate":        "StartDate",
	"endDate":          "EndDate",
	"utc":              "UTC",
	"tz":               "TZ",
	"nthDayOfWeek":     "NthDayOfWeek",
	"pattern":          "Pattern",
	"limit":            "Limit",
	"every":            "Every",
	"immediately":      "Immediately",
	"count":            "Count",
}

func JobOptsFromJson(rawOpts string) (types.JobOptions, error) {
	var opts types.JobOptions
	err := json.Unmarshal([]byte(rawOpts), &opts)
	return opts, err

	//var opts map[string]interface{}
	//var jobOpts types.JobOptions
	//
	//if err := json.Unmarshal([]byte(rawOpts), &opts); err != nil {
	//	return jobOpts, err
	//}
	//
	//for key, value := range opts {
	//	if field, ok := jobOptsDecodeMap[key]; ok {
	//		switch field {
	//		case "Priority":
	//			jobOpts.Priority = int(value.(float64))
	//		case "RemoveOnComplete":
	//			jobOpts.RemoveOnComplete = value.(bool)
	//		case "RemoveOnFail":
	//			jobOpts.RemoveOnFail = value.(bool)
	//		case "Attempts":
	//			jobOpts.Attempts = int(value.(float64))
	//		case "Delay":
	//			jobOpts.Delay = int(value.(float64))
	//		case "TimeStamp":
	//			jobOpts.TimeStamp = int64(value.(float64))
	//		case "Lifo":
	//			jobOpts.Lifo = value.(string)
	//		case "JobId":
	//			jobOpts.JobId = value.(string)
	//		case "RepeatJobKey":
	//			jobOpts.RepeatJobKey = value.(string)
	//		case "Token":
	//			jobOpts.Token = value.(string)
	//			// Additional cases for other fields as needed
	//		}
	//		// TODO: Repeat - JobRepeatOptions
	//	}
	//}
	//
	//return jobOpts, nil
}

func JobMoveToFailed(ctx context.Context, client redis.Cmdable, queueKey string, err error, token string) error {
	return nil
}

func JobMoveToCompleted(ctx context.Context, client redis.Cmdable, queueKey string, result interface{}, token string) error {
	return nil
}

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
