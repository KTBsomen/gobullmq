package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ktbsomen/gobullmq/internal/lua"
	"github.com/ktbsomen/gobullmq/types"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

type scripts struct {
	redisClient redis.Cmdable   // Redis client used to interact with the redis server
	ctx         context.Context // Context used to handle the queue events
	keyPrefix   string
}

func newScripts(redisClient redis.Cmdable, ctx context.Context, keyPrefix string) *scripts {
	return &scripts{
		redisClient: redisClient,
		ctx:         ctx,
		keyPrefix:   keyPrefix,
	}
}

func (s *scripts) moveToFailedArgs(job *types.Job, failedReason string, removeOnFailed types.KeepJobs, token string, fetchNext bool) ([]string, []interface{}, error) {
	timestamp := time.Now()
	return s.moveToFinishedArgs(job, failedReason, "failedReason", removeOnFailed, "failed", token, timestamp, fetchNext)
}

// getKeepJobs determines the job retention policy based on provided parameters
func (s *scripts) getKeepJobs(shouldRemove interface{}, workerKeepJobs *types.KeepJobs) types.KeepJobs {
	// If shouldRemove is nil/undefined, use workerKeepJobs or default
	if shouldRemove == nil {
		if workerKeepJobs != nil {
			return *workerKeepJobs
		}
		return types.KeepJobs{Count: -1} // keep all
	}

	// Handle different types of shouldRemove
	switch v := shouldRemove.(type) {
	case types.KeepJobs:
		return v
	case int:
		return types.KeepJobs{Count: v}
	case bool:
		if v {
			return types.KeepJobs{Count: 0} // Remove all (keep none)
		}
		return types.KeepJobs{Count: -1} // Keep all
	default:
		return types.KeepJobs{Count: -1} // Keep all
	}
}

func (s *scripts) moveToFinishedArgs(job *types.Job, value string, propValue string, shouldRemove interface{}, target string, token string, timestamp time.Time, fetchNext bool) ([]string, []interface{}, error) {
	// Build the keys array - equivalent to moveToFinishedKeys in JS
	keys := []string{
		s.keyPrefix + "wait",
		s.keyPrefix + "active",
		s.keyPrefix + "prioritized",
		s.keyPrefix + "events",
		s.keyPrefix + "stalled",
		s.keyPrefix + "limiter",
		s.keyPrefix + "delayed",
		s.keyPrefix + "paused",
		s.keyPrefix + "meta",
		s.keyPrefix + "pc",
		s.keyPrefix + target,
		s.keyPrefix + job.Id,
		s.keyPrefix + "metrics:" + target,
	}

	// Convert job data to JSON string for the event
	eventData, err := json.Marshal(map[string]interface{}{
		"jobId": job.Id,
		"val":   value,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal event data: %v", err)
	}
	var workerKeepJobs *types.KeepJobs // or the appropriate type
	if target == "completed" {
		workerKeepJobs = job.Opts.RemoveOnComplete
	} else {
		workerKeepJobs = job.Opts.RemoveOnFail
	}
	var keepJobs = s.getKeepJobs(shouldRemove, workerKeepJobs)
	var payload map[string]interface{}
	payload = map[string]interface{}{
		"age":   keepJobs.Age,
		"count": keepJobs.Count,
	}
	if keepJobs.Age == 0 {
		payload = map[string]interface{}{
			"count": keepJobs.Count,
		}
	}

	opts := map[string]interface{}{
		"token":          token,
		"keepJobs":       payload,
		"lockDuration":   30000, // TODO: Get from worker options?
		"attempts":       job.Opts.Attempts,
		"attemptsMade":   job.AttemptsMade,
		"maxMetricsSize": "",                                 // TODO: Get from metrics options?
		"fpof":           job.Opts.FailParentOnFailure,       // Use value from job options
		"rdof":           job.Opts.RemoveDependencyOnFailure, // Use value from job options
		"parentKey":      job.ParentKey,                      // Pass parent key
	}
	// Pack options using msgpack
	packedOpts, err := msgpack.Marshal(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal options: %v", err)
	}

	// Build the args array
	args := []interface{}{
		job.Id,
		timestamp.UnixMilli(),
		propValue,
		value,
		target,
		string(eventData),
		fetchNext && !false, // Replace with worker.closing check when available
		s.keyPrefix,
		packedOpts,
	}

	return keys, args, nil
}

// retryJobArgs builds the keys and args for the retryJob Lua script call.
func (s *scripts) retryJobArgs(jobId string, lifo bool, token string) ([]string, []interface{}) {
	keys := []string{
		s.keyPrefix + "active",
		s.keyPrefix + "wait",
		s.keyPrefix + "paused",
		s.keyPrefix + jobId,
		s.keyPrefix + "meta",
		s.keyPrefix + "events",
		s.keyPrefix + "delayed",
		s.keyPrefix + "prioritized",
		s.keyPrefix + "pc",
	}

	pushCmd := "LPUSH"
	if lifo {
		pushCmd = "RPUSH"
	}

	args := []interface{}{
		s.keyPrefix,
		time.Now().UnixMilli(),
		pushCmd,
		jobId,
		token,
	}

	return keys, args
}

// moveToDelayedArgs builds the keys and args for the moveToDelayed Lua script call.
// timestampMillis represents when the job should be retried (in ms).
func (s *scripts) moveToDelayedArgs(jobId string, timestampMillis int64, token string) ([]string, []interface{}) {
	// Normalize timestamp and bake in job id lower 12 bits like BullMQ
	if timestampMillis < 0 {
		timestampMillis = 0
	}
	var jobIdNumeric int64
	if parsed, err := strconv.ParseInt(jobId, 10, 64); err == nil {
		jobIdNumeric = parsed
	} else {
		jobIdNumeric = 0
	}
	var score int64
	if timestampMillis > 0 {
		score = timestampMillis*0x1000 + (jobIdNumeric & 0xfff)
	} else {
		score = 0
	}

	keys := []string{
		s.keyPrefix + "wait",
		s.keyPrefix + "active",
		s.keyPrefix + "prioritized",
		s.keyPrefix + "delayed",
		s.keyPrefix + jobId,
		s.keyPrefix + "events",
		s.keyPrefix + "paused",
		s.keyPrefix + "meta",
	}

	args := []interface{}{
		s.keyPrefix,
		time.Now().UnixMilli(),
		fmt.Sprintf("%d", score),
		jobId,
		token,
	}

	return keys, args
}

// UpdateProgress updates the progress of a job
func (s *scripts) updateProgress(jobId string, progress interface{}) error {
	keys := []string{
		s.keyPrefix + jobId,
		s.keyPrefix + jobId + ":events",
	}

	progressJson, err := json.Marshal(progress)
	if err != nil {
		return err
	}

	result, err := lua.UpdateProgress(s.redisClient, keys, jobId, progressJson)
	if err != nil {
		return err
	}

	resultInt64, ok := result.(int64)
	if !ok {
		return fmt.Errorf("invalid result type: %T", result)
	}

	if resultInt64 == -1 {
		return fmt.Errorf("job not found")
	}

	return nil
}

// updateData updates the job's data field atomically in Redis
func (s *scripts) updateData(jobId string, data interface{}) error {
	keys := []string{
		s.keyPrefix + jobId,
	}

	dataJson, err := json.Marshal(data)
	if err != nil {
		return err
	}

	result, err := lua.UpdateData(s.redisClient, keys, string(dataJson))
	if err != nil {
		return err
	}
	resultInt64, ok := result.(int64)
	if !ok {
		return fmt.Errorf("invalid result type: %T", result)
	}
	if resultInt64 == -1 {
		return fmt.Errorf("job not found")
	}
	return nil
}

// moveJobFromActiveToWait moves a job back from Active to Wait when manually rate limited.
// It returns the remaining TTL (in ms) for the limiter key, clamped to 0 if negative.
func (s *scripts) moveJobFromActiveToWait(jobId string, token string) (int64, error) {
	keys := []string{
		s.keyPrefix + "active",
		s.keyPrefix + "wait",
		s.keyPrefix + "stalled",
		s.keyPrefix + jobId + ":lock",
		s.keyPrefix + "paused",
		s.keyPrefix + "meta",
		s.keyPrefix + "limiter",
		s.keyPrefix + "prioritized",
		s.keyPrefix + "events",
	}

	args := []interface{}{
		jobId,
		token,
		s.keyPrefix + jobId,
	}

	result, err := lua.MoveJobFromActiveToWait(s.redisClient, keys, args...)
	if err != nil {
		return 0, err
	}

	pttl, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid result type from MoveJobFromActiveToWait: %T", result)
	}

	if pttl < 0 {
		return 0, nil
	}
	return pttl, nil
}
