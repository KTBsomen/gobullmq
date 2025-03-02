package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/types"
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

func (s *scripts) moveToFailedArgs(job *types.Job, failedReason string, removeOnFailed KeepJobs, token string, fetchNext bool) ([]string, []interface{}, error) {
	timestamp := time.Now()
	return s.moveToFinishedArgs(job, failedReason, "failedReason", removeOnFailed, "failed", token, timestamp, fetchNext)
}

func (s *scripts) moveToFinishedArgs(job *types.Job, value string, propValue string, shouldRemove KeepJobs, target string, token string, timestamp time.Time, fetchNext bool) ([]string, []interface{}, error) {
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
