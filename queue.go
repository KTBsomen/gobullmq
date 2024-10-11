package gobullmq

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v5"
	"math"
	"strconv"
	"time"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/redisAction"

	"github.com/google/uuid"
	"github.com/gorhill/cronexpr"
)

type QueueEventType string

var (
	QueueEventCompleted   QueueEventType = "completed"
	QueueEventWait        QueueEventType = "wait"
	QueueEventActive      QueueEventType = "active"
	QueueEventPaused      QueueEventType = "paused"
	QueueEventPrioritized QueueEventType = "prioritized"
	QueueEventDelayed     QueueEventType = "delayed"
	QueueEventFailed      QueueEventType = "failed"
)

type QueueIface interface {
	eventemitter.EventEmitterIface
	Init(name string, opts QueueOption) (*Queue, error)
	Add(jobName string, jobData JobData, options ...WithOption) (Job, error)
	AddBulk(jobs []QueueJob) ([]Job, error)
	Pause() error
	Resume()
	IsPaused() bool
	Drain(delayed bool) error
	Clean(grace int, limit int, cType QueueEventType) ([]string, error)
	Obliterate(opts ObliterateOpts) error
	Ping() error
	Remove(jobId string, removeChildren bool) error
	TrimEvents(max int64) (int64, error)
}

var _ QueueIface = (*Queue)(nil)

type Queue struct {
	eventemitter.EventEmitter
	Name      string
	Token     uuid.UUID
	KeyPrefix string
	Client    redis.Cmdable
	Prefix    string
	ctx       context.Context
}

type QueueOption struct {
	Mode        int
	KeyPrefix   string
	RedisIp     string
	RedisPasswd string

	// Options for the streams used internally in BullMQ.
	Streams struct {
		// Options for the events stream.
		Events struct {
			MaxLen int64 // Max approximated length for streams. Default is 10 000 events.
		}
	}
}

type QueueJob struct {
	Name string
	Data JobData
	Opts JobOptions
}

func NewQueue(name string, opts QueueOption) (*Queue, error) {
	q := &Queue{
		Name:  name,
		Token: uuid.New(),
		ctx:   context.Background(), // TODO: Take input of context
	}

	q.EventEmitter.Init()

	if opts.KeyPrefix == "" {
		q.KeyPrefix = "bull"
	} else {
		q.KeyPrefix = opts.KeyPrefix
	}
	q.Prefix = q.KeyPrefix
	q.KeyPrefix = q.KeyPrefix + ":" + name + ":"

	if name == "" {
		return nil, wrapError(nil, "Queue name must be provided'")
	}

	redisIp := opts.RedisIp
	redisPasswd := opts.RedisPasswd
	redisMode := opts.Mode
	var err error
	q.Client, err = redisAction.Init(redisIp, redisPasswd, redisMode)
	if err != nil {
		return nil, wrapError(err, "bull Init error")
	}

	// TODO: Get the default values for the job options and store them in jobOpts

	if opts.Streams.Events.MaxLen != 0 {
		q.Client.XTrim(q.ctx, q.toKey("events"), opts.Streams.Events.MaxLen)
	} else {
		q.Client.HSet(q.ctx, q.toKey("meta"), "opts.maxLenEvents", "10000")
	}

	return q, nil
}

func (q *Queue) Init(name string, opts QueueOption) (*Queue, error) {
	nq, err := NewQueue(name, opts)
	if err != nil {
		return nil, wrapError(err, "bull Init error")
	}
	return nq, nil
}

// TODO: Test repeat job logic
func (q *Queue) Add(jobName string, jobData JobData, options ...WithOption) (Job, error) {
	distOption := &JobOptions{}
	var name string

	for _, withOptionFunc := range options {
		withOptionFunc(distOption)
	}

	if distOption.JobId != "" {
		if distOption.JobId == "0" || (distOption.JobId[0] == '0' && distOption.JobId[1] != ':') {
			return Job{}, wrapError(nil, "JobId cannot be '0' or start with 0:")
		}
	}

	// TODO: setup this.jobsOpts for the default base options configured

	if jobName == "" {
		name = _DEFAULT_JOB_NAME
	} else {
		name = jobName
	}

	if distOption.Repeat.Every != 0 || distOption.Repeat.Pattern != "" {
		return q.addRepeatableJob(name, jobData, *distOption, true)
	}

	job, err := newJob(name, jobData, *distOption)
	if err != nil {
		return job, wrapError(err, "attempt to create new job failed")
	}
	jobId, err := q.addJob(job, distOption.JobId)
	if err != nil {
		return job, wrapError(err, "failed to add job")
	}
	job.Id = jobId

	q.Emit("waiting", job)

	return job, nil
}

// TODO: Implement AddBulk
func (q *Queue) AddBulk(jobs []QueueJob) ([]Job, error) {
	var jobArr []Job
	return jobArr, nil
}

func (q *Queue) pause(pause bool) error {
	client := q.Client
	p := "paused"

	// Determine the source and destination queues based on whether to pause or resume
	src := "wait"
	dst := "paused"
	if !pause {
		src = "paused"
		dst = "wait"
		p = "resumed"
	}

	// Check if the source queue exists
	exists, err := client.Exists(context.Background(), q.toKey(src)).Result()
	if err != nil {
		return wrapError(err, "failed to check if queue exists")
	}

	if exists == 0 {
		// If the queue doesn't exist, there's no need to rename it
		return wrapError(nil, "source queue does not exist, nothing to pause or resume")
	}

	// Define the keys to operate on
	keys := []string{
		q.toKey(src),
		q.toKey(dst),
		q.toKey("meta"),
		q.toKey("prioritized"),
		q.toKey("events"),
	}

	rs, err := lua.Pause(client, keys, p)
	if err != nil {
		return wrapError(err, "failed to pause or resume queue")
	}
	fmt.Println("Result: ", rs)

	return nil
}

func (q *Queue) Pause() error {
	err := q.pause(true)
	if err != nil {
		return wrapError(err, "failed to pause queue")
	}
	q.Emit("paused")
	return nil
}

func (q *Queue) Resume() {
	q.pause(false)
	q.Emit("resumed")
}

func (q *Queue) IsPaused() bool {
	client := q.Client
	pausedKeyExists, _ := client.HExists(context.Background(), q.KeyPrefix+"meta", "paused").Result()
	return pausedKeyExists
}

func (q *Queue) addJob(job Job, jobId string) (string, error) {
	rdb := q.Client

	keys := make([]string, 0, 8)
	keys = append(keys, q.KeyPrefix+"wait")
	keys = append(keys, q.KeyPrefix+"paused")
	keys = append(keys, q.KeyPrefix+"meta")
	keys = append(keys, q.KeyPrefix+"id")
	keys = append(keys, q.KeyPrefix+"delayed")
	keys = append(keys, q.KeyPrefix+"prioritized")
	keys = append(keys, q.KeyPrefix+"completed")
	keys = append(keys, q.KeyPrefix+"events")
	keys = append(keys, q.KeyPrefix+"pc")

	args := make([]interface{}, 0)
	args = append(args, q.KeyPrefix)
	args = append(args, jobId)
	args = append(args, job.Name)
	args = append(args, job.TimeStamp)
	for i := 0; i < 4; i++ {
		args = append(args, nil)
	}
	args = append(args, job.Opts.RepeatJobKey)

	msgPackedArgs, err := msgpack.Marshal(args)
	if err != nil {
		return "nil", fmt.Errorf("failed to marshal args: %v", err)
	}

	msgPackedOpts, err := msgpack.Marshal(job.Opts)
	if err != nil {
		return "nil", fmt.Errorf("failed to marshal opts: %v", err)
	}

	givenJobId, err := lua.AddJob(rdb, keys, msgPackedArgs, job.Data, msgPackedOpts)
	if err != nil {
		return "nil", fmt.Errorf("failed to add job: %v", err)
	}

	jobIdStr := givenJobId.(string)

	return jobIdStr, nil
}

// TODO: Finish
func (q *Queue) addRepeatableJob(name string, jobData JobData, opts JobOptions, skipCheckExists bool) (Job, error) {
	prevMillis := opts.Repeat.PrevMillis
	currentCount := opts.Repeat.Count

	if currentCount == 0 {
		currentCount = 1
	} else {
		currentCount++
	}

	if limit := opts.Repeat.Limit; limit > 0 && currentCount > limit {
		return Job{}, wrapError(nil, "repeatable job limit exceeded")
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)

	if opts.Repeat.EndDate != nil && now > opts.Repeat.EndDate.UnixNano()/int64(time.Millisecond) {
		return Job{}, wrapError(nil, "repeatable job end date exceeded")
	}

	if int64(prevMillis) > now {
		now = int64(prevMillis)
	}

	nextMillis, err := repeatStrategy(now, opts)
	if err != nil {
		return Job{}, wrapError(err, "failed to calculate nextMillis")
	}

	pattern := opts.Repeat.Pattern
	hasImmediately := false

	if (opts.Repeat.Every != 0 || pattern != "") && opts.Repeat.Immediately {
		hasImmediately = true
	}

	var offset int64
	if hasImmediately {
		offset = now - nextMillis
	} else {
		offset = 0
	}

	if nextMillis > 0 {
		//if (!prevMillis && opts.jobId) {
		//	repeatOpts.jobId = opts.jobId;
		//}
		if prevMillis != 0 && opts.JobId != "" {
			opts.Repeat.JobId = opts.JobId
		}

		repeatJobKey := getRepeatKey(name, opts.Repeat)

		repeatableExists := true

		if !skipCheckExists {
			f, err := q.Client.ZScore(q.ctx, q.toKey("repeat"), repeatJobKey).Result()
			if err != nil {
				repeatableExists = false
			}

			if f == 0 {
				repeatableExists = false
			}
		}

		if repeatableExists {
			jobId, err := getRepeatJobId(name, nextMillis, GetMD5Hash(repeatJobKey), opts.JobId)
			if err != nil {
				return Job{}, wrapError(err, "failed to get repeatable job id")
			}

			now := time.Now().UnixNano() / int64(time.Millisecond)
			delay := nextMillis + offset - now

			opts.JobId = jobId
			opts.Delay = int(delay) // delay < 0 || hasImmediately ? 0 : delay
			opts.TimeStamp = now
			opts.Repeat.PrevMillis = int(nextMillis)
			opts.RepeatJobKey = repeatJobKey
			opts.Repeat.Count = currentCount

			// TODO: Convert opts.Repeat from pointer to value

			q.Client.ZAdd(q.ctx, q.toKey("repeat"), &redis.Z{
				Score:  float64(nextMillis),
				Member: repeatJobKey,
			})

			fmt.Println("Repeatable job added: ", jobId)
			// name, nextMillis, repeatJobKey, opts, jobData, currentCount, hasImmediately
			fmt.Println("name:", name)
			fmt.Println("nextMillis:", nextMillis)
			fmt.Println("repeatJobKey:", repeatJobKey)
			j, _ := json.Marshal(opts)
			fmt.Println("opts:", string(j))
			fmt.Println("currentCount:", currentCount)
			fmt.Println("hasImmediately:", hasImmediately)

			job, err := newJob(name, jobData, opts)
			if err != nil {
				return job, wrapError(err, "attempt to create new job failed")
			}
			_, err = q.addJob(job, jobId)
			if err != nil {
				return job, wrapError(err, "failed to add repeatable job")
			}

			q.Emit("waiting", job)

			return job, nil
		} else {
			return Job{}, wrapError(nil, "repeatable job does not exist")
		}
	} else {
		return Job{}, wrapError(nil, "repeatable job nextMillis is invalid")
	}
}

func getRepeatJobId(name string, nextMillis int64, namespace string, jobId string) (string, error) {
	checksum := GetMD5Hash(fmt.Sprintf("%s:%d:%s", name, jobId, namespace))
	return fmt.Sprintf("repeat:%s:%s", checksum, strconv.FormatInt(nextMillis, 10)), nil
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func getRepeatKey(name string, repeat JobRepeatOptions) string {
	var endDate string
	if repeat.EndDate != nil {
		endDate = strconv.FormatInt(repeat.EndDate.UnixNano()/int64(time.Millisecond), 10)
	} else {
		endDate = ""
	}

	tz := repeat.TZ
	pattern := repeat.Pattern
	suffix := pattern
	if suffix == "" {
		suffix = strconv.Itoa(repeat.Every)
	}

	jobId := repeat.JobId

	return fmt.Sprintf("%s:%s:%s:%s:%s", name, jobId, endDate, tz, suffix)
}

func repeatStrategy(millis int64, opts JobOptions) (int64, error) {
	ropts := opts.Repeat
	pattern := ropts.Pattern

	if pattern != "" && ropts.Every != 0 {
		return 0, fmt.Errorf("both .pattern and .every options are defined for this repeatable job")
	}

	if ropts.Every != 0 {
		expr := math.Floor(float64(millis/int64(ropts.Every)))*float64(ropts.Every) + func() float64 {
			if ropts.Immediately {
				return 0
			}
			return float64(ropts.Every)
		}()
		return int64(expr), nil
	}

	// Calc currentData based of opts
	var currentDate time.Time
	if !ropts.StartDate.IsZero() {
		startDate, _ := time.Parse(time.RFC3339, ropts.StartDate.String())
		if startDate.After(time.Unix(millis/1000, 0)) {
			currentDate = startDate
		} else {
			currentDate = time.Unix(millis/1000, 0)
		}
	} else {
		currentDate = time.Unix(millis/1000, 0)
	}

	// Get interval next time
	nextTime := cronexpr.MustParse(pattern).Next(currentDate)

	return nextTime.UnixNano() / int64(time.Millisecond), nil
}

//export const getNextMillis = (millis: number, opts: RepeatOptions): number = > {
//const pattern = opts.pattern;
//if (pattern && opts.every) {
//throw new Error(
//'Both .pattern and .every options are defined for this repeatable job',
//);
//}
//
//if (opts.every) {
//return (
//Math.floor(millis / opts.every) * opts.every +
//(opts.immediately ? 0: opts.every)
//);
//}
//
//const currentDate =
//opts.startDate && new Date(opts.startDate) > new Date(millis)
//? new Date(opts.startDate)
//: new Date(millis);
//const interval = parseExpression(pattern, {
//...opts,
//currentDate,
//});
//
//try {
//return interval.next().getTime();
//} catch (e) {
//// Ignore error
//}
//}

func (q *Queue) Drain(delayed bool) error {
	keys := []string{
		q.toKey("wait"),
		q.toKey("paused"),
	}

	if delayed {
		keys = append(keys, q.toKey("delayed"))
	} else {
		keys = append(keys, "")
	}
	keys = append(keys, q.toKey("prioritized"))

	_, err := lua.Drain(q.Client, keys, q.KeyPrefix)
	if err != nil {
		return wrapError(err, "failed to drain queue")
	}
	return nil
}

// Clean cleans jobs from the queue (limit is the max number of jobs to clean, 0 is unlimited)
func (q *Queue) Clean(grace int, limit int, cType QueueEventType) ([]string, error) {
	var jobs []string

	set := cType
	timestamp := time.Now().Unix() - int64(grace)

	keys := []string{
		q.toKey(string(set)),
		q.toKey("events"),
	}

	i, err := lua.CleanJobsInSet(q.Client, keys, q.KeyPrefix, timestamp, limit, string(set))
	if err != nil {
		return jobs, wrapError(err, "failed to clean jobs")
	}

	jobs = i.([]string)

	q.Emit("cleaned", jobs, string(set))
	return jobs, nil
}

type ObliterateOpts struct {
	Force bool // Use force = true to force obliteration even with active jobs in the queue (default: false)
	Count int  // Use count with the maximum number of deleted keys per iteration (default: 1000)
}

func (q *Queue) Obliterate(opts ObliterateOpts) error {
	err := q.pause(true)
	if err != nil {
		return wrapError(err, "failed to pause queue")
	}

	var force string
	if opts.Force {
		force = "force"
	}

	count := opts.Count
	if count == 0 {
		count = 1000
	}

	keys := []string{
		q.toKey("meta"),
		q.KeyPrefix,
	}

	for {
		i, err := lua.Obliterate(q.Client, keys, count, force)
		if err != nil {
			return wrapError(err, "failed to obliterate queue")
		}

		// -1: Queue is not paused
		// -2: Queue has active jobs
		// 0: Queue obliterated completely
		// 1: Queue obliterated partially
		result := i.(int64)

		if result < 0 {
			switch result {
			case -1:
				return wrapError(nil, "cannot obliterate non-paused queue")
			case -2:
				return wrapError(nil, "cannot obliterate queue with active jobs")
			}
		} else if result == 0 {
			break
		}
	}

	return nil
}

func (q *Queue) Ping() error {
	return redisAction.Ping(q.Client)
}

func (q *Queue) toKey(name string) string {
	return q.KeyPrefix + name
}

func (q *Queue) Remove(jobId string, removeChildren bool) error {
	keys := []string{
		q.KeyPrefix,
	}

	i, err := lua.RemoveJob(q.Client, keys, jobId, removeChildren)
	if err != nil {
		return wrapError(err, fmt.Sprintf("failed to remove job: %s", jobId))
	}

	if i.(int64) == 0 {
		return wrapError(err, fmt.Sprintf("failed to remove job: %s, the job is locked", jobId))
	}

	return nil
}

func (q *Queue) TrimEvents(max int64) (int64, error) {
	return q.Client.XTrim(q.ctx, q.KeyPrefix+"events", max).Result()
}
