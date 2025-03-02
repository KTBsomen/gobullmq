package gobullmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/fifoqueue"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/types"
)

//type WorkerIface interface {
//}
//=
//var _ WorkerIface = (*Worker)(nil)

// TODO: Add metric tracking, allowing for storing of time per job, and other metrics
// Including speed, throughput, largest/smallest job time, average rate of completion or failure, etc

// TODO: At some point, expose a API to the processFn to allow it to extend the lock for a job
// And perform other tasks, without having to call the worker, or redis directly

// WorkerProcessFunc processFn is the function that processes the job
type WorkerProcessFunc func(ctx context.Context, job *types.Job) (interface{}, error)

type Worker struct {
	Name         string    // Name of the queue
	Token        uuid.UUID // Token used to identify the queue events
	ee           *eventemitter.EventEmitter
	running      bool               // Flag to indicate if the queue events is running
	closing      bool               // Flag to indicate if the queue events is closing
	paused       bool               // Flag to indicate if the queue events is paused
	redisClient  redis.Cmdable      // Redis client used to interact with the redis server
	ctx          context.Context    // Context used to handle the queue events
	cancel       context.CancelFunc // Cancel function used to stop the queue events
	Prefix       string
	KeyPrefix    string
	mutex        sync.Mutex     // Mutex used to lock/unlock the queue events
	wg           sync.WaitGroup // WaitGroup used to wait for the queue events to finish
	opts         WorkerOptions
	processFn    WorkerProcessFunc
	resumeWorker func()

	// Locks
	extendLocksTimer  *time.Timer
	stalledCheckTimer *time.Timer
	lemu              sync.Mutex

	jobsInProgress *jobsInProgress

	blockUntil int64
	limitUntil int64
	drained    bool

	scripts *scripts
}

type WorkerOptions struct {
	Autorun          bool
	Concurrency      int
	Limiter          *RateLimiterOptions
	Metrics          *MetricsOptions
	Prefix           string
	MaxStalledCount  int
	StalledInterval  int
	RemoveOnComplete *KeepJobs
	RemoveOnFail     *KeepJobs
	SkipStalledCheck bool
	SkipLockRenewal  bool
	DrainDelay       int
	LockDuration     int
	LockRenewTime    int
	RunRetryDelay    int
}

type KeepJobs struct {
	Age   int // Maximum age in seconds for job to be kept.
	Count int // Maximum count of jobs to be kept.
}

type RateLimiterOptions struct {
	Max      int `msgpack:"max"`
	Duration int `msgpack:"duration"`
}

type MetricsOptions struct {
	MaxDataPoints int
}

type GetNextJobOptions struct {
	Block bool
}

type jobsInProgress struct {
	sync.Mutex
	jobs map[string]jobInProgress
}

type jobInProgress struct {
	job types.Job
	ts  time.Time
}

// NextJobData represents the structured data returned by raw2NextJobData
type NextJobData struct {
	JobData    map[string]interface{} // Processed job data from raw[0]
	ID         string                 // ID of the job from raw[1]
	LimitUntil int64                  // Limit time from raw[2]
	DelayUntil int64                  // Delay time from raw[3]
}

// NewWorker creates a new Worker instance
func NewWorker(ctx context.Context, name string, opts WorkerOptions, connection redis.Cmdable, processor WorkerProcessFunc) (*Worker, error) {
	ctx, cancel := context.WithCancel(ctx)

	// TODO: Have default opts, then merge in the provided opts and allow the provided opts to override the default opts

	w := &Worker{
		Name:        name,
		Token:       uuid.New(),
		ee:          eventemitter.NewEventEmitter(),
		running:     false,
		closing:     false,
		ctx:         ctx,
		cancel:      cancel,
		redisClient: connection,
		opts:        opts,
		processFn:   processor,
		blockUntil:  0,
		limitUntil:  0,
		drained:     false,
	}

	w.jobsInProgress = &jobsInProgress{
		jobs: make(map[string]jobInProgress),
	}

	if opts.Prefix == "" {
		w.KeyPrefix = "bull"
	} else {
		w.KeyPrefix = opts.Prefix
	}
	w.Prefix = w.KeyPrefix
	w.KeyPrefix = w.KeyPrefix + ":" + name + ":"

	if w.opts.StalledInterval <= 0 {
		return nil, errors.New("stalledInterval must be greater than 0")
	}

	client, ok := w.redisClient.(*redis.Client)
	if !ok {
		return nil, errors.New("redis client is not a *redis.Client")
	}
	client.Do(w.ctx, "CLIENT", "SETNAME", fmt.Sprintf("%s:%s", w.Prefix, base64.StdEncoding.EncodeToString([]byte(w.Name))))

	w.scripts = newScripts(w.redisClient, w.ctx, w.KeyPrefix)

	if opts.Autorun {
		err := w.Run()
		if err != nil {
			return nil, fmt.Errorf("error running worker: %v", err)
		}
	}

	return w, nil
}

// Emit emits the event with the given name and arguments
func (w *Worker) Emit(event string, args ...interface{}) {
	w.ee.Emit(event, args...)
}

// Off stops listening for the event
func (w *Worker) Off(event string, listener func(...interface{})) {
	w.ee.RemoveListener(event, listener)
}

// On listens for the event
func (w *Worker) On(event string, listener func(...interface{})) {
	w.ee.On(event, listener)
}

// Once listens for the event only once
func (w *Worker) Once(event string, listener func(...interface{})) {
	w.ee.Once(event, listener)
}

func (w *Worker) createJob(jobData map[string]interface{}, jobId string) types.Job {
	// Convert jobData to map[string]string
	convertedData, err := utils.ConvertToMapString(jobData)
	if err != nil {
		fmt.Printf("Error converting jobData: %v\n", err)
		return types.Job{
			Id: jobId,
		}
	}

	// Use the converted map for JobFromJson
	job, err := JobFromJson(convertedData)
	if err != nil {
		return types.Job{
			Id: jobId,
		}
	}
	job.Id = jobId

	return job
}

// _______________________________________________________________________________________ //

// Run starts the worker
func (w *Worker) Run() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.running {
		return errors.New("worker is already running")
	}

	w.running = true

	if w.closing {
		return errors.New("worker is closing")
	}

	go w.startStalledCheckTimer()

	go w.startLockExtender()

	fifoQueue := fifoqueue.NewFifoQueue[types.Job](10, false)
	tokenPostfix := 0

	w.wg.Add(1)
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				w.wg.Done()
				return
			default:
			}

			//!this.waiting &&
			//	asyncFifoQueue.numTotal() < this.opts.concurrency &&
			//	(!this.limitUntil || asyncFifoQueue.numTotal() == 0)
			if !w.paused && fifoQueue.NumTotal() < w.opts.Concurrency {
				tokenPostfix++
				token := fmt.Sprintf("%s:%d", w.Token, tokenPostfix)

				if err := fifoQueue.Add(func() (types.Job, error) {
					j, err := w.retryIfFailed(func() (*types.Job, error) {
						// Fetch the next job
						nextJob, err := w.getNextJob(token, GetNextJobOptions{Block: true})
						if err != nil {
							return nil, err
						}

						if nextJob == nil {
							// Sleep briefly to avoid tight loop when no jobs are available
							time.Sleep(time.Second)
							return nil, nil
						}

						return nextJob, nil
					}, w.opts.RunRetryDelay)
					if err != nil {
						return types.Job{}, err
					}

					return j, nil
				}); err != nil {
					w.Emit("error", fmt.Sprintf("Error adding job to queue: %v", err))
					continue
				}
			}

			job, err := fifoQueue.Fetch(w.ctx)
			if err != nil {
				if err != context.Canceled && !errors.Is(err, fifoqueue.ErrQueueClosed) && !errors.Is(err, errors.New("no job available")) {
					w.Emit("error", fmt.Sprintf("Critical error in fetch: %v", err))
				}
				continue
			}

			if job != nil && job.Id != "" && job.Id != "0" {
				token := job.Token
				if err := fifoQueue.Add(func() (types.Job, error) {
					// Define the function to process the job
					processJobFunc := func() (*types.Job, error) {
						processJob, err := w.processJob(
							*job,
							token,
							func() bool {
								return fifoQueue.NumTotal() <= w.opts.Concurrency
							},
						)
						if err != nil {
							return nil, err
						}
						return &processJob, nil
					}

					// Pass the function to retryIfFailed
					return w.retryIfFailed(processJobFunc, w.opts.RunRetryDelay)
				}); err != nil {
					w.Emit("error", fmt.Sprintf("Error processing job: %v", err))
				}
			}
		}
	}()

	return nil
}

// getNextJob gets the next job
func (w *Worker) getNextJob(token string, opts GetNextJobOptions) (*types.Job, error) {
	if w.paused {
		if opts.Block {
			for w.paused && !w.closing {
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			return nil, nil
		}
	}

	if w.closing {
		return nil, nil
	}

	if w.drained && opts.Block && w.limitUntil == 0 {
		jobID, err := w.waitForJob()
		if err != nil {
			if !w.paused && !w.closing {
				return nil, fmt.Errorf("failed to wait for job: %v", err)
			}
			return nil, nil
		}

		j, err := w.moveToActive(token, jobID)
		if err != nil {
			return nil, err
		}
		return j, nil
	} else {
		if w.limitUntil != 0 {
			if err := w.delay(w.limitUntil); err != nil {
				return nil, fmt.Errorf("failed to delay: %v", err)
			}
		}
		j, err := w.moveToActive(token, "")
		if err != nil {
			return nil, err
		}
		return j, nil
	}
}

// TODO: rateLimit - Overrides the rate limit to be active for the next jobs.

// moveToActive moves the job to the active list
func (w *Worker) moveToActive(token string, jobId string) (*types.Job, error) {
	if jobId != "" && len(jobId) > 2 && jobId[0:2] == "0:" {
		blockUntil, err := strconv.Atoi(jobId[2:])
		if err != nil {
			return nil, fmt.Errorf("failed to parse blockUntil: %v", err)
		}
		w.blockUntil = int64(blockUntil)
	}

	keys := []string{
		w.KeyPrefix + "wait",
		w.KeyPrefix + "active",
		w.KeyPrefix + "prioritized",
		w.KeyPrefix + "events",
		w.KeyPrefix + "stalled",
		w.KeyPrefix + "limiter",
		w.KeyPrefix + "delayed",
		w.KeyPrefix + "paused",
		w.KeyPrefix + "meta",
		w.KeyPrefix + "pc",
	}

	opts := map[string]interface{}{
		"token":        token,
		"lockDuration": 30000,
		"limiter":      w.opts.Limiter,
	}

	msgPackedOpts, err := msgpack.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal: %v", err)
	}

	rawResult, err := lua.MoveToActive(w.redisClient, keys, w.KeyPrefix, time.Now().Unix(), jobId, string(msgPackedOpts))
	if err != nil {
		return nil, err
	}

	// Assert rawResults to []interface{}
	rawResultSlice, ok := rawResult.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type for rawResult: %T", rawResult)
	}

	// Process results using raw2NextJobData
	result := raw2NextJobData(rawResultSlice)

	// Check if the result indicates an invalid or non-existent job
	if result.ID == "0" || result.ID == "" {
		return nil, nil
	}

	job, err := w.nextJobFromJobData(result.JobData, result.ID, result.LimitUntil, result.DelayUntil, token)
	if err != nil {
		return nil, fmt.Errorf("failed to next job from job data: %v", err)
	}
	return job, nil
}

// raw2NextJobData processes raw data and returns a typed NextJobData structure
func raw2NextJobData(raw []interface{}) *NextJobData {
	if len(raw) < 4 {
		// Return nil if data is invalid
		return nil
	}

	// Initialize the result structure
	result := &NextJobData{
		ID:         fmt.Sprintf("%v", raw[1]),
		LimitUntil: int64(math.Max(float64(raw[2].(int64)), 0)),
		DelayUntil: int64(math.Max(float64(raw[3].(int64)), 0)),
	}

	// Process raw[0] if it exists
	if raw[0] != nil {
		result.JobData = utils.Array2obj(raw[0])
	}

	return result
}

// waitForJob waits for a job ID from the wait list
func (w *Worker) waitForJob() (string, error) {
	blockTimeout := math.Max(float64(w.opts.DrainDelay), 0.01)
	if w.blockUntil > 0 {
		blockTimeout = math.Max(float64((w.blockUntil-time.Now().UnixMilli())/1000), 0.01)
	}

	// // Only Redis v6.0.0 and above supports doubles as block time
	// blockTimeout = isRedisVersionLowerThan(
	//   this.blockingConnection.redisVersion,
	//   '6.0.0',
	// )
	//   ? Math.ceil(blockTimeout)
	//   : blockTimeout;

	// We restrict the maximum block timeout to 10 second to avoid
	// blocking the connection for too long in the case of reconnections
	// reference: https://github.com/taskforcesh/bullmq/issues/1658
	blockTimeout = math.Min(blockTimeout, 10)

	result, err := w.redisClient.BRPopLPush(w.ctx, w.KeyPrefix+"wait", w.KeyPrefix+"active", time.Duration(blockTimeout)).Result()
	if err != nil {
		return "", err
	}
	return result, nil
}

// delay delays the execution for the specified time
func (w *Worker) delay(until int64) error {
	now := time.Now().UnixMilli()
	if until > now {
		time.Sleep(time.Duration(until-now) * time.Millisecond)
	}
	return nil
}

// nextJobFromJobData processes the next job data and returns a Job.
func (w *Worker) nextJobFromJobData(
	jobData map[string]interface{},
	jobID string,
	limitUntil int64,
	delayUntil int64,
	token string,
) (*types.Job, error) {
	// Handle nil jobData
	if jobData == nil {
		if !w.drained {
			w.Emit("drained")
			w.drained = true
			w.blockUntil = 0
		}
	}

	// Update limitUntil and delayUntil
	w.limitUntil = int64(math.Max(float64(limitUntil), 0))
	if delayUntil > 0 {
		w.blockUntil = int64(math.Max(float64(delayUntil), 0))
	}

	if jobData == nil {
		return nil, nil
	}

	// Process jobData if present
	w.drained = false
	job := w.createJob(jobData, jobID)
	job.Token = token
	if job.Opts.Repeat.Every != 0 || job.Opts.Repeat.Pattern != "" {
		// TODO: Repeatable.AddNextRepeatableJob
		//err := w.Repeatable.AddNextRepeatableJob("jobName", job.Data, job.Opts)
		//if err != nil {
		//	return nil, err
		//}
	}
	return &job, nil
}

// processJob processes the job
func (w *Worker) processJob(job types.Job, token string, fetchNextCallback func() bool) (types.Job, error) {
	if w.closing || w.paused {
		return types.Job{}, nil
	}

	w.Emit("active", job, "waiting")

	w.jobsInProgress.Lock()
	w.jobsInProgress.jobs[job.Id] = jobInProgress{job: job, ts: time.Now()}
	w.jobsInProgress.Unlock()

	var result interface{}
	var err error

	// Wrap processFn call with recover to handle panics
	func() {
		defer func() {
			if r := recover(); r != nil {
				w.Emit("error", fmt.Sprintf("Panic recovered for job %s with token %s: %v", job.Id, token, r))
				err = fmt.Errorf("panic: %v", r)
			}
		}()

		result, err = w.processFn(w.ctx, &job)
	}()

	// Remove job from jobsInProgress
	for _, jp := range w.jobsInProgress.jobs {
		if jp.job.Id == job.Id {
			w.jobsInProgress.Lock()
			delete(w.jobsInProgress.jobs, job.Id)
			w.jobsInProgress.Unlock()
			break
		}
	}

	if err != nil {
		if err.Error() == RateLimitError {
			//this.limitUntil = await this.moveLimitedBackToWait(job, token);
			return types.Job{}, err
		}

		if err.Error() == "DelayedError" || err.Error() == "WaitingChildrenError" {
			return types.Job{}, err
		}

		if err != nil {
			w.Emit("error", fmt.Sprintf("Error moving job to failed: %v", err))
			return types.Job{}, err
		}

		// job.moveToFailed
		func(err error, token string) {
			job.FailedReason = err.Error()

			// this.saveStacktrace(multi,err);

			//
			// Check if an automatic retry should be performed
			//
			moveToFailed := false
			var finishedOn time.Time

			if job.AttemptsMade < job.Opts.Attempts {
				// const opts = queue.opts as WorkerOptions;

				// // Check if backoff is needed
				// const delay = await Backoffs.calculate(
				//   <BackoffOptions>this.opts.backoff,
				//   this.attemptsMade,
				//   err,
				//   this,
				//   opts.settings && opts.settings.backoffStrategy,
				// );

				// if (delay === -1) {
				//   moveToFailed = true;
				// } else if (delay) {
				//   const args = this.scripts.moveToDelayedArgs(
				// 	this.id,
				// 	Date.now() + delay,
				// 	token,
				//   );
				//   (<any>multi).moveToDelayed(args);
				//   command = 'delayed';
				// } else {
				//   // Retry immediately
				//   (<any>multi).retryJob(
				// 	this.scripts.retryJobArgs(this.id, this.opts.lifo, token),
				//   );
				//   command = 'retryJob';
				// }
			} else {
				moveToFailed = true
			}

			if moveToFailed {
				keys, args, err := w.scripts.moveToFailedArgs(w.KeyPrefix, job.Id, token)
				if err != nil {
					w.Emit("error", fmt.Sprintf("Error moving job to failed: %v", err))
					return
				}
		}(err, token)

		w.Emit("failed", job, err, "active")

		return types.Job{}, err
	}

	// Move job to completed and fetch next if needed
	keys, args, err := func(ctx context.Context, client redis.Cmdable, queueKey string, job *types.Job, result interface{}, token string, getNext bool) ([]string, []interface{}, error) {
		job.Returnvalue = result

		stringifiedReturnValue, err := json.Marshal(result)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal return value: %v", err)
		}

		stringifiedJsonReturnValueWithJobId, err := json.Marshal(struct {
			Returnvalue interface{} `json:"returnvalue"`
			Id          string      `json:"id"`
		}{
			Returnvalue: result,
			Id:          job.Id,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal return value with job id: %v", err)
		}

		// workerKeepJobs := w.opts.RemoveOnComplete
		// keepJobs := workerKeepJobs
		metricsKey := fmt.Sprintf("%smetrics:%s", w.KeyPrefix, "completed")

		keys := []string{
			w.KeyPrefix + "wait",
			w.KeyPrefix + "active",
			w.KeyPrefix + "prioritized",
			w.KeyPrefix + "events",
			w.KeyPrefix + "stalled",
			w.KeyPrefix + "limiter",
			w.KeyPrefix + "delayed",
			w.KeyPrefix + "paused",
			w.KeyPrefix + "meta",
			w.KeyPrefix + "pc",
			w.KeyPrefix + "completed",
			w.KeyPrefix + job.Id,
			metricsKey,
		}

		// TODO: Some of this data should not be hard coded, kinda defeats the point
		opts := map[string]interface{}{
			"token": token,
			"keepJobs": map[string]interface{}{
				"Age":   0,
				"Count": -1,
			},
			"lockDuration":   30000,
			"attempts":       job.Opts.Attempts,
			"attemptsMade":   job.AttemptsMade,
			"maxMetricsSize": "",
			"fpof":           false,
			"rdof":           false,
		}

		packedOpts, err := msgpack.Marshal(opts)

		if err != nil {
			return nil, nil, err
		}

		args := []interface{}{
			job.Id,
			time.Now().Unix(),
			"returnvalue",
			stringifiedReturnValue,
			"completed",
			stringifiedJsonReturnValueWithJobId,
			(getNext && !(w.closing || w.paused)),
			w.KeyPrefix,
			packedOpts,
		}

		return keys, args, nil
	}(w.ctx, w.redisClient, w.KeyPrefix, &job, result, token, (fetchNextCallback() && !(w.closing || w.paused)))
	if err != nil {
		w.Emit("error", fmt.Sprintf("Error moving job to completed: %v", err))
		return types.Job{}, err
	}

	completed, err := func(id string, keys []string, args []interface{}) ([]interface{}, error) {
		rawResult, err := lua.MoveToFinished(w.redisClient, keys, args...)
		if err != nil {
			return []interface{}{}, err
		}

		rawResultSlice, ok := rawResult.(int64)
		if !ok {
			// Attempt to type assert rawResult to a []interface{}
			rawResultSliceInterface, ok := rawResult.([]interface{})
			if !ok {
				return []interface{}{}, fmt.Errorf("unexpected type for rawResult: %T", rawResult)
			}

			return []interface{}{rawResultSliceInterface}, nil
		}

		return []interface{}{rawResultSlice}, nil
	}(job.Id, keys, args)
	if err != nil {
		w.Emit("error", fmt.Sprintf("Error moving job to completed: %v", err))
		return types.Job{}, err
	}

	job.FinishedOn = time.Unix(args[1].(int64), 0)

	// IF completed[0] is a inteface{} skip this part??
	if _, ok := completed[0].(int64); !ok {
		// Just, skip this...
	} else {
		if completed[0].(int64) < 0 {
			switch completed[0].(int64) {
			case -1:
				return types.Job{}, fmt.Errorf("missing key for job %s: %d", job.Id, completed)
			case -2:
				return types.Job{}, fmt.Errorf("missing lock for job %s: %d", job.Id, completed)
			case -3:
				return types.Job{}, fmt.Errorf("not in active set for job %s: %d", job.Id, completed)
			case -4:
				return types.Job{}, fmt.Errorf("has pending dependencies for job %s: %d", job.Id, completed)
			case -6:
				return types.Job{}, fmt.Errorf("lock is not owned by this client for job %s: %d", job.Id, completed)
			default:
				return types.Job{}, fmt.Errorf("unknown error for job %s: %d", job.Id, completed)
			}
		}
	}

	w.Emit("completed", job, result, "active")

	// Parse the completed data
	nextData := raw2NextJobData(completed)
	if nextData != nil {
		// Get the next job
		j, err := w.nextJobFromJobData(
			nextData.JobData,
			nextData.ID,
			nextData.LimitUntil,
			nextData.DelayUntil,
			token,
		)
		if err != nil {
			w.Emit("error", fmt.Sprintf("Error getting next job: %v", err))
			return types.Job{}, err
		}
		return *j, nil
	}
	return types.Job{}, nil
}

// TODO: pause
func (w *Worker) pause() {
	if w.paused {
		return
	}

	// await (!doNotWaitActive && this.whenCurrentJobsFinished());

	w.paused = true
	w.Emit("paused")
}

// Resume resumes processing of this worker (if paused).
func (w *Worker) resume() {
	if w.resumeWorker != nil {
		w.resumeWorker()
		w.Emit("resumed")
	}
}

// IsPaused returns true if the worker is paused
func (w *Worker) IsPaused() bool {
	return !!w.paused
}

// IsRunning returns true if the worker is running
func (w *Worker) IsRunning() bool {
	return w.running
}

func (w *Worker) Wait() {
	w.wg.Wait()
}

// Close closes the worker
func (w *Worker) Close() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closing {
		return
	}

	w.closing = true

	w.cancel()

	// Stop timers
	if w.stalledCheckTimer != nil {
		w.stalledCheckTimer.Stop()
	}
	if w.extendLocksTimer != nil {
		w.extendLocksTimer.Stop()
	}

	// Wait for all jobs to finish
	w.wg.Wait()
}

// startStalledCheckTimer starts the stalled check timer
func (w *Worker) startStalledCheckTimer() {
	if w.closing || w.opts.SkipStalledCheck {
		return
	}

	w.stalledCheckTimer = time.AfterFunc(time.Duration(w.opts.StalledInterval), func() {
		if w.closing || w.opts.SkipStalledCheck {
			return
		}

		if err := w.moveStalledJobsToWait(); err != nil {
			w.Emit("error", err)
		}
		w.startStalledCheckTimer()
	})
}

// startLockExtender starts the lock extender
func (w *Worker) startLockExtender() {
	if w.closing || w.opts.SkipLockRenewal {
		return
	}

	w.extendLocksTimer = time.AfterFunc(time.Duration(w.opts.LockRenewTime/2), func() {
		w.lemu.Lock()
		defer w.lemu.Unlock()
		if w.closing || w.opts.SkipLockRenewal {
			return
		}

		now := time.Now()
		var jobsToExtend []*types.Job

		for _, jp := range w.jobsInProgress.jobs {
			if jp.ts.IsZero() {
				jp.ts = now
				continue
			}

			if jp.ts.Add(time.Duration(w.opts.LockRenewTime / 2)).Before(now) {
				jp.ts = now
				jobsToExtend = append(jobsToExtend, &jp.job)
			}
		}

		if len(jobsToExtend) > 0 {
			if err := w.extendLocksForJobs(jobsToExtend); err != nil {
				w.Emit("error", err)
			}
		}
		w.startLockExtender() // Restart the timer
	})
}

// TODO: whenCurrentJobsFinished
func (w *Worker) whenCurrentJobsFinished(reconnect bool) {
	////
	//// Force reconnection of blocking connection to abort blocking redis call immediately.
	////
	//if (this.waiting) {
	//	await this.blockingConnection.disconnect();
	//} else {
	//	reconnect = false;
	//}
	//
	//if (this.asyncFifoQueue) {
	//	await this.asyncFifoQueue.waitAll();
	//}
	//
	//reconnect && (await this.blockingConnection.reconnect());
}

// retryIfFailed retries a job if it failed
func (w *Worker) retryIfFailed(jobFunc func() (*types.Job, error), delay int) (types.Job, error) {
	for {
		nextJob, err := jobFunc()
		if err != nil {
			w.Emit("error", err)

			// Retry only if a delay is specified
			if delay > 0 {
				time.Sleep(time.Duration(delay) * time.Millisecond)
				continue
			}

			return types.Job{}, err
		}

		// If no job is available, retry after the delay
		if nextJob == nil {
			if delay > 0 {
				time.Sleep(time.Duration(delay) * time.Millisecond)
				continue
			}

			return types.Job{}, nil
		}

		// Successfully retrieved a job
		return *nextJob, nil
	}
}

// extendLocks extends the locks for the jobs in progress
func (w *Worker) extendLocks() error {
	var jobs []*types.Job
	w.jobsInProgress.Lock()
	for _, jip := range w.jobsInProgress.jobs {
		jobs = append(jobs, &jip.job)
	}
	w.jobsInProgress.Unlock()
	return w.extendLocksForJobs(jobs)
}

// extendLocksForJobs extends the locks for the jobs in progress
func (w *Worker) extendLocksForJobs(jobs []*types.Job) error {
	for _, job := range jobs {
		keys := []string{
			w.KeyPrefix + "lock",
			w.KeyPrefix + "stalled",
		}

		_, err := lua.ExtendLock(w.redisClient, keys, job.Token, w.opts.LockDuration, job.Id)
		if err != nil {
			// Log or handle the error if the lock cannot be extended
			w.Emit("error", fmt.Errorf("could not renew lock for job %s: %w", job.Id, err))
			return err
		}
	}

	return nil
}

// moveStalledJobsToWait moves stalled jobs to the wait list
func (w *Worker) moveStalledJobsToWait() error {
	chunkSize := 50
	failed, stalled, err := func() (failed []string, stalled []string, error error) {
		keys := []string{
			w.KeyPrefix + "stalled",
			w.KeyPrefix + "wait",
			w.KeyPrefix + "active",
			w.KeyPrefix + "failed",
			w.KeyPrefix + "stalled-check",
			w.KeyPrefix + "meta",
			w.KeyPrefix + "paused",
			w.KeyPrefix + "events",
		}

		result, err := lua.MoveStalledJobsToWait(w.redisClient, keys, w.opts.MaxStalledCount,
			w.KeyPrefix,
			time.Now().Unix(),
			w.opts.StalledInterval)
		if err != nil {
			return nil, nil, err
		}

		// Type assert result as []interface{} to access failed and stalled lists
		resultSlice, ok := result.([]interface{})
		if !ok || len(resultSlice) != 2 {
			return nil, nil, fmt.Errorf("unexpected Lua script result format")
		}

		// Convert the first element (failed jobs) to []string
		failedInterfaces, ok := resultSlice[0].([]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("failed jobs format incorrect")
		}

		failed = make([]string, len(failedInterfaces))
		for i, f := range failedInterfaces {
			failed[i], ok = f.(string)
			if !ok {
				return nil, nil, fmt.Errorf("failed job ID format incorrect")
			}
		}

		// Convert the second element (stalled jobs) to []string
		stalledInterfaces, ok := resultSlice[1].([]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("stalled jobs format incorrect")
		}

		stalled = make([]string, len(stalledInterfaces))
		for i, s := range stalledInterfaces {
			stalled[i], ok = s.(string)
			if !ok {
				return nil, nil, fmt.Errorf("stalled job ID format incorrect")
			}
		}

		return failed, stalled, nil
	}()
	if err != nil {
		return err
	}

	for _, jobId := range stalled {
		w.Emit("stalled", jobId, "active")
	}

	failedJobs := make([]types.Job, len(failed))
	for i, jobId := range failed {
		j, err := JobFromId(w.ctx, w.redisClient, w.KeyPrefix, jobId)
		if err != nil {
			return err
		}

		failedJobs = append(failedJobs, j)

		if (i+1)%chunkSize == 0 {
			w.notifyFailedJobs(failedJobs)
			failedJobs = failedJobs[:0]
		}
	}

	w.notifyFailedJobs(failedJobs)
	return nil
}

// notifyFailedJobs emits a failed event for each job in the provided list
func (w *Worker) notifyFailedJobs(jobs []types.Job) {
	for _, job := range jobs {
		w.Emit("failed", job, errors.New("job stalled more than allowable limit"), "active")
	}
}

// TODO: moveLimitedBackToWait
