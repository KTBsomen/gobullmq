package gobullmq

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/fifoqueue"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/types"
	"log"
	"math"
	"strconv"
	"sync"
	"time"
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

func (w *Worker) getStalledJobs() ([]string, error) {
	// Simulate retrieving stalled jobs. Replace with actual Redis query logic.
	// Here, we're returning a hardcoded list for demonstration purposes.
	return []string{"job1", "job2"}, nil
}

func (w *Worker) extendLocksForJobs(jobs []*types.Job) error {
	for _, job := range jobs {
		log.Printf("Extending lock for job: %s", job.Id)
		// Extend lock logic here
	}
	return nil
}

// TODO: callProcessJob

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
			if fifoQueue.NumTotal() < w.opts.Concurrency && fifoQueue.NumTotal() == 0 {
				tokenPostfix++
				token := fmt.Sprintf("%s:%d", w.Token, tokenPostfix)

				fifoQueue.Add(func() (types.Job, error) {
					return w.retryIfFailed(func() (*types.Job, error) {
						// Fetch the next job
						nextJob, err := w.getNextJob(token, GetNextJobOptions{})
						if err != nil {
							return nil, err
						}

						if nextJob == nil {
							return nil, errors.New("no job available")
						}

						return nextJob, nil
					}, w.opts.RunRetryDelay)
				})
			}

			job, err := fifoQueue.Fetch()
			if err != nil {
				w.Emit("error", fmt.Sprintf("Critical error in fetch: %v", err))
			}

			if job != nil {
				token := job.Opts.Token
				// TODO: Seems that the processJob function is actually being called concurrently, even when set to 1
				fifoQueue.Add(func() (types.Job, error) {
					// Define the function to process the job
					processJobFunc := func() (*types.Job, error) {
						processJob, err := w.processJob(
							*job,
							token,
							fifoQueue.NumTotal,
						)
						if err != nil {
							return nil, err
						}
						return &processJob, nil
					}

					// Pass the function to retryIfFailed
					return w.retryIfFailed(processJobFunc, w.opts.RunRetryDelay)
				})
			}
		}
	}()

	return nil
}

// getNextJob gets the next job
func (w *Worker) getNextJob(token string, opts GetNextJobOptions) (*types.Job, error) {
	if w.paused {
		if opts.Block {
			// Wait for resume event, not even sure how? Possible pause actually needs to be a wait group?
		} else {
			return nil, nil
		}
	}

	if w.closing {
		return nil, nil
	}

	// TODO: Implement getNextJob logic, need all of this, very critical

	return nil, nil

	//if (this.drained && block && !this.limitUntil && !this.waiting) {
	//	try {
	//		this.waiting = this.waitForJob();
	//		try {
	//		const jobId = await this.waiting;
	//		return this.moveToActive(token, jobId);
	//	} finally {
	//		this.waiting = null;
	//	}
	//	} catch (err) {
	//		// Swallow error if locally paused or closing since we did force a disconnection
	//		if (
	//			!(this.paused || this.closing) &&
	//				isNotConnectionError(<Error>err)
	//	) {
	//			throw err;
	//		}
	//	}
	//} else {
	//	if (this.limitUntil) {
	//		this.abortDelayController?.abort();
	//		this.abortDelayController = new AbortController();
	//		await this.delay(this.limitUntil, this.abortDelayController);
	//	}
	//	return this.moveToActive(token);
	//}

	//return w.moveToActive(token, "")
}

// TODO: rateLimit - Overrides the rate limit to be active for the next jobs.

// moveToActive moves the job to the active list
func (w *Worker) moveToActive(token string, jobId string) (types.Job, error) {
	if jobId != "" && jobId[0:2] == "0:" {
		blockUntil, err := strconv.Atoi(jobId[2:])
		if err != nil {
			return types.Job{}, fmt.Errorf("failed to parse blockUntil: %v", err)
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

	msgPacked, err := msgpack.Marshal(struct {
		token        string             `msgpack:"token"`
		lockDuration int                `msgpack:"lockDuration"`
		limiter      RateLimiterOptions `msgpack:"limiter"`
	}{
		token:        token,
		lockDuration: w.opts.LockDuration,
		limiter:      *w.opts.Limiter,
	})
	if err != nil {
		return types.Job{}, fmt.Errorf("failed to marshal: %v", err)
	}

	rawResult, err := lua.MoveToActive(w.redisClient, keys, w.KeyPrefix, time.Now().Unix(), jobId, msgPacked)
	if err != nil {
		return types.Job{}, err
	}

	// Assert rawResults to []interface{}
	rawResultSlice, ok := rawResult.([]interface{})
	if !ok {
		return types.Job{}, fmt.Errorf("unexpected type for rawResult: %T", rawResult)
	}

	// Process results using raw2NextJobData
	result := raw2NextJobData(rawResultSlice)

	job, err := w.nextJobFromJobData(result.JobData, result.ID, result.LimitUntil, result.DelayUntil, token)
	if err != nil {
		return types.Job{}, err
	}
	return *job, err
}

// raw2NextJobData processes raw data and returns a typed NextJobData structure
func raw2NextJobData(raw []interface{}) *NextJobData {
	if raw == nil || len(raw) < 4 {
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

// TODO: waitForJob

// TODO: delay

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
		return nil, nil
	}

	// Update limitUntil and delayUntil
	w.limitUntil = int64(math.Max(float64(limitUntil), 0))
	if delayUntil > 0 {
		w.blockUntil = int64(math.Max(float64(delayUntil), 0))
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
func (w *Worker) processJob(job types.Job, token string, fetchNextCallback func() int) (types.Job, error) {
	if w.closing || w.paused {
		return types.Job{}, nil
	}

	w.Emit("active", job, "waiting")

	w.jobsInProgress.Lock()
	w.jobsInProgress.jobs[job.Id] = jobInProgress{job: job, ts: time.Now()}
	w.jobsInProgress.Unlock()

	result, err := w.processFn(w.ctx, &job)

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

		errMoveToFailed := JobMoveToFailed(w.ctx, w.redisClient, w.KeyPrefix, err, token)
		if errMoveToFailed != nil {
			w.Emit("error", errMoveToFailed)
			return types.Job{}, errMoveToFailed
		}

		w.Emit("failed", job, err, "active")
		return types.Job{}, err
	}

	//const completed = await job.moveToCompleted(
	//	result,
	//	token,
	//	fetchNextCallback() && !(this.closing || this.paused),
	//);
	_ = JobMoveToCompleted(w.ctx, w.redisClient, w.KeyPrefix, result, token)
	w.Emit("completed", job, result, "active")
	//const [jobData, jobId, limitUntil, delayUntil] = completed || [];
	//return this.nextJobFromJobData(
	//	jobData,
	//	jobId,
	//	limitUntil,
	//	delayUntil,
	//	token,
	//);

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

// TODO: extendLocsk

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
