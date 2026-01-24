package fifoqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// FifoQueue handles asynchronous FIFO tasks with thread-safe operations using a fixed worker pool.
type FifoQueue[T any] struct {
	queue        chan T                 // Channel to queue items (results)
	errors       chan error             // Channel for errors
	tasks        chan func() (T, error) // Channel of tasks to execute
	pending      sync.WaitGroup         // WaitGroup to manage pending tasks
	ignoreErrors bool                   // Flag to ignore errors
	isClosed     atomic.Bool            // Thread-safe flag for queue state
	pendingCount atomic.Int32           // Thread-safe counter for pending tasks
	mu           sync.RWMutex           // Mutex for thread-safe operations
	ctx          context.Context
	cancel       context.CancelFunc
	workers      int
}

// NewFifoQueue initializes a new FifoQueue with a fixed number of worker goroutines.
// bufferSize controls buffering for results and tasks; workers controls concurrency (>=1).
func NewFifoQueue[T any](workers int, ignoreErrors bool) *FifoQueue[T] {
	if workers <= 0 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &FifoQueue[T]{
		queue:        make(chan T, workers),
		errors:       make(chan error, workers),
		tasks:        make(chan func() (T, error), workers),
		ignoreErrors: ignoreErrors,
		ctx:          ctx,
		cancel:       cancel,
		workers:      workers,
	}
	q.pendingCount.Store(0)
	q.startWorkers()
	return q
}

func (q *FifoQueue[T]) startWorkers() {
	for i := 0; i < q.workers; i++ {
		go func() {
			for {
				select {
				case <-q.ctx.Done():
					return
				case task, ok := <-q.tasks:
					if !ok { // tasks channel closed
						return
					}
					q.executeTask(task)
				}
			}
		}()
	}
}

func (q *FifoQueue[T]) executeTask(task func() (T, error)) {
	q.pending.Add(1)
	q.pendingCount.Add(1)
	defer func() {
		q.pending.Done()
		q.pendingCount.Add(-1)
	}()

	select {
	case <-q.ctx.Done():
		return
	default:
	}

	result, err := task()
	if err != nil {
		if !q.ignoreErrors {
			select {
			case q.errors <- err:
			case <-q.ctx.Done():
			}
		}
		return
	}

	select {
	case q.queue <- result:
	case <-q.ctx.Done():
	}
}

// Add enqueues a new task for execution by the worker pool.
func (q *FifoQueue[T]) Add(task func() (T, error)) error {
	if q.isClosed.Load() {
		return ErrQueueClosed
	}
	select {
	case <-q.ctx.Done():
		return ErrQueueClosed
	case q.tasks <- task:
		return nil
	}
}

// Fetch retrieves the next item from the result queue or error channel.
func (q *FifoQueue[T]) Fetch(ctx context.Context) (*T, error) {
	if q.isClosed.Load() && q.NumTotal() == 0 && len(q.queue) == 0 && len(q.errors) == 0 {
		return nil, ErrQueueClosed
	}

	select {
	case item := <-q.queue:
		return &item, nil
	case err := <-q.errors:
		if !q.ignoreErrors {
			return nil, err
		}
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.ctx.Done():
		return nil, ErrQueueClosed
	}
}

// WaitAll waits until all tasks are completed (with timeout) and properly closes the queue.
// The timeout parameter specifies the maximum time to wait for tasks to complete.
// Returns an error if the timeout is exceeded.
func (q *FifoQueue[T]) WaitAll(timeout time.Duration) error {
	q.mu.Lock()
	if !q.isClosed.Load() {
		q.isClosed.Store(true)
		close(q.tasks) // stop accepting new tasks
		q.cancel()     // cancel context FIRST to unblock any blocked tasks
		q.mu.Unlock()

		// Wait for pending tasks with timeout
		done := make(chan struct{})
		go func() {
			q.pending.Wait()
			close(done)
		}()

		var timedOut bool
		select {
		case <-done:
			// All tasks completed normally
		case <-time.After(timeout):
			// Timeout - force close anyway
			timedOut = true
		}

		q.mu.Lock()
		close(q.queue)
		close(q.errors)
		q.mu.Unlock()

		if timedOut {
			return ErrWaitTimeout
		}
		return nil
	}
	q.mu.Unlock()
	return nil
}

// NumPending returns the number of pending tasks.
func (q *FifoQueue[T]) NumPending() int { return int(q.pendingCount.Load()) }

// NumQueued returns the number of items in the queue.
func (q *FifoQueue[T]) NumQueued() int { return len(q.queue) }

// NumTotal returns the total number of tasks (pending + queued results).
func (q *FifoQueue[T]) NumTotal() int { return q.NumPending() + q.NumQueued() + len(q.tasks) }

// IsClosed returns whether the queue is closed.
func (q *FifoQueue[T]) IsClosed() bool { return q.isClosed.Load() }

var ErrQueueClosed = errors.New("queue is closed")
var ErrWaitTimeout = errors.New("WaitAll timed out waiting for tasks to complete")
