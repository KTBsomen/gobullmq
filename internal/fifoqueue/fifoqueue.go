package fifoqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// FifoQueue handles asynchronous FIFO tasks with thread-safe operations.
type FifoQueue[T any] struct {
	queue        chan T         // Channel to queue items
	errors       chan error     // Channel for errors
	pending      sync.WaitGroup // WaitGroup to manage pending tasks
	ignoreErrors bool           // Flag to ignore errors
	isClosed     atomic.Bool    // Thread-safe flag for queue state
	pendingCount atomic.Int32   // Thread-safe counter for pending tasks
	mu           sync.RWMutex   // Mutex for thread-safe operations
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewFifoQueue initializes a new FifoQueue with proper buffering and context management.
func NewFifoQueue[T any](bufferSize int, ignoreErrors bool) *FifoQueue[T] {
	ctx, cancel := context.WithCancel(context.Background())
	q := &FifoQueue[T]{
		queue:        make(chan T, bufferSize),
		errors:       make(chan error, bufferSize),
		ignoreErrors: ignoreErrors,
		ctx:          ctx,
		cancel:       cancel,
	}
	q.pendingCount.Store(0)
	return q
}

// Add adds a new task to the queue with proper error handling and context cancellation.
func (q *FifoQueue[T]) Add(task func() (T, error)) error {
	if q.isClosed.Load() {
		return ErrQueueClosed
	}

	q.pending.Add(1)
	q.pendingCount.Add(1)

	go func() {
		defer func() {
			q.pending.Done()
			q.pendingCount.Add(-1)
		}()

		select {
		case <-q.ctx.Done():
			return
		default:
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
	}()

	return nil
}

// Fetch retrieves the next item from the queue with context cancellation support.
func (q *FifoQueue[T]) Fetch(ctx context.Context) (*T, error) {
	if q.isClosed.Load() && q.NumTotal() == 0 {
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

// WaitAll waits until all tasks are completed and properly closes the queue.
func (q *FifoQueue[T]) WaitAll() {
	q.mu.Lock()
	if !q.isClosed.Load() {
		q.isClosed.Store(true)
		q.cancel()
		q.pending.Wait()
		close(q.queue)
		close(q.errors)
	}
	q.mu.Unlock()
}

// NumPending returns the number of pending tasks.
func (q *FifoQueue[T]) NumPending() int {
	return int(q.pendingCount.Load())
}

// NumQueued returns the number of items in the queue.
func (q *FifoQueue[T]) NumQueued() int {
	return len(q.queue)
}

// NumTotal returns the total number of tasks.
func (q *FifoQueue[T]) NumTotal() int {
	return q.NumPending() + q.NumQueued()
}

// IsClosed returns whether the queue is closed.
func (q *FifoQueue[T]) IsClosed() bool {
	return q.isClosed.Load()
}

var ErrQueueClosed = errors.New("queue is closed")
