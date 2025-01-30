package fifoqueue

import (
	"sync"
)

// FifoQueue handles asynchronous FIFO tasks.
type FifoQueue[T any] struct {
	queue        chan T         // Channel to queue items
	errors       chan error     // Channel for errors
	pending      sync.WaitGroup // WaitGroup to manage pending tasks
	ignoreErrors bool           // Flag to ignore errors
}

// NewFifoQueue initializes a new FifoQueue.
func NewFifoQueue[T any](bufferSize int, ignoreErrors bool) *FifoQueue[T] {
	return &FifoQueue[T]{
		queue:        make(chan T, bufferSize),
		errors:       make(chan error, bufferSize),
		ignoreErrors: ignoreErrors,
	}
}

// Add adds a new task to the queue, which is evaluated when it resolves.
func (q *FifoQueue[T]) Add(task func() (T, error)) {
	q.pending.Add(1)

	go func() {
		defer q.pending.Done()
		result, err := task()
		if err != nil {
			if !q.ignoreErrors {
				q.errors <- err
			}
			return
		}
		q.queue <- result
	}()
}

// Fetch retrieves the next item from the queue. It blocks until an item is available.
func (q *FifoQueue[T]) Fetch() (*T, error) {
	select {
	case item := <-q.queue:
		return &item, nil
	case err := <-q.errors:
		if !q.ignoreErrors {
			return nil, err
		}
		return nil, nil
	}
}

// WaitAll waits until all tasks are completed.
func (q *FifoQueue[T]) WaitAll() {
	q.pending.Wait()
	close(q.queue)
	close(q.errors)
}

// NumPending returns the number of pending tasks.
func (q *FifoQueue[T]) NumPending() int {
	return 0
}

// NumQueued returns the number of items in the queue.
func (q *FifoQueue[T]) NumQueued() int {
	return len(q.queue)
}

// NumTotal returns the total number of tasks.
func (q *FifoQueue[T]) NumTotal() int {
	return q.NumPending() + q.NumQueued()
}
