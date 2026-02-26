package eventemitter

import "sync"

type EventEmitterIface interface {
	On(event string, listener func(...interface{}))
	Once(event string, listener func(...interface{}))
	Emit(event string, args ...interface{})
	RemoveListener(event string, listener func(...interface{}))
	RemoveAllListeners(event string)
}

var _ EventEmitterIface = (*EventEmitter)(nil)

type EventEmitter struct {
	mu        sync.RWMutex
	eventChan map[string]chan []interface{}
}

func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		eventChan: make(map[string]chan []interface{}),
	}
}

func (e *EventEmitter) Init() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.eventChan = make(map[string]chan []interface{})
}

func (e *EventEmitter) On(event string, listener func(...interface{})) {
	e.mu.Lock()
	if _, exists := e.eventChan[event]; !exists {
		e.eventChan[event] = make(chan []interface{})
	}
	ch := e.eventChan[event]
	e.mu.Unlock()

	go func() {
		for {
			args, ok := <-ch
			if !ok {
				return // Channel closed, exit goroutine
			}
			listener(args...)
		}
	}()
}

func (e *EventEmitter) Once(event string, listener func(...interface{})) {
	e.mu.Lock()
	if _, exists := e.eventChan[event]; !exists {
		e.eventChan[event] = make(chan []interface{})
	}
	ch := e.eventChan[event]
	e.mu.Unlock()

	go func() {
		args, ok := <-ch
		if !ok {
			return // Channel closed
		}
		listener(args...)
	}()
}

func (e *EventEmitter) RemoveListener(event string, listener func(...interface{})) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ch, exists := e.eventChan[event]; exists {
		close(ch)
		delete(e.eventChan, event)
	}
}

func (e *EventEmitter) RemoveAllListeners(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ch, exists := e.eventChan[event]; exists {
		close(ch)
		delete(e.eventChan, event)
	}
}

func (e *EventEmitter) Emit(event string, args ...interface{}) {
	e.mu.RLock()
	ch, exists := e.eventChan[event]
	e.mu.RUnlock()

	if exists {
		go func() {
			defer func() {
				// Recover from panic if channel is closed during send
				recover()
			}()
			ch <- args
		}()
	}
}
