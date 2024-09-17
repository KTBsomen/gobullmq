package eventemitter

type EventEmitterIface interface {
	On(event string, listener func(...interface{}))
	Once(event string, listener func(...interface{}))
	Emit(event string, args ...interface{})
	RemoveListener(event string, listener func(...interface{}))
	RemoveAllListeners(event string)
}

type EventEmitter struct {
	eventChan map[string]chan []interface{}
}

func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		eventChan: make(map[string]chan []interface{}),
	}
}

func (e *EventEmitter) Init() {
	e.eventChan = make(map[string]chan []interface{})
}

func (e *EventEmitter) On(event string, listener func(...interface{})) {
	if _, exists := e.eventChan[event]; !exists {
		e.eventChan[event] = make(chan []interface{})
	}
	go func() {
		for {
			args := <-e.eventChan[event]
			listener(args...)
		}
	}()
}

func (e *EventEmitter) Once(event string, listener func(...interface{})) {
	if _, exists := e.eventChan[event]; !exists {
		e.eventChan[event] = make(chan []interface{})
	}
	go func() {
		args := <-e.eventChan[event]
		listener(args...)
	}()
}

func (e *EventEmitter) RemoveListener(event string, listener func(...interface{})) {
	if ch, exists := e.eventChan[event]; exists {
		close(ch)
		delete(e.eventChan, event)
	}
}

func (e *EventEmitter) RemoveAllListeners(event string) {
	if ch, exists := e.eventChan[event]; exists {
		close(ch)
		delete(e.eventChan, event)
	}
}

func (e *EventEmitter) Emit(event string, args ...interface{}) {
	if ch, exists := e.eventChan[event]; exists {
		go func() {
			ch <- args
		}()
	}
}
