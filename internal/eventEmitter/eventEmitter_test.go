package eventemitter

import (
	"sync"
	"testing"
	"time"
)

func TestEventEmitter_On(t *testing.T) {
	e := NewEventEmitter()
	var wg sync.WaitGroup
	wg.Add(1)

	e.On("testEvent", func(args ...interface{}) {
		if args[0].(string) != "test" {
			t.Errorf("Expected 'test', got %v", args[0])
		}
		wg.Done()
	})

	e.Emit("testEvent", "test")
	wg.Wait()
}

func TestEventEmitter_RemoveListener(t *testing.T) {
	e := NewEventEmitter()
	var triggered bool

	listener := func(args ...interface{}) {
		triggered = true
	}

	e.On("testRemoveEvent", listener)
	e.RemoveListener("testRemoveEvent", listener)
	e.Emit("testRemoveEvent", "test")

	time.Sleep(100 * time.Millisecond)

	if triggered {
		t.Errorf("Listener should have been removed but it was triggered")
	}
}

func TestEventEmitter_RemoveAllListeners(t *testing.T) {
	e := NewEventEmitter()
	var triggered1, triggered2 bool

	listener1 := func(args ...interface{}) {
		triggered1 = true
	}
	listener2 := func(args ...interface{}) {
		triggered2 = true
	}

	e.On("testRemoveAllEvent", listener1)
	e.On("testRemoveAllEvent", listener2)

	e.RemoveAllListeners("testRemoveAllEvent")
	e.Emit("testRemoveAllEvent", "test")

	time.Sleep(100 * time.Millisecond)

	if triggered1 || triggered2 {
		t.Errorf("Listeners should have been removed but at least one was triggered")
	}
}

func TestEventEmitter_Emit(t *testing.T) {
	e := NewEventEmitter()
	var wg sync.WaitGroup
	wg.Add(1)

	e.On("testEmitEvent", func(args ...interface{}) {
		if args[0].(string) != "emit" {
			t.Errorf("Expected 'emit', got %v", args[0])
		}
		wg.Done()
	})

	e.Emit("testEmitEvent", "emit")
	wg.Wait()
}

func TestEventEmitter_Init(t *testing.T) {
	e := NewEventEmitter()

	e.On("testEvent", func(args ...interface{}) {})
	if len(e.eventChan) == 0 {
		t.Errorf("Expected eventChan to have registered events")
	}

	e.Init()

	if len(e.eventChan) != 0 {
		t.Errorf("Expected eventChan to be re-initialized and empty")
	}
}
