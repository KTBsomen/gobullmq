package types

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

type QueueWithOption func(o *JobOptions)
