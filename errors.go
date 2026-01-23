package gobullmq

import "errors"

// Sentinel error types for proper error handling
var (
	ErrRateLimit       = errors.New("bullmq:rateLimitExceeded")
	ErrDelayed         = errors.New("bullmq:delayed")
	ErrWaitingChildren = errors.New("bullmq:waitingChildren")
)

// Backwards compatibility - deprecated, use sentinel errors with errors.Is() instead
var (
	RateLimitError = ErrRateLimit.Error()
)
