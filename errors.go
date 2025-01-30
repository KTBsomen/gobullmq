package gobullmq

import "fmt"

var (
	RateLimitError = "bullmq:rateLimitExceeded"
)

type StandardError struct {
	OriginalError error
	Message       string
}

func (e *StandardError) Error() string {
	return fmt.Sprintf("%s: %v", e.Message, e.OriginalError)
}

func wrapError(err error, message string) error {
	return &StandardError{
		OriginalError: err,
		Message:       message,
	}
}
