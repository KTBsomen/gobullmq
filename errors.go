/**
 * @Description:
 * @FilePath: /bull-golang/errors.go
 * @Author: liyibing liyibing@lixiang.com
 * @Date: 2023-07-19 17:11:08
 */
package gobullmq

import "fmt"

var (
// LimiterError = errors.New("Limiter requires `max` and `duration` options")
// NameError    = errors.New("empty name")
// InitUrlErr   = errors.New("bad url input in queue init")
// RedisIPErr   = errors.New("wrong with redis ip")
// EnvSetErr    = errors.New("undefined env")
)

type MyError struct {
	OriginalError error
	Message       string
}

func (e *MyError) Error() string {
	return fmt.Sprintf("%s: %v", e.Message, e.OriginalError)
}

// 使用这个函数来包装外部库产生的错误
func wrapError(err error, message string) error {
	return &MyError{
		OriginalError: err,
		Message:       message,
	}
}
