/**
 * @Description:
 * @FilePath: /bull-golang/common.go
 * @Author: liyibing liyibing@lixiang.com
 * @Date: 2023-07-28 10:25:57
 */
package gobullmq

type withOption func(o *JobOptions)

/**
 * @description: set priority, 0 is highest and default value
 * @param {int} priority
 * @return {*}
 */
func WithPriorityOp(priority int) withOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.Priority = priority
	}
}

/**
 * @description: false is default
 * @param {bool} flag
 * @return {*}
 */
func WithRemoveOnCompleteOp(flag bool) withOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.RemoveOnComplete = flag
	}
}

/**
 * @description: false is default
 * @param {bool} flag
 * @return {*}
 */
func WithRemoveOnFailOp(flag bool) withOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.RemoveOnFail = flag
	}
}

/**
 * @description: set attemp times and 1 is default
 * @param {int} times
 * @return {*}
 */
func WithAttemptsOp(times int) withOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.Attempts = times
	}
}

/**
 * @description: set delay time and 0 is default
 * @param {int} delayTime
 * @return {*}
 */
func WithDelayOp(delayTime int) withOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.Delay = delayTime
	}
}

/**
 * @description:
 * @param {int64} timeStamp by time.Now().UnixMilli()
 * @return {*}
 */
func WithTimeStamp(timeStamp int64) withOption {
	return func(o *JobOptions) {
		if o == nil {
			return
		}
		o.TimeStamp = timeStamp
	}
}
