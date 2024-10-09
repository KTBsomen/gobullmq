<!--
 * @Description: README.md
 * @FilePath: /bull-golang/README.md
 * @Author: liyibing liyibing@lixiang.com
 * @Date: 2023-07-28 11:23:27
-->
# bull-golang

bull-js golang 版本

使用方法

样例见./example/example.go

导入包后使用定义结构体添加bull.QueueIface，该接口定义了Add方法
接受QueueOption将队列初始化，Option包括keyPrefix与QueueName与redis地址与登录密码

Add方法接受一个json格式的JobData与一系列提供的初始化方法
初始化方法是可选的，当前支持
WithPriorityOp(priority int)
WithRemoveOnCompleteOp(flag bool)
WithRemoveOnFailOp(flag bool)
WithAttemptsOp(times int)
WithDelayOp(delayTime int)

## Notes

Repeating code is fine, this isn't meant to 1-to-1 match the original BullMQ library. We're trying to make a library that is idiomatic to Go, and that means we're going to have to make some changes to the way things are done.

Not only that, but we only need to be compat with the version it's based on.
We can/plan on expanding to include things we want, where at that point we will rebrand, and leave behind the legacy code for those who wish to work with BulMQ version X.X.X in Golang.

## Priorities for Development

- [ ] Build a proper list of all the basic things we want to implement from BullMQ, so we have an easy list/set of items to work off of
- [ ] Redesign the way the redis connection is configured
  - Remove support for a redis cluster (for now)
  - Setup pool connections to prevent reconnecting to redis
  - 
- [ ] push to queues (Queue)
- [ ] get and react to queue events (QueueEvents)
- [ ] worker support for processing jobs (Worker)
- [ ] support parent/child jobs (Job/Queue)
- [ ] support delayed/repeating jobs (Job/Queue)
- [ ] analytics
    - when in debug
        - support logging of all events
        - api for debugging memory usage, metrics, etc