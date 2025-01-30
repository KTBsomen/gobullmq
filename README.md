# bull-golang

After importing the package, use the defined structure to add bull.QueueIface. The interface defines the Add method. 
Accepts QueueOption to initialize the queue. Option includes keyPrefix, QueueName, redis address, and login password.

The Add method accepts a JobData in json format and a series of provided initialization methods. 
The initialization method is optional and currently supports
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
  - Queue.Add
  - Queue.AddBulk
  - Queue.Close (prob don't)
  - Queue.UpdateJobProgress
  - Queue.AddJobLog
  - Queue.RetryJobs
  - Queue.promoteJobs
- [ ] Redesign the way the redis connection is configured
  - Remove support for a redis cluster (for now)
  - Setup pool connections to prevent reconnecting to redis
  - 
- [ ] push to queues (Queue)
- [x] get and react to queue events (QueueEvents)
- [ ] worker support for processing jobs (Worker)
- [ ] support parent/child jobs (Job/Queue)
- [ ] support delayed/repeating jobs (Job/Queue)
- [ ] analytics
    - when in debug
        - support logging of all events
        - api for debugging memory usage, metrics, etc