# BullMQ for Golang

BullMQ for Golang is a powerful and flexible job queue library that allows you to manage and process jobs using Redis. It provides a robust set of features for creating, processing, and managing jobs in a distributed environment.

## Features

- **Queue Management**: Create and manage job queues with ease.
- **Worker Processing**: Define workers to process jobs concurrently.
- **Event Handling**: Listen to and emit events for job lifecycle management.
- **Repeatable Jobs**: Schedule jobs to run at regular intervals.
- **Job Options**: Configure job behavior with flexible options.

## Installation

To install BullMQ for Golang, use the following command:

```bash
go get go.codycody31.dev/gobullmq
```

## Usage

### Queue

Create a new queue and add jobs to it:

```go
ctx := context.Background()
queue, err := gobullmq.NewQueue(ctx, "myQueue", gobullmq.QueueOption{
    RedisIp:     "127.0.0.1:6379",
    RedisPasswd: "",
})
if err != nil {
    log.Fatal(err)
}

jobData := types.JobData{"foo": "bar"}
_, err = queue.Add("myJob", jobData, gobullmq.JobOptionWithPriority(1))
if err != nil {
    log.Fatal(err)
}
```

### Worker

Define a worker to process jobs from the queue:

```go
workerProcess := func(ctx context.Context, job *types.Job) (interface{}, error) {
    fmt.Printf("Processing job: %s\n", job.Name)
    return nil, nil
}

worker, err := gobullmq.NewWorker(ctx, "myQueue", gobullmq.WorkerOptions{
    Concurrency:     1,
    StalledInterval: 30000,
}, redis.NewClient(&redis.Options{
    Addr:     "127.0.0.1:6379",
    Password: "",
}), workerProcess)
if err != nil {
    log.Fatal(err)
}

worker.Run()
worker.Wait()
```

### QueueEvents

Listen to events emitted by the queue:

```go
events, err := gobullmq.NewQueueEvents(ctx, "myQueue", gobullmq.QueueEventsOptions{
    RedisClient: *redis.NewClient(&redis.Options{
        Addr:     "127.0.0.1:6379",
        Password: "",
        DB:       0,
    }),
    Autorun: true,
})
if err != nil {
    log.Fatal(err)
}

events.On("added", func(args ...interface{}) {
    fmt.Println("Job added:", args)
})

events.On("error", func(args ...interface{}) {
    fmt.Println("Error event:", args)
})
```

## Configuration

### Queue Options

- `RedisIp`: The IP address of the Redis server.
- `RedisPasswd`: The password for the Redis server.
- `KeyPrefix`: The prefix for Redis keys.

### Worker Options

- `Concurrency`: The number of concurrent jobs the worker can process.
- `StalledInterval`: The interval for checking stalled jobs.

### QueueEvents Options

- `RedisClient`: The Redis client used for connecting to the Redis server.
- `Autorun`: Whether to automatically start listening for events.

## Examples

### Adding a Repeatable Job

```go
_, err = queue.Add("myRepeatableJob", jobData, gobullmq.JobOptionWithRepeat(types.JobRepeatOptions{
    Every: 1000,
}))
if err != nil {
    log.Fatal(err)
}
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
