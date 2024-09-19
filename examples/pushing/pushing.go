package main

import (
	"encoding/json"
	"fmt"

	"go.codycody31.dev/gobullmq"
)

// Person represents the structure for the job data.
type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	// Create a new JobQueue
	jobQueue, err := gobullmq.NewQueue(gobullmq.QueueOption{
		QueueName:   "emailJobs",
		KeyPrefix:   "awd:bull",
		RedisIp:     "127.0.0.1:6379",
		RedisPasswd: "",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	// jobQueue.Pause()
	// fmt.Println(jobQueue.IsPaused())
	// jobQueue.Resume()
	// fmt.Println(jobQueue.IsPaused())

	jobQueue.On("waiting", func(args ...interface{}) {
		job, _ := args[0].(gobullmq.Job)
		fmt.Printf("Job is waiting: %s\n", job.Id)
	})

	// Create job data
	person := Person{
		Name: "John Doe",
		Age:  30,
	}

	jobData, err := json.Marshal(person)
	if err != nil {
		fmt.Println("failed to serialize job data:", err)
		return
	}

	// Submit the job to the queue
	fmt.Println("Submitting job...")
	_, err = jobQueue.Add("person", jobData)
	if err != nil {
		fmt.Println("error submitting job:", err)
		return
	}

	fmt.Println("Job submitted successfully")

	select {}
}
