package queue_test

import (
	"fmt"
	"testing"

	"go.codycody31.dev/gobullmq"
)

func TestQueues(t *testing.T) {
	t.Run("when empty name is provided", func(t *testing.T) {
		_, err := gobullmq.NewQueue("", gobullmq.QueueOption{
			RedisIp:     "127.0.0.1:6379",
			RedisPasswd: "",
		})

		if err == nil {
			fmt.Println(err)
			t.Fatal("expected error, got none")
		}
	})
}
