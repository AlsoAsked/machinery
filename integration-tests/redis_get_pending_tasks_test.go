package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestRedisGetPendingTasks(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Lock:          fmt.Sprintf("redis://%v", redisURL),
	})
	pendingMessages, err := server.GetBroker().GetPendingTasks(context.TODO(), server.GetConfig().DefaultQueue)
	if err != nil {
		t.Error(err)
	}
	if len(pendingMessages) != 0 {
		t.Errorf(
			"%d pending messages, should be %d",
			len(pendingMessages),
			0,
		)
	}
}
