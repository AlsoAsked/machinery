package redis_test

import (
	"context"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestGroupCompleted(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	groupUUID := "testGroupUUID"
	task1 := &tasks.Signature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &tasks.Signature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	backend := redis.New(new(config.Config), redisURL, redisPassword, "", 0)

	// Cleanup before the test
	backend.PurgeState(context.TODO(), task1.UUID)
	backend.PurgeState(context.TODO(), task2.UUID)
	backend.PurgeGroupMeta(context.TODO(), groupUUID)

	groupCompleted, err := backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redigo: nil returned", err.Error())
	}

	backend.InitGroup(context.TODO(), groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, err = backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "Expected byte array, instead got: <nil>", err.Error())
	}

	backend.SetStatePending(context.TODO(), task1)
	backend.SetStateStarted(context.TODO(), task2)
	groupCompleted, err = backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	taskResults := []*tasks.TaskResult{new(tasks.TaskResult)}
	backend.SetStateStarted(context.TODO(), task1)
	backend.SetStateSuccess(context.TODO(), task2, taskResults)
	groupCompleted, err = backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	backend.SetStateFailure(context.TODO(), task1, "Some error")
	groupCompleted, err = backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.NoError(t, err) {
		assert.True(t, groupCompleted)
	}
}

func TestGetState(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := redis.New(new(config.Config), redisURL, redisPassword, "", 0)

	backend.PurgeState(context.TODO(), "testTaskUUID")

	var (
		taskState *tasks.TaskState
		err       error
	)

	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.Equal(t, "redigo: nil returned", err.Error())
	assert.Nil(t, taskState)

	//Pending State
	backend.SetStatePending(context.TODO(), signature)
	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	createdAt := taskState.CreatedAt

	//Received State
	backend.SetStateReceived(context.TODO(), signature)
	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	//Started State
	backend.SetStateStarted(context.TODO(), signature)
	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	//Success State
	taskResults := []*tasks.TaskResult{
		{
			Type:  "float64",
			Value: 2,
		},
	}
	backend.SetStateSuccess(context.TODO(), signature, taskResults)
	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)
	assert.NotNil(t, taskState.Results)
}

func TestPurgeState(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := redis.New(new(config.Config), redisURL, redisPassword, "", 0)

	backend.SetStatePending(context.TODO(), signature)
	taskState, err := backend.GetState(context.TODO(), signature.UUID)
	assert.NotNil(t, taskState)
	assert.NoError(t, err)

	backend.PurgeState(context.TODO(), taskState.TaskUUID)
	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.Nil(t, taskState)
	assert.Error(t, err)
}
