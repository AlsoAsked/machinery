package redis_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends/iface"

	"github.com/RichardKnop/machinery/v1/backends/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func getRedisG() iface.Backend {
	// host1:port1,host2:port2
	redisURL := os.Getenv("REDIS_URL_GR")
	//redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return nil
	}
	backend := redis.NewGR(new(config.Config), strings.Split(redisURL, ","), 0)
	return backend
}

func TestGroupCompletedGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
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

	// Cleanup before the test
	backend.PurgeState(context.TODO(), task1.UUID)
	backend.PurgeState(context.TODO(), task2.UUID)
	backend.PurgeGroupMeta(context.TODO(), groupUUID)

	groupCompleted, err := backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.InitGroup(context.TODO(), groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, err = backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
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

func TestGetStateGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend.PurgeState(context.TODO(), "testTaskUUID")

	var (
		taskState *tasks.TaskState
		err       error
	)

	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.Equal(t, "redis: nil", err.Error())
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

func TestPurgeStateGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend.SetStatePending(context.TODO(), signature)
	taskState, err := backend.GetState(context.TODO(), signature.UUID)
	assert.NotNil(t, taskState)
	assert.NoError(t, err)

	backend.PurgeState(context.TODO(), taskState.TaskUUID)
	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.Nil(t, taskState)
	assert.Error(t, err)
}
