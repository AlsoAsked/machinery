package memcache_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/memcache"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestGroupCompleted(t *testing.T) {
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if memcacheURL == "" {
		t.Skip("MEMCACHE_URL is not defined")
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

	backend := memcache.New(new(config.Config), []string{memcacheURL})

	// Cleanup before the test
	backend.PurgeState(context.TODO(), task1.UUID)
	backend.PurgeState(context.TODO(), task2.UUID)
	backend.PurgeGroupMeta(context.TODO(), groupUUID)

	groupCompleted, err := backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "memcache: cache miss", err.Error())
	}

	backend.InitGroup(context.TODO(), groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, err = backend.GroupCompleted(context.TODO(), groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "memcache: cache miss", err.Error())
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
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if memcacheURL == "" {
		t.Skip("MEMCACHE_URL is not defined")
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := memcache.New(new(config.Config), []string{memcacheURL})

	go func() {
		backend.SetStatePending(context.TODO(), signature)
		time.Sleep(2 * time.Millisecond)
		backend.SetStateReceived(context.TODO(), signature)
		time.Sleep(2 * time.Millisecond)
		backend.SetStateStarted(context.TODO(), signature)
		time.Sleep(2 * time.Millisecond)
		taskResults := []*tasks.TaskResult{
			{
				Type:  "float64",
				Value: 2,
			},
		}
		backend.SetStateSuccess(context.TODO(), signature, taskResults)
	}()

	var (
		taskState *tasks.TaskState
		err       error
	)
	for {
		taskState, err = backend.GetState(context.TODO(), signature.UUID)
		if taskState == nil {
			assert.Equal(t, "memcache: cache miss", err.Error())
			continue
		}

		assert.NoError(t, err)
		if taskState.IsCompleted() {
			break
		}
	}
}

func TestPurgeState(t *testing.T) {
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if memcacheURL == "" {
		t.Skip("MEMCACHE_URL is not defined")
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := memcache.New(new(config.Config), []string{memcacheURL})

	backend.SetStatePending(context.TODO(), signature)
	taskState, err := backend.GetState(context.TODO(), signature.UUID)
	assert.NotNil(t, taskState)
	assert.NoError(t, err)

	backend.PurgeState(context.TODO(), taskState.TaskUUID)
	taskState, err = backend.GetState(context.TODO(), signature.UUID)
	assert.Nil(t, taskState)
	assert.Error(t, err)
}
