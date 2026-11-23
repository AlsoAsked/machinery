package machinery_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/machinery/v1"
	eagerbackend "github.com/RichardKnop/machinery/v1/backends/eager"
	eagerbroker "github.com/RichardKnop/machinery/v1/brokers/eager"
	"github.com/RichardKnop/machinery/v1/config"
	eagerlock "github.com/RichardKnop/machinery/v1/locks/eager"
	"github.com/RichardKnop/machinery/v1/tasks"
)

type redeliveryFixture struct {
	worker   *machinery.Worker
	server   *machinery.Server
	calls    int
	failNext bool
}

func newRedeliveryFixture(t *testing.T) *redeliveryFixture {
	t.Helper()

	fixture := &redeliveryFixture{}
	fixture.server = machinery.NewServerWithBrokerBackendLock(&config.Config{}, eagerbroker.New(), eagerbackend.New(), eagerlock.New())
	require.NoError(t, fixture.server.RegisterTask("count", func() (string, error) {
		fixture.calls++
		if fixture.failNext {
			fixture.failNext = false
			return "", errors.New("boom")
		}
		return "done", nil
	}))
	fixture.worker = fixture.server.NewWorker("test", 1)

	return fixture
}

func (f *redeliveryFixture) signature(uuid string) *tasks.Signature {
	return &tasks.Signature{UUID: uuid, Name: "count"}
}

func TestProcessSkipsATaskThatAlreadySucceeded(t *testing.T) {
	t.Parallel()

	fixture := newRedeliveryFixture(t)

	require.NoError(t, fixture.worker.Process(context.Background(), fixture.signature("task_succeeded")))
	require.NoError(t, fixture.worker.Process(context.Background(), fixture.signature("task_succeeded")))

	assert.Equal(t, 1, fixture.calls)

	state, err := fixture.server.GetBackend().GetState(context.Background(), "task_succeeded")
	require.NoError(t, err)
	assert.Equal(t, tasks.StateSuccess, state.State)
}

func TestProcessSkipsATaskThatAlreadyFailed(t *testing.T) {
	t.Parallel()

	fixture := newRedeliveryFixture(t)
	fixture.failNext = true

	require.NoError(t, fixture.worker.Process(context.Background(), fixture.signature("task_failed")))
	require.NoError(t, fixture.worker.Process(context.Background(), fixture.signature("task_failed")))

	assert.Equal(t, 1, fixture.calls)

	state, err := fixture.server.GetBackend().GetState(context.Background(), "task_failed")
	require.NoError(t, err)
	assert.Equal(t, tasks.StateFailure, state.State)
}

func TestProcessRunsATaskThatHasNotFinished(t *testing.T) {
	t.Parallel()

	for _, state := range []string{tasks.StatePending, tasks.StateReceived, tasks.StateStarted, tasks.StateRetry} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()

			fixture := newRedeliveryFixture(t)
			signature := fixture.signature("task_" + state)
			backend := fixture.server.GetBackend()

			switch state {
			case tasks.StatePending:
				require.NoError(t, backend.SetStatePending(context.Background(), signature))
			case tasks.StateReceived:
				require.NoError(t, backend.SetStateReceived(context.Background(), signature))
			case tasks.StateStarted:
				require.NoError(t, backend.SetStateStarted(context.Background(), signature))
			case tasks.StateRetry:
				require.NoError(t, backend.SetStateRetry(context.Background(), signature))
			}

			require.NoError(t, fixture.worker.Process(context.Background(), signature))
			assert.Equal(t, 1, fixture.calls)
		})
	}
}
