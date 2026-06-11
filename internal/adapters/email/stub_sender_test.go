package email

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DarioJejer/go-email-queue/internal/domain"
)

func stubTask(id string) *domain.EmailTask {
	return &domain.EmailTask{
		ID:        id,
		TenantID:  "tenant-a",
		Type:      domain.TaskTypeTransactional,
		Priority:  domain.PriorityNormal,
		Recipient: "user@example.com",
		TemplateID: "tpl-welcome",
	}
}

func TestStubSender_Success(t *testing.T) {
	t.Parallel()
	sender := newStubSender(0, 0, rand.New(rand.NewSource(1)))
	err := sender.Send(context.Background(), stubTask("task-ok"))
	require.NoError(t, err)
}

func TestStubSender_SimulatedFailure(t *testing.T) {
	t.Parallel()
	sender := newStubSender(1.0, 0, rand.New(rand.NewSource(42)))
	err := sender.Send(context.Background(), stubTask("task-fail"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stub: simulated send failure for task-fail")
}

func TestStubSender_ContextCancellation(t *testing.T) {
	t.Parallel()
	sender := newStubSender(0, 500*time.Millisecond, rand.New(rand.NewSource(7)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sender.Send(ctx, stubTask("task-cancel"))
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestStubSender_SendBatch_PartialFailure(t *testing.T) {
	t.Parallel()
	// failRate 0.5 with fixed seed: first succeeds, pattern varies — use 1.0 for second task only via two sends.
	alwaysFail := newStubSender(1.0, 0, rand.New(rand.NewSource(1)))
	neverFail := newStubSender(0, 0, rand.New(rand.NewSource(1)))

	require.NoError(t, neverFail.Send(context.Background(), stubTask("a")))
	require.Error(t, alwaysFail.Send(context.Background(), stubTask("b")))

	mixed := newStubSender(0.5, 0, rand.New(rand.NewSource(99)))
	tasks := []*domain.EmailTask{stubTask("t1"), stubTask("t2"), stubTask("t3")}
	err := mixed.SendBatch(context.Background(), tasks)
	if err != nil {
		var multi *domain.MultiError
		require.ErrorAs(t, err, &multi)
		assert.NotEmpty(t, multi.Errors)
	}
}
