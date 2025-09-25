package eventprocessor

import (
	"context"
	"testing"

	"github.com/nivedita-verma/event-processor/pkg/eventspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_NewService(t *testing.T) {
	store := &mockStore{}
	service := NewService(store)
	assert.Equal(t, store, service.store)
}

type mockStore struct {
	mock.Mock
}

func (m *mockStore) Persist(ctx context.Context, event eventspec.Event) error {
	args := m.Called(ctx, event)
	if args.Get(0) == nil {
		return nil
	}
	return args.Error(0)
}

func Test_Service_Process(t *testing.T) {
	t.Run("when store.Persist returns an error", func(t *testing.T) {
		store := &mockStore{}
		service := NewService(store)
		event := eventspec.Event{EventID: "1", ClientID: "client-1", Type: "TestEvent", Data: map[string]interface{}{"key": "value"}}

		store.On("Persist", mock.Anything, event).Return(assert.AnError)

		err := service.Process(context.Background(), event)
		t.Run("should return the Persist error", func(t *testing.T) {
			assert.Error(t, err)
			store.AssertExpectations(t)
		})
	})

	t.Run("when store.Persist is successful", func(t *testing.T) {
		store := &mockStore{}
		service := NewService(store)
		event := eventspec.Event{EventID: "1", ClientID: "client-1", Type: "TestEvent", Data: map[string]interface{}{"key": "value"}}

		store.On("Persist", mock.Anything, event).Return(nil)

		err := service.Process(context.Background(), event)

		t.Run("should complete without error", func(t *testing.T) {
			assert.NoError(t, err)
			store.AssertExpectations(t)
		})
	})
}
