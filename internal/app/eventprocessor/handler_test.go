package eventprocessor

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/nivedita-verma/event-processor/pkg/eventspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func Test_NewHandler(t *testing.T) {
	store := &mockStore{}
	service := NewService(store)
	handler := NewHandler(zap.NewNop().Sugar(), service)
	assert.Equal(t, service, handler.service)
	assert.NotNil(t, handler.logger)
}

func Test_Handler_validateSQSMessage(t *testing.T) {
	t.Run("when SQS message body is empty", func(t *testing.T) {
		service := &mockService{}
		handler := NewHandler(zap.NewNop().Sugar(), service)

		_, err := handler.validateSQSMessage(events.SQSMessage{Body: ""})

		t.Run("should return validation error", func(t *testing.T) {
			assert.Error(t, err)
			assert.Equal(t, "empty message body", err.Error())
		})
	})

	t.Run("when SQS message body is valid JSON", func(t *testing.T) {
		service := &mockService{}
		handler := NewHandler(zap.NewNop().Sugar(), service)

		event, err := handler.validateSQSMessage(events.SQSMessage{Body: `{"eventId":"1","clientId":"client-1","type":"notification","data":{"key":"value"}}`, MessageId: "msg-1"})

		t.Run("should complete without error", func(t *testing.T) {
			assert.NoError(t, err)
		})

		t.Run("should return the correct event", func(t *testing.T) {
			expectedEvent := &eventspec.Event{EventID: "1", ClientID: "client-1", Type: "notification", Data: map[string]interface{}{"key": "value"}}
			assert.Equal(t, expectedEvent, event)
		})
	})

	t.Run("when SQS message body is invalid JSON", func(t *testing.T) {
		service := &mockService{}
		handler := NewHandler(zap.NewNop().Sugar(), service)

		_, err := handler.validateSQSMessage(events.SQSMessage{Body: `{"eventId":"1","clientId":"client-1","type":"monitoringAlert","data":{"key":"value"`, MessageId: "msg-1"})

		t.Run("should return unmarshalling error", func(t *testing.T) {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to unmarshal message body")
		})
	})

	t.Run("when SQS message has unsupported event type", func(t *testing.T) {
		service := &mockService{}
		handler := NewHandler(zap.NewNop().Sugar(), service)

		_, err := handler.validateSQSMessage(events.SQSMessage{Body: `{"eventId":"1","clientId":"client-1","type":"UnsupportedEvent","data":{"key":"value"}}`, MessageId: "msg-1"})

		t.Run("should return unsupported event type error", func(t *testing.T) {
			assert.Error(t, err)
			assert.Equal(t, "unsupported event type: UnsupportedEvent", err.Error())
		})
	})
}

type mockService struct {
	mock.Mock
}

func (m *mockService) Process(ctx context.Context, event eventspec.Event) error {
	args := m.Called(ctx, event)
	if args.Get(0) == nil {
		return nil
	}
	return args.Error(0)
}

func createSQSEvent(bodies []string) events.SQSEvent {
	records := make([]events.SQSMessage, len(bodies))
	for i, body := range bodies {
		records[i] = events.SQSMessage{
			MessageId:   fmt.Sprintf("msg-%d", i+1),
			Body:        body,
			EventSource: "aws:sqs",
		}
	}
	return events.SQSEvent{
		Records: records,
	}
}

func Test_Handler_HandleSQSEvent(t *testing.T) {
	t.Run("when error validating SQS message", func(t *testing.T) {
		service := &mockService{}
		handler := NewHandler(zap.NewNop().Sugar(), service)

		// Empty body to trigger validation error
		sqsEvent := createSQSEvent([]string{""})

		response, err := handler.HandleSQSEvent(context.Background(), sqsEvent)

		t.Run("then there should be no error", func(t *testing.T) {
			assert.NoError(t, err)
		})

		t.Run("then the response should include message in batch item failures", func(t *testing.T) {
			assert.NotEmpty(t, response.BatchItemFailures)
			assert.Equal(t, "msg-1", response.BatchItemFailures[0].ItemIdentifier)
		})
	})

	t.Run("when service.Process returns an error", func(t *testing.T) {
		service := &mockService{}
		handler := NewHandler(zap.NewNop().Sugar(), service)

		validBody := `{"eventId":"1","clientId":"client-1","type":"notification","data":{"key":"value"}}`
		sqsEvent := createSQSEvent([]string{validBody})

		event := eventspec.Event{EventID: "1", ClientID: "client-1", Type: "notification", Data: map[string]interface{}{"key": "value"}}
		service.On("Process", mock.Anything, event).Return(assert.AnError)

		response, err := handler.HandleSQSEvent(context.Background(), sqsEvent)

		t.Run("then there should be no error", func(t *testing.T) {
			assert.NoError(t, err)
		})

		t.Run("then the response should include message in batch item failures", func(t *testing.T) {
			assert.NotEmpty(t, response.BatchItemFailures)
			assert.Equal(t, "msg-1", response.BatchItemFailures[0].ItemIdentifier)
		})
	})

	t.Run("when service.Process is successful", func(t *testing.T) {
		service := &mockService{}
		handler := NewHandler(zap.NewNop().Sugar(), service)

		validBody := `{"eventId":"1","clientId":"client-1","type":"transaction","data":{"key":"value"}}`
		sqsEvent := createSQSEvent([]string{validBody})

		event := eventspec.Event{EventID: "1", ClientID: "client-1", Type: "transaction", Data: map[string]interface{}{"key": "value"}}
		service.On("Process", mock.Anything, event).Return(nil)

		response, err := handler.HandleSQSEvent(context.Background(), sqsEvent)

		t.Run("then there should be no error", func(t *testing.T) {
			assert.NoError(t, err)
		})

		t.Run("then the response should have no batch item failures", func(t *testing.T) {
			assert.Empty(t, response.BatchItemFailures)
		})

		service.AssertExpectations(t)
	})

	t.Run("when multiple SQS messages with mixed results", func(t *testing.T) {
		service := &mockService{}
		handler := NewHandler(zap.NewNop().Sugar(), service)

		validBody1 := `{"eventId":"1","clientId":"client-1","type":"notification","data":{"key":"value"}}`
		invalidBody := `{"eventId":"2","clientId":"client-2","type":"unsupported","data":{"key":"value"}}`
		validBody2 := `{"eventId":"3","clientId":"client-3","type":"transaction","data":{"key":"value"}}`
		sqsEvent := createSQSEvent([]string{validBody1, invalidBody, validBody2})

		event1 := eventspec.Event{EventID: "1", ClientID: "client-1", Type: "notification", Data: map[string]interface{}{"key": "value"}}
		event2 := eventspec.Event{EventID: "3", ClientID: "client-3", Type: "transaction", Data: map[string]interface{}{"key": "value"}}

		service.On("Process", mock.Anything, event1).Return(nil)
		service.On("Process", mock.Anything, event2).Return(nil)

		response, err := handler.HandleSQSEvent(context.Background(), sqsEvent)

		t.Run("then there should be no error", func(t *testing.T) {
			assert.NoError(t, err)
		})

		t.Run("then the response should include only the invalid messages in batch item failures", func(t *testing.T) {
			assert.Len(t, response.BatchItemFailures, 1)
			assert.Equal(t, "msg-2", response.BatchItemFailures[0].ItemIdentifier)
		})

		service.AssertExpectations(t)
	})
}
