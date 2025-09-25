package eventprocessor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/nivedita-verma/event-processor/pkg/eventspec"
	"go.uber.org/zap"
)

type Handler struct {
	logger  *zap.SugaredLogger
	service ServiceApi
}

func NewHandler(logger *zap.SugaredLogger, service ServiceApi) *Handler {
	return &Handler{
		logger:  logger,
		service: service,
	}
}

type ServiceApi interface {
	Process(context.Context, eventspec.Event) error
}

func (h *Handler) HandleSQSEvent(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
	sqsEventResponse := events.SQSEventResponse{
		BatchItemFailures: []events.SQSBatchItemFailure{},
	}
	for _, message := range sqsEvent.Records {
		h.logger.Infof("Received message ID: %s, from source %s", message.MessageId, message.EventSource)
		event, err := h.validateSQSMessage(message)
		if err != nil {
			h.logger.Errorf("failed to validate SQS message: %v", err)
			sqsEventResponse.BatchItemFailures = append(sqsEventResponse.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: message.MessageId})
			continue
		}

		if err := h.service.Process(ctx, *event); err != nil {
			h.logger.Errorf("failed to process event ID %s: %v", event.EventID, err)
			sqsEventResponse.BatchItemFailures = append(sqsEventResponse.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: message.MessageId})
			continue
		}
	}

	return sqsEventResponse, nil
}

func (h *Handler) validateSQSMessage(message events.SQSMessage) (*eventspec.Event, error) {
	h.logger.Infof("Validating message ID: %s", message.MessageId)
	if message.Body == "" {
		return nil, fmt.Errorf("empty message body")
	}

	event := &eventspec.Event{}
	if err := json.Unmarshal([]byte(message.Body), event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message body: %w", err)
	}

	if event.EventID == "" || event.ClientID == "" || event.Type == "" || event.Data == nil {
		return nil, fmt.Errorf("missing required event fields")
	}

	if !eventspec.IsValidEventType(event.Type) {
		return nil, fmt.Errorf("unsupported event type: %s", event.Type)
	}

	return event, nil
}
