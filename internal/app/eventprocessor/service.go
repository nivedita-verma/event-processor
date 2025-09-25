package eventprocessor

import (
	"context"

	"github.com/nivedita-verma/event-processor/internal/pkg/eventstore"
	"github.com/nivedita-verma/event-processor/pkg/eventspec"
)

type Service struct {
	store eventstore.Api
}

func NewService(store eventstore.Api) *Service {
	return &Service{
		store: store,
	}
}

func (s *Service) Process(ctx context.Context, event eventspec.Event) error {
	// Add any business logic or transformations here

	// Persist the event
	return s.store.Persist(ctx, event)
}
