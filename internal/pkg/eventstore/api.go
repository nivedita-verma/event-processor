package eventstore

import (
	"context"

	"github.com/nivedita-verma/event-processor/pkg/eventspec"
)

type Api interface {
	Persist(context.Context, eventspec.Event) error
}
