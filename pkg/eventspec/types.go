package eventspec

import "slices"

type EventType string

const (
	MonitoringAlert EventType = "monitoringAlert"
	Notification    EventType = "notification"
	Transaction     EventType = "transaction"
)

var ValidEventTypes = []EventType{
	MonitoringAlert,
	Notification,
	Transaction,
}

func IsValidEventType(eventType string) bool {
	return slices.Contains(ValidEventTypes, EventType(eventType))
}

func (e *EventType) String() string {
	return string(*e)
}
