package eventspec

type Event struct {
	EventID  string                 `json:"eventId" validate:"required"`
	ClientID string                 `json:"clientId" validate:"required"`
	Type     string                 `json:"type" validate:"required"`
	Data     map[string]interface{} `json:"data" validate:"required"`
}
