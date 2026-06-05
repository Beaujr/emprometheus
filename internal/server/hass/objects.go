package hass

type Response struct {
	EntityID    string     `json:"entity_id"`
	State       string     `json:"state"`
	Attributes  Attributes `json:"attributes"`
	LastChanged string     `json:"last_changed"`
	LastUpdated string     `json:"last_updated"`
}
type Attributes struct {
	StateClass        string `json:"state_class"`
	UnitOfMeasurement string `json:"unit_of_measurement"`
	DeviceClass       string `json:"device_class"`
	FriendlyName      string `json:"friendly_name"`
}
