package imagefetcher

// Job struct
// Each Job has
// ObjectID representing the ID of the item
// AlertID fk to Alerts table, representing the unique ID for Alerts
type Job struct {
	AlertID  string
	ObjectID string
}
