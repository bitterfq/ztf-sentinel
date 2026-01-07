package domains

import "time"

type Alert struct {
	ID                       string
	ObjectID                 string
	Data                     []byte // raw JSON
	GMag                     float64
	RMag                     float64
	LatestDetection          float64 // Julian date
	FirstDetection           float64 // Julian date
	BrighteningRate          float64
	DetectionsLast7d         int
	Classification           string
	ClassificationConfidence float64
	ReceivedAt               time.Time
}
