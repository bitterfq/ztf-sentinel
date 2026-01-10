package domains

import "time"

type Alert struct {
	ID                       string
	ObjectID                 string  `json:"objectId"`
	Data                     []byte  `json:"data"`
	GMag                     float64 `json:"g_magnitude"`
	RMag                     float64 `json:"r_magnitude"`
	LatestDetection          float64 `json:"latest_detection"`
	FirstDetection           float64 `json:"first_detection"`
	BrighteningRate          float64 `json:"brightening_rate"`
	DetectionsLast7d         int     `json:"detections_last_7d"`
	Classification           string  `json:"predicted_classification"`
	ClassificationConfidence float64 `json:"prediction_confidence"`
	ReceivedAt               time.Time
}
