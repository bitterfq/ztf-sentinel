package domains

import "time"

type Image struct {
	ID        int
	AlertID   string
	ImageType string // "reference", "science", "difference"
	FilePath  string
	CreatedAt time.Time
}
