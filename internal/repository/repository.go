package repository

import (
	"context"

	"github.com/bitterfq/ztf-sentinel/internal/domains"
)

type AlertRepository interface {
	Save(ctx context.Context, alert domains.Alert) error
	GetByID(ctx context.Context, id string) (domains.Alert, error)
	List(ctx context.Context, limit int32) ([]domains.Alert, error)
}

type ImageRepository interface {
	SaveImage(ctx context.Context, image domains.Image) error
	GetByAlertID(ctx context.Context, alertID string) ([]domains.Image, error)
}
