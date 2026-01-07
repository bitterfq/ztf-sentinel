package repository

import (
	"context"
	"database/sql"

	"github.com/bitterfq/ztf-sentinel/internal/database/db"
	"github.com/bitterfq/ztf-sentinel/internal/domains"
	"github.com/sqlc-dev/pqtype"
)

type PostgresRepo struct {
	queries *db.Queries
}

func NewPostgresRepo(queries *db.Queries) *PostgresRepo {
	return &PostgresRepo{queries: queries}
}

func (p *PostgresRepo) Save(ctx context.Context, alert domains.Alert) error {
	params := domainToDBParams(alert)
	return p.queries.CreateAlert(ctx, params)
}

func (p *PostgresRepo) GetByID(ctx context.Context, id string) (domains.Alert, error) {
	dbAlert, err := p.queries.GetAlertByID(ctx, id)
	if err != nil {
		return domains.Alert{}, err
	}
	return dbToDomain(dbAlert), nil
}

func (p *PostgresRepo) List(ctx context.Context, limit int32) ([]domains.Alert, error) {

	dbAlerts, err := p.queries.ListAlerts(ctx, limit)
	if err != nil {
		return nil, err
	}

	alerts := make([]domains.Alert, len(dbAlerts))
	for i, a := range dbAlerts {
		alerts[i] = dbToDomain(a)
	}
	return alerts, nil
}

func domainToDBParams(a domains.Alert) db.CreateAlertParams {
	return db.CreateAlertParams{
		ID:                       a.ID,
		ObjectID:                 a.ObjectID,
		Data:                     pqtype.NullRawMessage{RawMessage: a.Data, Valid: len(a.Data) > 0},
		GMag:                     sql.NullFloat64{Float64: a.GMag, Valid: true},
		RMag:                     sql.NullFloat64{Float64: a.RMag, Valid: true},
		LatestDetection:          sql.NullFloat64{Float64: a.LatestDetection, Valid: true},
		FirstDetection:           sql.NullFloat64{Float64: a.FirstDetection, Valid: true},
		BrighteningRate:          sql.NullFloat64{Float64: a.BrighteningRate, Valid: true},
		DetectionsLast7d:         sql.NullInt32{Int32: int32(a.DetectionsLast7d), Valid: true},
		Classification:           sql.NullString{String: a.Classification, Valid: a.Classification != ""},
		ClassificationConfidence: sql.NullFloat64{Float64: a.ClassificationConfidence, Valid: true},
	}
}

func dbToDomain(a db.Alert) domains.Alert {
	return domains.Alert{
		ID:                       a.ID,
		ObjectID:                 a.ObjectID,
		Data:                     a.Data.RawMessage,
		GMag:                     a.GMag.Float64,
		RMag:                     a.RMag.Float64,
		LatestDetection:          a.LatestDetection.Float64,
		FirstDetection:           a.FirstDetection.Float64,
		BrighteningRate:          a.BrighteningRate.Float64,
		DetectionsLast7d:         int(a.DetectionsLast7d.Int32),
		Classification:           a.Classification.String,
		ClassificationConfidence: a.ClassificationConfidence.Float64,
		ReceivedAt:               a.ReceivedAt.Time,
	}
}
