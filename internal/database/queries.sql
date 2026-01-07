-- name: CreateAlert :exec
INSERT INTO alerts (
    id, object_id, data, g_mag, r_mag,
    latest_detection, first_detection, brightening_rate,
    detections_last_7d, classification, classification_confidence
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
);

-- name: GetAlertByID :one
SELECT * FROM alerts WHERE id = $1;

-- name: ListAlerts :many
SELECT * FROM alerts
ORDER BY received_at DESC
LIMIT $1;

-- name: ListAlertsByClassification :many
SELECT * FROM alerts
WHERE classification = $1
ORDER BY received_at DESC
LIMIT $2;

-- name: CreateImage :exec
INSERT INTO images (alert_id, image_type, file_path)
VALUES ($1, $2, $3);

-- name: GetImagesByAlertID :many
SELECT * FROM images WHERE alert_id = $1;