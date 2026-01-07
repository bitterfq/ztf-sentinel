CREATE TABLE alerts (
    id TEXT PRIMARY KEY,                          -- '{objectId}_{latest_detection}'
    object_id TEXT NOT NULL,
    data JSONB,                                   -- raw alert JSON
    g_mag FLOAT,
    r_mag FLOAT,
    latest_detection FLOAT,                       -- Julian date
    first_detection FLOAT,                        -- Julian date
    brightening_rate FLOAT,
    detections_last_7d INT,
    classification TEXT,
    classification_confidence FLOAT,
    received_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_alerts_received ON alerts(received_at DESC);
CREATE INDEX idx_alerts_classification ON alerts(classification);
CREATE INDEX idx_alerts_object ON alerts(object_id);