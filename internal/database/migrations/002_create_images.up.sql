CREATE TABLE images (
    id SERIAL PRIMARY KEY,
    alert_id TEXT REFERENCES alerts(id),
    image_type TEXT CHECK (image_type IN ('reference', 'science', 'difference')),
    file_path TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
