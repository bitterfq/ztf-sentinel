# ZTF Sentinel Design Document

## Overview
Real-time Kafka consumer for ZTF (Zwicky Transient Facility) astronomical alerts. Processes filtered transient events, fetches associated images, stores metadata, and provides web interface for browsing discoveries.

## Current State
- Go-based Kafka consumer reading from Lasair stream
- Basic string logging to files
- Synchronous message processing
- No persistence layer yet

## Scale & Load Characteristics

### Data Volume
- **Total stream**: ~200K alerts/night from ZTF
- **Peak rate**: Up to 2K alerts/sec (burst during observation windows)
- **Filtered volume**: Estimated 1-10K alerts/night after filters applied
- **Images per alert**: 3 (reference, science, difference frames)
- **Storage needs**: ~30K images/night × 50KB = ~1.5GB/night
- **Retention**: ~600 days on 1TB storage before rotation needed

### Filter Criteria (Current)
```sql
-- Applied via Lasair query
- Detection within last 90 days (jdmax > jdnow()-90)
- Magnitude < 19 (gmag < 19) - brighter objects
- Brightening rate > 0.3 mag/day (dmdt_g > 0.3) - rapid transients
- Color constraint (g_minus_r BETWEEN -0.5 AND 0.5)
- At least 1 detection in last 7 days (ncandgp_7 >= 1)
- Classification confidence > 30% (classificationReliability > 0.3)
```

## Architecture Decisions

### 1. Logging Strategy
**Decision**: Structured logging with `zerolog`

**Rationale**:
- Current string logs are noisy and unparseable
- Need log levels to reduce noise in production
- JSON output enables log aggregation/querying
- zerolog is fastest option with zero allocations
- Clean, intuitive fluent API
- Minimal dependencies

**Implementation**:
```go
// Before
fmt.Fprintf(errorLog, "[%s] %s\n", now, msg.String())

// After
log.Error().Err(err).Str("component", "consumer").Msg("kafka error")
log.Info().Str("alert_id", id).Msg("processed alert")
```

### 2. Database
**Decision**: PostgreSQL with JSONB

**Rationale**:
- Need concurrent writes (SQLite insufficient at 1-10K inserts/night with bursts)
- JSONB allows flexible alert payload storage while maintaining queryability
- Partitioning by date handles long-term volume (146M rows/year if unfiltered)
- ACID guarantees important for astronomical data integrity
- Familiar SQL querying

**Schema** (draft):
```sql
CREATE TABLE alerts (
  id TEXT PRIMARY KEY,
  object_id TEXT,
  data JSONB,
  received_at TIMESTAMPTZ,
  g_mag FLOAT,
  r_mag FLOAT,
  classification TEXT,
  classification_confidence FLOAT
) PARTITION BY RANGE (received_at);

CREATE TABLE images (
  id SERIAL PRIMARY KEY,
  alert_id TEXT REFERENCES alerts(id),
  image_type TEXT CHECK (image_type IN ('reference', 'science', 'difference')),
  file_path TEXT,
  created_at TIMESTAMPTZ
);

-- Indexes TBD based on query patterns
CREATE INDEX idx_alerts_received ON alerts(received_at DESC);
CREATE INDEX idx_alerts_classification ON alerts(classification);
```

**Retention Policy**: 90 days (matches filter window) - to be validated

### 3. Consumer Architecture
**Decision**: Worker pool pattern for concurrent processing

**Problem**:
- Current synchronous processing blocks on slow image fetches (~500ms-2s each)
- Peak bursts of 2K msgs/sec (even filtered) would overwhelm serial processing
- Kafka consumer can't keep up if blocked on I/O

**Solution**:
```
┌─────────────┐
│   Kafka     │
│   Stream    │
└──────┬──────┘
       │
       v
┌──────────────────┐
│  Main Consumer   │  (fast: just read & dispatch)
│   goroutine      │
└──────┬───────────┘
       │
       v
  ┌────────────┐
  │  Job Queue │ (buffered channel, ~1000 depth)
  └─────┬──────┘
        │
        ├──> Worker 1 ─┐
        ├──> Worker 2 ─┤
        ├──> Worker 3 ─┤  (N=50 concurrent workers)
        ├──> ...       │
        └──> Worker N ─┘
                │
                v
        ┌───────────────────┐
        │ • Fetch images    │
        │ • Save to disk    │
        │ • Write to DB     │
        └───────────────────┘
```

**Parameters** (to be tuned):
- Worker count: 50 (allows 50 concurrent image fetches)
- Queue buffer: 1000 alerts
- Max retries: 3 with exponential backoff

### 4. Storage
**Decision**: Local disk on home server (not S3)

**Rationale**:
- Cost: Free vs AWS spend
- Complexity: Simpler than S3 SDK + credentials
- Performance: LAN speed sufficient for 1.5GB/night
- Control: Full ownership of data

**Structure**:
```
/data/ztf-sentinel/
  images/
    2026-01-02/
      {alert_id}_reference.fits
      {alert_id}_science.fits
      {alert_id}_difference.fits
    2026-01-03/
      ...
```

**Rotation**: Delete directories older than retention window (90 days)

### 5. API Layer
**Decision**: PostgREST

**Rationale**:
- Instant REST API from Postgres schema
- Built-in filtering, pagination, sorting
- No need to write CRUD endpoints manually
- Direct SQL access when needed for complex queries

**Example queries**:
```bash
# Last 50 alerts
GET /alerts?order=received_at.desc&limit=50

# Supernovae only
GET /alerts?classification=eq.SN

# Bright recent transients
GET /alerts?g_mag=lt.17&received_at=gte.2026-01-01
```

### 6. End Product
**Decision**: Web gallery (Priority 1), Live dashboard (Priority 2)

**Priority 1 - Web Gallery**:
- Browse recent alerts with thumbnails
- Click alert → view 3 images side-by-side
- Filter by magnitude, classification, date
- Tech: Go `net/http` + templates or htmx

**Priority 2 - Live Dashboard** (future):
- WebSocket feed from consumer
- Real-time cards as alerts arrive
- "Blink comparator" animation between frames

**Rationale**:
- Humans are visual - raw data isn't impressive
- Gallery showcases actual telescope discoveries
- Learning opportunity for Go web development
- Tangible demo-able artifact

## Open Questions

1. **Actual filtered alert rate**: Need to run consumer and measure real throughput
2. **Image fetch performance**: Latency/bandwidth to image source?
3. **Worker pool sizing**: 50 workers is guess, tune based on measurement
4. **Retention policy**: Confirm 90 days or adjust based on usage
5. **Error handling**: Dead letter queue for failed image fetches?
6. **Monitoring**: Metrics for lag, throughput, error rates?
7. **Backpressure**: What happens if workers can't keep up? Drop alerts? Block?

## Next Steps

1. Implement structured logging (slog)
2. Set up Postgres + schema
3. Implement worker pool pattern
4. Add image fetching logic
5. Measure actual throughput with filters
6. Build simple web gallery
7. Add monitoring/metrics

## Resources

- Lasair filters: [Current SQL query]
- ZTF data: https://www.ztf.caltech.edu/
- PostgREST: https://postgrest.org/
