# Polar Hub

Central aggregation server for heart rate and HRV data from Polar sensors. Receives real-time beats from Bluetooth relay devices, deduplicates batch uploads from the iOS app, and writes everything to InfluxDB. Artifact detection uses Lipponen & Tarvainen (2019) dRR pattern classification.

Includes a built-in status page with real-time updates via Server-Sent Events.

## Prerequisites

- Node.js
- InfluxDB 1.x

## Setup

```bash
npm install
cp config.example.json config.json   # edit with your InfluxDB host/database
npm run server
```

The server starts on port 3000 by default. Use `npm run server:debug` to enable request logging.

Logs are written to both stdout and `logs/hub.log`.

## Configuration

All configuration is done via `config.json` in the project root. Copy `config.example.json` to get started. Any missing keys fall back to defaults.

| Key | Default | Description |
|-----|---------|-------------|
| `influxHost` | `"localhost"` | InfluxDB host |
| `influxPort` | `8086` | InfluxDB port |
| `influxDatabase` | `"polar_hub"` | InfluxDB database name |
| `port` | `3000` | HTTP server port |
| `hrvSummaryIntervalMs` | `300000` | HRV summary write interval (ms) |

## API Endpoints

### `POST /beats`

Receive one real-time heartbeat from the app. Raw RR is written to `polar_raw` and post-processed for artifact correction. A 60-beat sliding window provides real-time HRV for the dashboard.

**Request body:**

```json
{
  "source": "ios-app",
  "device": "A0B1C2D3",
  "sessionId": "8f3a6a53-8dd8-4f93-83e8-8e8b51d3d9af",
  "beatSeq": 10425,
  "timestamp": 1709042400000,
  "heartRate": 62,
  "rrInterval": 968.75,
  "posture": "sitting",
  "rssi": -55
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | no | Source identifier |
| `device` | string | **yes** | Sensor device ID |
| `sessionId` | string | **yes** | Session UUID for live/buffered identity |
| `beatSeq` | number (int) | **yes** | Monotonic beat sequence within `sessionId` |
| `timestamp` | number | **yes** | Unix epoch ms |
| `heartRate` | number | no | Heart rate in bpm (from device) |
| `rrInterval` | number | **yes** | RR interval in ms (single beat) |
| `posture` | string | no | Current posture (e.g. `"sitting"`, `"standing"`) |
| `rssi` | number | no | Bluetooth signal strength in dBm |

**Response:**

```json
{ "ok": true, "received": 1, "new": 1, "duplicates": 0 }
```

---

### `POST /beats/batch`

Receive historical beat data from the iOS app.

Dedup strategy depends on `batchType`:
- `buffered_realtime`: exact identity dedup on `(sessionId, beatSeq)`
- `recording`: identity dedup on `(recordingId, recordingSeq)` plus gap-based dedup against existing timeline

`heartRate` is optional.

**Request body:**

```json
{
  "source": "ios-app",
  "device": "A0B1C2D3",
  "batchType": "buffered_realtime",
  "uploadId": "buf-2026-03-03T10:15:00Z-0007",
  "beats": [
    {
      "sessionId": "8f3a6a53-8dd8-4f93-83e8-8e8b51d3d9af",
      "beatSeq": 10426,
      "timestamp": 1709042400000,
      "heartRate": 62,
      "rrInterval": 968.75
    },
    {
      "sessionId": "8f3a6a53-8dd8-4f93-83e8-8e8b51d3d9af",
      "beatSeq": 10427,
      "timestamp": 1709042402000,
      "rrInterval": 951.17
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | no | Source identifier |
| `device` | string | **yes** | Sensor device ID |
| `batchType` | string | **yes** | `"buffered_realtime"` or `"recording"` |
| `uploadId` | string | **yes** | Idempotency key for the batch chunk |
| `beats` | object[] | **yes** | Array of beat objects |
| `beats[].timestamp` | number | **yes** | Unix epoch ms |
| `beats[].rrInterval` | number | **yes** | RR interval in ms (single beat) |
| `beats[].heartRate` | number | no | Heart rate in bpm |
| `beats[].sessionId` | string | conditional | Required for `batchType="buffered_realtime"` |
| `beats[].beatSeq` | number (int) | conditional | Required for `batchType="buffered_realtime"` |
| `beats[].recordingId` | string | conditional | Required for `batchType="recording"` |
| `beats[].recordingSeq` | number (int) | conditional | Required for `batchType="recording"` |

**Response:**

```json
{ "ok": true, "replay": false, "received": 3, "new": 1, "duplicates": 2 }
```

---

### `POST /posture`

Receive posture transition events from a relay.

**Request body:**

```json
{
  "source": "relay-living-room",
  "device": "A0B1C2D3",
  "fromPosture": "sitting",
  "toPosture": "standing",
  "fromDurationSeconds": 1800,
  "confidence": 0.92
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | no | Relay identifier |
| `device` | string | no | Sensor device ID |
| `fromPosture` | string | **yes** | Previous posture |
| `toPosture` | string | **yes** | New posture |
| `fromDurationSeconds` | number | no | How long the previous posture was held |
| `confidence` | number | no | Classification confidence (0–1) |

**Response:**

```json
{ "ok": true }
```

---

### `POST /status`

Receive status events from relays and the iOS app. Events are categorised by `category.event`. Only events in the server-side allow-list are persisted to InfluxDB; all others are logged but not stored.

**Request body:**

```json
{
  "source": "relay-living-room",
  "device": "A0B1C2D3",
  "category": "ble",
  "event": "connected",
  "description": "Connected to Polar H10",
  "fields": { "rssi": -55 }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | no | Relay or client identifier |
| `device` | string | no | Sensor device ID |
| `category` | string | **yes** | Event category (e.g. `"ble"`, `"session"`, `"stream"`, `"upload"`) |
| `event` | string | **yes** | Event name within the category |
| `description` | string | no | Human-readable description |
| `fields` | object | no | Arbitrary key/value pairs persisted as InfluxDB fields |

**Persisted events** (written to InfluxDB):

`ble.connected`, `ble.disconnected`, `ble.pmd_locked`, `session.recording`, `session.download_complete`, `session.error`, `stream.hr_interrupted`, `stream.hr_recovered`, `upload.server_online`, `upload.server_offline`

All other `category.event` combinations are logged to stdout only.

**Response:**

```json
{ "ok": true }
```

On `ble.disconnected` events, the server resets device state.

---

### `GET /`

Status page with real-time heart rate, RR intervals, and HRV data.

### `GET /events`

Server-Sent Events stream. Pushes a full status update after every beat and relay event. Each message is a JSON object with device states, relay states, and hub uptime.

### `GET /health`

Health check.

```json
{ "ok": true, "devices": 1 }
```

## InfluxDB Measurements

### `polar_raw`

Every beat as received, plus post-processed clean values.

| Type | Name | Description |
|------|------|-------------|
| Tag | `device` | Sensor device ID |
| Field | `rr_interval` | Raw RR interval in ms (null for synthetic inserted beats) |
| Field | `heart_rate` | Device-reported HR (null for batch without HR) |
| Field | `source` | Relay/client identifier |
| Field | `path` | `"realtime"` or `"batch"` |
| Field | `session_id` | Session id (live/buffered identity) |
| Field | `beat_seq` | Beat sequence within `session_id` |
| Field | `batch_type` | `"buffered_realtime"` or `"recording"` |
| Field | `upload_id` | Batch idempotency key |
| Field | `recording_id` | Recording identity for recording uploads |
| Field | `recording_seq` | Sequence within `recording_id` |
| Field | `rr_clean` | Corrected RR interval (set by post-processor) |
| Field | `hr_clean` | `60000 / rr_clean` (set by post-processor) |
| Field | `artifact_type` | `"none"`, `"ectopic"`, `"missed"`, `"extra"`, `"longshort"`, `"missed_inserted"`, `"extra_absorbed"` |
| Field | `correction_applied` | Whether a material correction was applied |
| Field | `correction_delta_ms` | Signed delta `rr_clean - rr_interval` in ms |
| Field | `weak_correction` | True when a tiny `longshort` correction (<5ms) was demoted to non-artifact |

### `polar_realtime`

Per-beat HRV from the 60-beat sliding window (real-time dashboard data).

| Type | Name | Description |
|------|------|-------------|
| Tag | `device` | Sensor device ID |
| Field | `rmssd` | RMSSD in ms |
| Field | `sdnn` | SDNN in ms |
| Field | `pnn50` | pNN50 percentage |
| Field | `hr` | Average HR over the window |
| Field | `hr_instant` | Instant HR from current canonical beat (`60000 / rr_clean`) |

### `polar_hrv_summary`

5-minute HRV summaries computed from `rr_clean` values.

| Type | Name | Description |
|------|------|-------------|
| Tag | `device` | Sensor device ID |
| Tag | `posture` | Posture during the window (when available) |
| Field | `rmssd` | RMSSD in ms |
| Field | `sdnn` | SDNN in ms |
| Field | `pnn50` | pNN50 percentage |
| Field | `heart_rate` | Average HR |
| Field | `sample_count` | Number of clean beats in the window |
| Field | `artifact_count` | Number of artifacts detected |

### `polar_quality_1m`

One-minute canonical data quality metrics derived from `polar_raw`.

| Type | Name | Description |
|------|------|-------------|
| Tag | `device` | Sensor device ID |
| Field | `sample_count` | Number of raw beats in the minute |
| Field | `artifact_count` | Number of beats with `artifact_type != 'none'` |
| Field | `artifact_rate` | `artifact_count / sample_count` |
| Field | `corrected_count` | Number of strong corrected beats (`correction_applied=true` and not weak) |
| Field | `correction_rate` | `corrected_count / sample_count` |
| Field | `weak_correction_count` | Number of demoted weak corrections in the minute |
| Field | `max_consecutive_corrected` | Longest run of strongly corrected beats |
| Field | `correction_burst_level` | `"none"`, `"warning"`, `"critical"` from correction burst guard |
| Field | `low_confidence` | True when correction burst guard is warning/critical |
| Field | `unclassified_corrected_count` | Beats where `rr_clean` changed materially while artifact was unclassified |
| Field | `out_of_order_count` | Count of non-monotonic sequence transitions by stream identity |
| Field | `extreme_rr_count` | Raw beats with `rr_interval < 300` or `rr_interval > 2200` |

### `polar_upload_receipts`

Processed batch `uploadId` receipts for idempotent retries.

| Type | Name | Description |
|------|------|-------------|
| Tag | `device` | Sensor device ID |
| Tag | `batch_type` | Batch type |
| Tag | `upload_id` | Batch upload idempotency key |
| Field | `received` | Number of beats received in batch |
| Field | `new_points` | Number of beats inserted |
| Field | `duplicate_points` | Number of beats deduplicated |

### `polar_posture`

Posture transition events.

### `polar_relay_status`

Relay status events (connect, disconnect, etc.).

## Data Pipeline

```
Client (raw RR) → POST /beats → polar_raw (rr_interval, no filtering)
                               → 60-beat window → Lipponen → polar_realtime (dashboard HRV)

iOS app → POST /beats/batch (batchType=buffered_realtime)
        → dedup by (session_id, beat_seq) → polar_raw

iOS app → POST /beats/batch (batchType=recording)
        → dedup by (recording_id, recording_seq)
        → gap-based dedup → polar_raw

all batch uploads:
        → write upload receipt (upload_id) → polar_upload_receipts
        → trigger post-processor on new inserts

Post-processor (every 60s):
  → query unprocessed beats + 91-beat context on each side
  → Lipponen artifact detection & correction
  → write rr_clean + hr_clean + artifact_type → polar_raw
  → recompute + overwrite affected per-beat dashboard points → polar_realtime
  → write per-minute quality metrics → polar_quality_1m
  → recompute 5-min summaries → polar_hrv_summary
```

Post-processed data appears ~2-3 minutes after ingestion (120s buffer + up to one 60s cycle). Real-time dashboard data is immediate on ingest, then overwritten by canonical post-processed values.

## Operations: Quality Triage

Quick InfluxQL checks for a device:

```sql
-- 1) 1-minute quality timeline
SELECT sample_count, artifact_count, artifact_rate,
       corrected_count, correction_rate, weak_correction_count,
       max_consecutive_corrected, correction_burst_level, low_confidence,
       out_of_order_count, extreme_rr_count
FROM polar_quality_1m
WHERE "device" = '1A2B3C4D'
  AND time >= now() - 6h
ORDER BY time ASC;

-- 2) Investigate artifact types in raw stream
SELECT artifact_type, rr_interval, rr_clean, correction_delta_ms
FROM polar_raw
WHERE "device" = '1A2B3C4D'
  AND time >= now() - 30m
  AND artifact_type <> 'none'
ORDER BY time ASC
LIMIT 200;

-- 3) Check for unclassified corrections
SELECT correction_delta_ms, rr_interval, rr_clean, artifact_type
FROM polar_raw
WHERE "device" = '1A2B3C4D'
  AND time >= now() - 30m
  AND correction_applied = true
  AND (artifact_type = 'none' OR artifact_type = '')
ORDER BY time ASC
LIMIT 200;
```

Incident checklist:
1. If `extreme_rr_count` rises, inspect raw RR ingress quality (sensor contact/motion).
2. If `out_of_order_count` rises, inspect app upload ordering and network retry behavior.
3. If `correction_burst_level` is `warning`/`critical`, treat that window as low confidence in downstream HRV usage.
4. If `artifact_rate` spikes but `extreme_rr_count` is low, inspect stream stitching/gap-fill behavior.
5. If `unclassified_corrected_count` is non-zero, treat as regression and inspect post-processor logs/code.
