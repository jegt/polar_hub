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

Receive real-time heartbeat data from a relay. Raw RR intervals are written to `polar_raw` and post-processed for artifact correction. A 60-beat sliding window provides real-time HRV for the dashboard.

**Request body:**

```json
{
  "source": "relay-living-room",
  "device": "A0B1C2D3",
  "timestamp": 1709042400000,
  "heartRate": 62,
  "rrIntervals": [968.75, 972.65],
  "posture": "sitting",
  "rssi": -55
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | no | Relay identifier |
| `device` | string | **yes** | Sensor device ID |
| `timestamp` | number | no | Unix epoch ms (defaults to server time) |
| `heartRate` | number | no | Heart rate in bpm (from device) |
| `rrIntervals` | number[] | **yes** | RR intervals in ms |
| `posture` | string | no | Current posture (e.g. `"sitting"`, `"standing"`) |
| `rssi` | number | no | Bluetooth signal strength in dBm |

**Response:**

```json
{ "ok": true, "received": 2 }
```

---

### `POST /beats/batch`

Receive historical beat data from the iOS app. Uses gap-based deduplication: only inserts beats where real-time coverage is missing. Post-processor handles artifact correction and HRV summary computation.

`heartRate` is optional — recorded RR data from the app may not include it.

**Request body:**

```json
{
  "source": "ios-app",
  "device": "A0B1C2D3",
  "beats": [
    {
      "timestamp": 1709042400000,
      "heartRate": 62,
      "rrIntervals": [968.75, 972.65]
    },
    {
      "timestamp": 1709042402000,
      "rrIntervals": [951.17]
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | no | Source identifier |
| `device` | string | **yes** | Sensor device ID |
| `beats` | object[] | **yes** | Array of beat objects |
| `beats[].timestamp` | number | **yes** | Unix epoch ms |
| `beats[].heartRate` | number | no | Heart rate in bpm |
| `beats[].rrIntervals` | number[] | no | RR intervals in ms |

**Response:**

```json
{ "ok": true, "received": 3, "new": 1, "duplicates": 2 }
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
| Field | `rr_clean` | Corrected RR interval (set by post-processor) |
| Field | `hr_clean` | `60000 / rr_clean` (set by post-processor) |
| Field | `artifact_type` | `"none"`, `"ectopic"`, `"missed"`, `"extra"`, `"longshort"`, `"missed_inserted"`, `"extra_absorbed"` |

### `polar_realtime`

Per-beat HRV from the 60-beat sliding window (real-time dashboard data).

| Type | Name | Description |
|------|------|-------------|
| Tag | `device` | Sensor device ID |
| Field | `rmssd` | RMSSD in ms |
| Field | `sdnn` | SDNN in ms |
| Field | `pnn50` | pNN50 percentage |
| Field | `hr` | Average HR over the window |

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

### `polar_posture`

Posture transition events.

### `polar_relay_status`

Relay status events (connect, disconnect, etc.).

## Data Pipeline

```
Client (raw RR) → POST /beats → polar_raw (rr_interval, no filtering)
                               → 60-beat window → Lipponen → polar_realtime (dashboard HRV)

iOS app → POST /beats/batch → dedup → polar_raw → trigger post-processor

Post-processor (every 60s):
  → query unprocessed beats + 91-beat context on each side
  → Lipponen artifact detection & correction
  → write rr_clean + hr_clean + artifact_type → polar_raw
  → recompute 5-min summaries → polar_hrv_summary
```

Post-processed data appears ~2 minutes after ingestion (120s buffer ensures full Lipponen look-ahead context). Real-time dashboard HRV is immediate but uses a smaller 60-beat window.
