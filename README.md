# Polar Hub

Central aggregation server for heart rate and HRV data from Polar sensors. Receives real-time beats from Bluetooth relay devices, deduplicates batch uploads from the iOS app, calculates HRV metrics (RMSSD, SDNN, pNN50), and writes everything to InfluxDB.

Includes a built-in status page with real-time updates via Server-Sent Events.

## Prerequisites

- Node.js
- InfluxDB 1.x

## Setup

```bash
npm install
cp .env.example .env   # edit with your InfluxDB host/database
npm run server
```

The server starts on port 3000 by default. Use `npm run server:debug` to enable request logging.

## Configuration

All configuration is done via environment variables (or a `.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `INFLUX_HOST` | `localhost` | InfluxDB host |
| `INFLUX_PORT` | `8086` | InfluxDB port |
| `INFLUX_DATABASE` | `polar_hub` | InfluxDB database name |
| `PORT` | `3000` | HTTP server port |
| `HRV_SUMMARY_INTERVAL_MS` | `300000` | HRV summary write interval (ms) |

## API Endpoints

### `POST /beats`

Receive real-time heartbeat data from a relay.

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
| `heartRate` | number | no | Heart rate in bpm |
| `rrIntervals` | number[] | **yes** | RR intervals in ms |
| `posture` | string | no | Current posture (e.g. `"sitting"`, `"standing"`) |
| `rssi` | number | no | Bluetooth signal strength in dBm |

**Response:**

```json
{ "ok": true, "received": 2 }
```

---

### `POST /beats/batch`

Receive historical beat data from the iOS app. Uses gap-based deduplication: only inserts beats where real-time coverage is missing, then recomputes per-beat RMSSD and 5-minute HRV summaries for the affected range.

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
      "heartRate": 63,
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
| `beats[].heartRate` | number | **yes** | Heart rate in bpm |
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

On `ble.disconnected` events, the server flushes any pending HRV summary for the device and resets its state.

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

| Measurement | Description |
|-------------|-------------|
| `polar_hrv_raw` | Per-beat data: heart rate, RR interval, RMSSD, SDNN, posture, source |
| `polar_hrv` | 5-minute HRV summaries: RMSSD, SDNN, pNN50, average HR, sample count |
| `polar_posture` | Posture transition events |
| `polar_relay_status` | Relay connect/disconnect events |
