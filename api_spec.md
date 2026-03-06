# Polar Hub Beat Transport Spec v2

## 1. Purpose

Define a deterministic beat identity and remove array ambiguity by making one beat object equal one heartbeat (`rrInterval` is a single number).

## 2. Core Rules

1. `rrIntervals` is deprecated. Use `rrInterval` everywhere.
2. One request to `/beats` contains exactly one heartbeat.
3. One item in `/beats/batch.beats[]` contains exactly one heartbeat.
4. `sessionId + beatSeq` is the canonical identity only for live and buffered realtime beats.
5. Recording is a separate source and uses `recordingId + recordingSeq` identity.
6. For `batchType="recording"`, `sessionId` is optional metadata and not part of identity.

## 3. Endpoint: `POST /beats` (Live Realtime)

### Required fields

- `source` (string)
- `device` (string)
- `sessionId` (string UUID)
- `beatSeq` (int, strictly increasing within `sessionId`)
- `timestamp` (epoch ms, app-assigned)
- `rrInterval` (number, ms)

### Optional fields

- `heartRate` (number)
- `posture` (string or null)
- `rssi` (number)

### Example

```json
{
  "source": "iphone",
  "device": "1A2B3C4D",
  "sessionId": "8f3a6a53-8dd8-4f93-83e8-8e8b51d3d9af",
  "beatSeq": 10425,
  "timestamp": 1709472000000,
  "heartRate": 72,
  "rrInterval": 833.0,
  "posture": "upright",
  "rssi": -65
}
```

## 4. Endpoint: `POST /beats/batch` (Historical Upload)

### Required top-level fields

- `source` (string)
- `device` (string)
- `batchType` (`"buffered_realtime"` or `"recording"`)
- `uploadId` (string, stable for retries of same chunk)
- `beats` (non-empty array)

### Per-beat required fields for `batchType="buffered_realtime"`

- `sessionId` (string UUID)
- `beatSeq` (int)
- `timestamp` (epoch ms)
- `rrInterval` (number, ms)

### Per-beat required fields for `batchType="recording"`

- `recordingId` (string UUID)
- `recordingSeq` (int)
- `timestamp` (epoch ms)
- `rrInterval` (number, ms)

### Per-beat optional fields (both batch types)

- `sessionId` (string UUID; optional for recording, required for buffered realtime)
- `heartRate` (number)
- `posture` (string or null)
- `rssi` (number)

### Example (`buffered_realtime`)

```json
{
  "source": "iphone",
  "device": "1A2B3C4D",
  "batchType": "buffered_realtime",
  "uploadId": "buf-2026-03-03T10:15:00Z-0007",
  "beats": [
    {
      "sessionId": "8f3a6a53-8dd8-4f93-83e8-8e8b51d3d9af",
      "beatSeq": 10426,
      "timestamp": 1709472000833,
      "rrInterval": 845.0
    }
  ]
}
```

### Example (`recording`)

```json
{
  "source": "iphone",
  "device": "1A2B3C4D",
  "batchType": "recording",
  "uploadId": "rec-2026-03-03T10:20:00Z-0003",
  "beats": [
    {
      "recordingId": "f1d16f1a-5d86-4d7f-9f01-1f2fe1a4f2d9",
      "recordingSeq": 1,
      "timestamp": 1709471990000,
      "rrInterval": 857.0
    }
  ]
}
```

## 5. Endpoint: `POST /posture`

Used for explicit posture transition events.

### Required fields

- `fromPosture` (string)
- `toPosture` (string)

### Optional fields

- `source` (string)
- `device` (string)
- `fromDurationSeconds` (number, seconds)
- `confidence` (number)

### Example

```json
{
  "source": "iphone",
  "device": "1A2B3C4D",
  "fromPosture": "lying_back",
  "toPosture": "upright",
  "fromDurationSeconds": 123,
  "confidence": 0.91
}
```

### Response

```json
{ "ok": true }
```

Notes:
- The current server only validates `fromPosture` and `toPosture`.
- The current implementation writes the posture transition to `polar_posture`.

## 6. Endpoint: `POST /status`

Used for relay/app status events.

### Required fields

- `category` (string)
- `event` (string)

### Optional fields

- `source` (string)
- `device` (string)
- `description` (string)
- `fields` (object)

### Example

```json
{
  "source": "iphone",
  "device": "1A2B3C4D",
  "category": "ble",
  "event": "connected",
  "description": "Connected to Polar H10",
  "fields": { "rssi": -55 }
}
```

### Response

```json
{ "ok": true }
```

Notes:
- The current server only validates `category` and `event`.
- All status events are logged.
- Only this allow-list is persisted to `polar_relay_status`:
  - `ble.connected`
  - `ble.disconnected`
  - `ble.pmd_locked`
  - `session.recording`
  - `session.download_complete`
  - `session.error`
  - `stream.hr_interrupted`
  - `stream.hr_recovered`
  - `upload.server_online`
  - `upload.server_offline`
- On `ble.disconnected`, the current server resets in-memory device state.

## 7. Dedup/Reconciliation Contract

1. Live vs buffered realtime: exact match on `(sessionId, beatSeq)`; retries are idempotent.
2. Recording vs anything else: do not exact-match with `beatSeq`; use server heuristic reconciliation.
3. Recording identity is `(recordingId, recordingSeq)` for recording-only idempotency and replay handling.
4. Batch replay protection: dedupe by `uploadId` at chunk level.

## 8. Sequence Requirements

1. `beatSeq` must never be reused within a `sessionId`.
2. If `/beats` fails and later goes via buffered batch, keep the same `sessionId + beatSeq`.
3. `recordingSeq` must be strictly increasing within one `recordingId`.
4. New H10 connection starts a new `sessionId`.
