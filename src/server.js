/**
 * Polar Hub Server
 *
 * Central aggregation server that receives data from multiple relays,
 * deduplicates, and writes to InfluxDB. Artifact detection uses
 * Lipponen & Tarvainen (2019) dRR pattern detection.
 *
 * Run with: npm start
 */

import express from 'express';
import { createServer } from 'http';
import { createWriteStream, mkdirSync } from 'fs';
import { readFile } from 'fs/promises';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { calculateHRV } from './hrv.js';
import { analyzeRR } from './lipponen.js';
import { createInfluxClient, createWriters } from './influx.js';
import { createPostProcessor } from './postprocessor.js';
import config from './config.js';

// Get directory path for serving static files
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const TEST_MODE = process.argv.includes('--test');
const PORT = TEST_MODE ? (config.testPort || config.port + 1) : config.port;
const LOG_REQUESTS = process.argv.includes('--log-requests');
const GAP_THRESHOLD_MS = 1000;
const RECORDING_DUPLICATE_PROXIMITY_MS = 500;
const RECORDING_GAP_EDGE_TOLERANCE_MS = 300;
const REPROCESS_REWIND_MS = 10_000;
const REALTIME_WINDOW_SIZE = 60;
const BATCH_TYPES = new Set(['buffered_realtime', 'recording']);

// Create components
const influx = createInfluxClient({
  host: config.influxHost,
  port: config.influxPort,
  database: TEST_MODE ? config.influxDatabase + '_test' : config.influxDatabase
});
const writers = createWriters(influx, log);
const postProcessor = createPostProcessor(writers, log, {
  summaryIntervalMs: config.hrvSummaryIntervalMs
});

// Events that get persisted to InfluxDB (category.event); everything else is log-only
const PERSIST_EVENTS = new Set([
  'ble.connected', 'ble.disconnected', 'ble.pmd_locked',
  'session.recording', 'session.download_complete', 'session.error',
  'stream.hr_interrupted', 'stream.hr_recovered',
  'upload.server_online', 'upload.server_offline'
]);

// Per-device state
const deviceState = new Map();

// Relay state tracking for status page
const relayState = new Map();
const hubStartTime = Date.now();

// Latest beat data for broadcasting (per device)
const latestBeatData = new Map();

function getDeviceState(device) {
  if (!deviceState.has(device)) {
    deviceState.set(device, {
      rrWindow: [],       // last 60 raw RR values for real-time Lipponen
      rmssdBuffer: [],    // last 30 RMSSD values for visualization
      totalBeats: 0,
      lastPosture: 'unknown'
    });
    // Register with post-processor
    postProcessor.registerDevice(device);
  }
  return deviceState.get(device);
}

/**
 * Log with timestamp (stdout + logs/hub.log)
 */
const logDir = join(__dirname, '..', 'logs');
mkdirSync(logDir, { recursive: true });
const logStream = createWriteStream(join(logDir, TEST_MODE ? 'hub-test.log' : 'hub.log'), { flags: 'a' });

function localTimestamp(date = new Date()) {
  return date.toLocaleString('sv-SE', { hour12: false }).replace(' ', 'T');
}

function log(message) {
  const line = `[${localTimestamp()}] ${message}`;
  console.log(line);
  logStream.write(line + '\n');
}

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

// Express app
const app = express();
const server = createServer(app);

// SSE clients for real-time status updates
const sseClients = new Set();

/**
 * Build full status object for SSE broadcast
 */
function buildStatusPayload() {
  const devices = {};
  for (const [deviceId, state] of deviceState.entries()) {
    const beatData = latestBeatData.get(deviceId) || {};
    devices[deviceId] = {
      totalBeats: state.totalBeats,
      posture: state.lastPosture,
      rrBuffer: state.rrWindow.slice(-30),
      rmssdBuffer: state.rmssdBuffer,
      ...beatData
    };
  }

  const relays = {};
  for (const [relayId, state] of relayState.entries()) {
    relays[relayId] = state;
  }

  return {
    type: 'status',
    timestamp: Date.now(),
    hub: {
      uptime: Date.now() - hubStartTime,
      startTime: hubStartTime
    },
    devices,
    relays
  };
}

/**
 * Broadcast status to all connected SSE clients
 */
function broadcastStatus() {
  if (sseClients.size === 0) return;

  const payload = JSON.stringify(buildStatusPayload());
  for (const client of sseClients) {
    client.write(`data: ${payload}\n\n`);
  }
}

app.use(express.json({ limit: '5mb' }));

if (LOG_REQUESTS) {
  app.use((req, res, next) => {
    const start = Date.now();
    const origJson = res.json.bind(res);
    let errorMsg = '';
    res.json = (body) => {
      if (res.statusCode >= 400 && body && body.error) {
        errorMsg = ` — ${body.error}`;
      }
      return origJson(body);
    };
    res.on('finish', () => {
      log(`${req.method} ${req.originalUrl} ${res.statusCode} (${Date.now() - start}ms)${errorMsg}`);
    });
    next();
  });
}

/**
 * POST /beats - Receive one realtime heartbeat
 *
 * Strict v2 payload:
 * - one beat per request (`rrInterval`)
 * - identity key: (`sessionId`, `beatSeq`)
 */
app.post('/beats', async (req, res) => {
  const {
    source, device, sessionId, beatSeq, timestamp,
    heartRate, rrInterval, posture, rssi
  } = req.body;

  if (!isNonEmptyString(device)) {
    return res.status(400).json({ ok: false, error: 'device is required' });
  }
  if (!isNonEmptyString(sessionId)) {
    return res.status(400).json({ ok: false, error: 'sessionId is required' });
  }
  if (!Number.isInteger(beatSeq) || beatSeq < 0) {
    return res.status(400).json({ ok: false, error: 'beatSeq must be a non-negative integer' });
  }
  if (!isFiniteNumber(timestamp)) {
    return res.status(400).json({ ok: false, error: 'timestamp must be a number' });
  }
  if (!isFiniteNumber(rrInterval) || rrInterval <= 0) {
    return res.status(400).json({ ok: false, error: 'rrInterval must be a positive number' });
  }
  if (heartRate != null && !isFiniteNumber(heartRate)) {
    return res.status(400).json({ ok: false, error: 'heartRate must be a number when provided' });
  }

  let existing;
  try {
    existing = await writers.queryBeatIdentityExists(device, sessionId, beatSeq);
  } catch (err) {
    log(`Realtime dedup lookup failed: ${err.message}`);
    return res.status(500).json({ ok: false, error: 'Dedup query failed' });
  }

  if (existing) {
    return res.json({ ok: true, received: 1, new: 0, duplicates: 1 });
  }

  const state = getDeviceState(device);
  state.totalBeats++;
  state.lastPosture = posture || 'unknown';

  const beatTimestamp = timestamp;
  const rr = rrInterval;
  const hrInstant = Math.round((60000 / rr) * 100) / 100;

  try {
    await writers.writeRawBeat(device, {
      timestamp: beatTimestamp,
      rr_interval: rr,
      heart_rate: heartRate ?? null,
      source,
      path: 'realtime',
      session_id: sessionId,
      beat_seq: beatSeq
    });
  } catch {
    return res.status(500).json({ ok: false, error: 'InfluxDB write failed' });
  }

  // Push to real-time window
  state.rrWindow.push(rr);
  if (state.rrWindow.length > REALTIME_WINDOW_SIZE) {
    state.rrWindow.shift();
  }

  // Run Lipponen on the real-time window for dashboard HRV
  let hrv = { rmssd: null, sdnn: null, pnn50: null };
  let windowHR = heartRate;

  if (state.rrWindow.length >= 6) {
    let rrSeries = [...state.rrWindow];
    let cleanRRs = rrSeries;
    let prevArtifacts = Infinity;

    for (let pass = 0; pass < 20; pass++) {
      if (rrSeries.length < 4) break;
      const analysis = analyzeRR(rrSeries);
      if (analysis.stats.artifacts > prevArtifacts) break;
      prevArtifacts = analysis.stats.artifacts;
      cleanRRs = analysis.cleanSeries;
      rrSeries = cleanRRs;
      if (prevArtifacts === 0) break;
    }

    cleanRRs = cleanRRs.slice(1, -2);
    if (cleanRRs.length >= 2) {
      hrv = calculateHRV(cleanRRs);
      const meanCleanRR = cleanRRs.reduce((a, b) => a + b, 0) / cleanRRs.length;
      windowHR = Math.round(60000 / meanCleanRR);
    }
  }

  await writers.writeRealtime(device, beatTimestamp, hrv, windowHR, posture, hrInstant);

  if (hrv.rmssd !== null) {
    state.rmssdBuffer.push(hrv.rmssd);
    if (state.rmssdBuffer.length > 30) {
      state.rmssdBuffer.shift();
    }
  }

  latestBeatData.set(device, {
    heartRate: windowHR,
    rr,
    hrv,
    rssi,
    source,
    timestamp: beatTimestamp
  });

  if (source) {
    relayState.set(source, {
      lastSeen: Date.now(),
      device,
      rssi,
      status: 'active'
    });
  }

  const hrvStr = hrv.rmssd !== null ? `RMSSD=${hrv.rmssd}` : 'RMSSD=-';
  const postureStr = posture && posture !== 'unknown' ? ` [${posture}]` : '';
  const rssiStr = rssi != null ? ` ${rssi}dBm` : '';
  const sourceStr = source ? `[${source}] ` : '';
  const lagMs = Date.now() - beatTimestamp;
  const sessionShort = sessionId.slice(0, 8);
  log(`${sourceStr}#${state.totalBeats} seq=${sessionShort}:${beatSeq} HR=${windowHR} RR=${rr.toFixed(0)}ms ${hrvStr}${rssiStr}${postureStr} ts=${beatTimestamp} lag=${lagMs}ms`);

  broadcastStatus();

  res.json({ ok: true, received: 1, new: 1, duplicates: 0 });
});

/**
 * POST /beats/batch - Receive historical beat data from iOS app
 *
 * Strict v2 payload:
 * - one beat object = one heartbeat (`rrInterval`)
 * - requires top-level `batchType` + `uploadId`
 * - buffered realtime dedup: identity key (`sessionId`, `beatSeq`)
 * - recording dedup: (`recordingId`, `recordingSeq`) + timestamp gap heuristic
 */
app.post('/beats/batch', async (req, res) => {
  const { source, device, batchType, uploadId, beats } = req.body;

  if (!isNonEmptyString(device)) {
    return res.status(400).json({ ok: false, error: 'device is required' });
  }
  if (!BATCH_TYPES.has(batchType)) {
    return res.status(400).json({ ok: false, error: 'batchType must be buffered_realtime or recording' });
  }
  if (!isNonEmptyString(uploadId)) {
    return res.status(400).json({ ok: false, error: 'uploadId is required' });
  }
  if (!Array.isArray(beats) || beats.length === 0) {
    return res.status(400).json({ ok: false, error: 'beats must be a non-empty array' });
  }

  let previousReceipt;
  try {
    previousReceipt = await writers.queryUploadReceipt(device, batchType, uploadId);
  } catch (err) {
    log(`Upload receipt lookup failed: ${err.message}`);
    return res.status(500).json({ ok: false, error: 'Upload lookup failed' });
  }
  if (previousReceipt) {
    return res.json({
      ok: true,
      replay: true,
      received: previousReceipt.received,
      new: previousReceipt.new,
      duplicates: previousReceipt.duplicates
    });
  }

  await postProcessor.registerDevice(device);

  const incomingPoints = [];
  for (let i = 0; i < beats.length; i++) {
    const beat = beats[i];

    if (!isFiniteNumber(beat.timestamp)) {
      return res.status(400).json({ ok: false, error: `Beat ${i} timestamp must be a number` });
    }
    if (!isFiniteNumber(beat.rrInterval) || beat.rrInterval <= 0) {
      return res.status(400).json({ ok: false, error: `Beat ${i} rrInterval must be a positive number` });
    }
    if (beat.heartRate != null && !isFiniteNumber(beat.heartRate)) {
      return res.status(400).json({ ok: false, error: `Beat ${i} heartRate must be a number when provided` });
    }

    if (batchType === 'buffered_realtime') {
      if (!isNonEmptyString(beat.sessionId)) {
        return res.status(400).json({ ok: false, error: `Beat ${i} sessionId is required for buffered_realtime` });
      }
      if (!Number.isInteger(beat.beatSeq) || beat.beatSeq < 0) {
        return res.status(400).json({ ok: false, error: `Beat ${i} beatSeq must be a non-negative integer` });
      }
    } else {
      if (!isNonEmptyString(beat.recordingId)) {
        return res.status(400).json({ ok: false, error: `Beat ${i} recordingId is required for recording` });
      }
      if (!Number.isInteger(beat.recordingSeq) || beat.recordingSeq < 0) {
        return res.status(400).json({ ok: false, error: `Beat ${i} recordingSeq must be a non-negative integer` });
      }
      if (beat.sessionId != null && !isNonEmptyString(beat.sessionId)) {
        return res.status(400).json({ ok: false, error: `Beat ${i} sessionId must be a non-empty string when provided` });
      }
    }

    incomingPoints.push({
      timestamp: beat.timestamp,
      rr_interval: beat.rrInterval,
      heart_rate: beat.heartRate ?? null,
      source,
      path: 'batch',
      batch_type: batchType,
      upload_id: uploadId,
      session_id: beat.sessionId ?? null,
      beat_seq: beat.beatSeq ?? null,
      recording_id: beat.recordingId ?? null,
      recording_seq: beat.recordingSeq ?? null
    });
  }

  incomingPoints.sort((a, b) => a.timestamp - b.timestamp);
  let newPoints = [];
  let duplicateCount = 0;

  if (batchType === 'buffered_realtime') {
    const bySession = new Map();
    for (const p of incomingPoints) {
      if (!bySession.has(p.session_id)) bySession.set(p.session_id, []);
      bySession.get(p.session_id).push(p);
    }

    const existingBySession = new Map();
    for (const [sessionId, points] of bySession.entries()) {
      const seqs = points.map(p => p.beat_seq);
      const minSeq = Math.min(...seqs);
      const maxSeq = Math.max(...seqs);
      const existingSeqs = await writers.queryExistingBeatSeqs(device, sessionId, minSeq, maxSeq);
      existingBySession.set(sessionId, existingSeqs);
    }

    const seen = new Set();
    for (const p of incomingPoints) {
      const key = `${p.session_id}:${p.beat_seq}`;
      if (seen.has(key)) {
        duplicateCount++;
        continue;
      }
      seen.add(key);

      const existingSeqs = existingBySession.get(p.session_id) || new Set();
      if (existingSeqs.has(p.beat_seq)) {
        duplicateCount++;
      } else {
        newPoints.push(p);
      }
    }
  } else {
    const byRecording = new Map();
    for (const p of incomingPoints) {
      if (!byRecording.has(p.recording_id)) byRecording.set(p.recording_id, []);
      byRecording.get(p.recording_id).push(p);
    }

    const existingByRecording = new Map();
    for (const [recordingId, points] of byRecording.entries()) {
      const seqs = points.map(p => p.recording_seq);
      const minSeq = Math.min(...seqs);
      const maxSeq = Math.max(...seqs);
      const existingSeqs = await writers.queryExistingRecordingSeqs(device, recordingId, minSeq, maxSeq);
      existingByRecording.set(recordingId, existingSeqs);
    }

    const seenRecording = new Set();
    const candidates = [];
    for (const p of incomingPoints) {
      const recKey = `${p.recording_id}:${p.recording_seq}`;
      if (seenRecording.has(recKey)) {
        duplicateCount++;
        continue;
      }
      seenRecording.add(recKey);

      const existingSeqs = existingByRecording.get(p.recording_id) || new Set();
      if (existingSeqs.has(p.recording_seq)) {
        duplicateCount++;
      } else {
        candidates.push(p);
      }
    }

    if (candidates.length > 0) {
      const firstTs = candidates[0].timestamp;
      const lastTs = candidates[candidates.length - 1].timestamp;

      let existing = [];
      try {
        existing = await writers.queryRawBeats(device, firstTs - 2000, lastTs + 2000);
      } catch (err) {
        log(`Recording dedup query failed, writing all candidates: ${err.message}`);
      }

      if (existing.length === 0) {
        newPoints = candidates;
      } else {
        // First pass: drop candidates that already have a nearby existing beat
        // (prevents recording path from reinserting surrounding beats near a real gap).
        const dedupCandidates = [];
        let exIdx = 0;
        for (const p of candidates) {
          while (exIdx < existing.length - 1
              && existing[exIdx + 1].time < p.timestamp - RECORDING_DUPLICATE_PROXIMITY_MS) {
            exIdx++;
          }

          let hasNearbyExisting = false;
          let i = Math.max(0, exIdx - 1);
          while (i < existing.length && existing[i].time <= p.timestamp + RECORDING_DUPLICATE_PROXIMITY_MS) {
            if (Math.abs(existing[i].time - p.timestamp) <= RECORDING_DUPLICATE_PROXIMITY_MS) {
              hasNearbyExisting = true;
              break;
            }
            i++;
          }

          if (hasNearbyExisting) {
            duplicateCount++;
          } else {
            dedupCandidates.push(p);
          }
        }

        const gaps = [];
        for (let i = 0; i < existing.length - 1; i++) {
          const expectedNext = existing[i].time + (existing[i].rr_interval || 0);
          const actualNext = existing[i + 1].time;
          if (actualNext - expectedNext > GAP_THRESHOLD_MS) {
            gaps.push({ start: expectedNext, end: actualNext });
          }
        }

        if (firstTs < existing[0].time - GAP_THRESHOLD_MS) {
          gaps.unshift({ start: firstTs, end: existing[0].time });
        }

        const lastEx = existing[existing.length - 1];
        const trailingStart = lastEx.time + (lastEx.rr_interval || 0);
        if (lastTs > trailingStart + GAP_THRESHOLD_MS) {
          gaps.push({ start: trailingStart, end: lastTs + 2000 });
        }

        newPoints = dedupCandidates.filter(p =>
          gaps.some(g => p.timestamp >= g.start - RECORDING_GAP_EDGE_TOLERANCE_MS
                      && p.timestamp <= g.end + RECORDING_GAP_EDGE_TOLERANCE_MS));
        duplicateCount += dedupCandidates.length - newPoints.length;
      }
    }
  }

  if (newPoints.length > 0) {
    try {
      await writers.writeRawBatch(device, newPoints);
    } catch {
      return res.status(500).json({ ok: false, error: 'InfluxDB write failed' });
    }
    const firstNewTs = newPoints[0].timestamp;
    const reprocessFrom = Math.max(0, firstNewTs - REPROCESS_REWIND_MS);
    const streamHints = batchType === 'buffered_realtime'
      ? [...new Set(newPoints.map(p => p.session_id).filter(Boolean))].map(id => ({ type: 'session', id }))
      : [...new Set(newPoints.map(p => p.recording_id).filter(Boolean))].map(id => ({ type: 'recording', id }));
    await postProcessor.triggerReprocess(device, reprocessFrom, streamHints);
  }

  try {
    await writers.writeUploadReceipt(device, batchType, uploadId, {
      received: incomingPoints.length,
      new: newPoints.length,
      duplicates: duplicateCount
    });
  } catch (err) {
    log(`Upload receipt write failed: ${err.message}`);
  }

  const firstTs = incomingPoints[0].timestamp;
  const lastTs = incomingPoints[incomingPoints.length - 1].timestamp;
  const durationMin = ((lastTs - firstTs) / 60000).toFixed(1);
  const deviceShort = device.length > 11 ? device.slice(0, 11) + '...' : device;
  const fmt = ts => localTimestamp(new Date(ts)).slice(11, 19);
  log(`Batch[${batchType}] uploadId=${uploadId}: ${incomingPoints.length} beats, ${newPoints.length} new, ${duplicateCount} dupes from ${source || 'unknown'} for ${deviceShort} | ${fmt(firstTs)} → ${fmt(lastTs)} (${durationMin}min)`);

  res.json({
    ok: true,
    replay: false,
    received: incomingPoints.length,
    new: newPoints.length,
    duplicates: duplicateCount
  });
});

/**
 * POST /posture - Receive posture transition from relay
 */
app.post('/posture', async (req, res) => {
  const { source, device, fromPosture, toPosture, fromDurationSeconds, confidence } = req.body;

  if (!fromPosture || !toPosture) {
    return res.status(400).json({ ok: false, error: 'Invalid request' });
  }

  await writers.writePostureTransition(fromPosture, toPosture, fromDurationSeconds, confidence, source);

  res.json({ ok: true });
});

/**
 * POST /status - Receive status events from relays and iOS app
 */
app.post('/status', async (req, res) => {
  const { source, device, category, event, description, fields } = req.body;

  if (!category || !event) {
    return res.status(400).json({ ok: false, error: 'category and event are required' });
  }

  // Log all events
  const qualifiedName = `${category}.${event}`;
  const sourceStr = source ? `[${source}] ` : '';
  const descStr = description ? ` — ${description}` : '';
  const fieldsStr = fields ? ` ${JSON.stringify(fields)}` : '';
  log(`${sourceStr}${qualifiedName}${descStr}${fieldsStr}`);

  // Only persist allow-listed events to InfluxDB
  if (PERSIST_EVENTS.has(qualifiedName)) {
    await writers.writeRelayStatus(category, event, source, device, fields);
  }

  // Update relay state (SSE status page sees everything)
  if (source) {
    relayState.set(source, {
      lastSeen: Date.now(),
      device,
      status: event,
      lastEvent: qualifiedName
    });
  }

  // Reset device state on disconnect
  if (category === 'ble' && event === 'disconnected' && device) {
    deviceState.delete(device);
    latestBeatData.delete(device);
  }

  // Broadcast status update to SSE clients
  broadcastStatus();

  res.json({ ok: true });
});

/**
 * GET / - Serve status page (read from disk on every request for live editing)
 */
app.get('/', async (req, res) => {
  try {
    const html = await readFile(join(__dirname, 'public', 'status.html'), 'utf-8');
    res.set({
      'Cache-Control': 'no-store, no-cache, must-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0'
    });
    res.type('html').send(html);
  } catch (err) {
    res.status(500).send('Status page not found. Create src/public/status.html');
  }
});

/**
 * GET /events - Server-Sent Events endpoint for real-time status updates
 */
app.get('/events', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  res.flushHeaders();

  sseClients.add(res);
  log(`SSE client connected (total: ${sseClients.size})`);

  const payload = JSON.stringify(buildStatusPayload());
  res.write(`data: ${payload}\n\n`);

  req.on('close', () => {
    sseClients.delete(res);
    log(`SSE client disconnected (total: ${sseClients.size})`);
  });
});

/**
 * GET /health - Health check endpoint
 */
app.get('/health', (req, res) => {
  res.json({
    ok: true,
    devices: deviceState.size
  });
});

// Start server and post-processor
server.listen(PORT, () => {
  log(`Polar Hub listening on port ${PORT}`);
  log(`Status page available at http://localhost:${PORT}/`);
  log(`Recording gap-dedup threshold: ${GAP_THRESHOLD_MS}ms (dup proximity=${RECORDING_DUPLICATE_PROXIMITY_MS}ms, edge tolerance=${RECORDING_GAP_EDGE_TOLERANCE_MS}ms)`);
  if (LOG_REQUESTS) log(`Request logging enabled`);
  if (TEST_MODE) log(`TEST MODE: database=${config.influxDatabase}_test, port=${PORT}`);
  postProcessor.start();
});
