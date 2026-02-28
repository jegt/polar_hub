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
const PORT = config.port;
const LOG_REQUESTS = process.argv.includes('--log-requests');
const GAP_THRESHOLD_MS = 300;
const REALTIME_WINDOW_SIZE = 60;

// Create components
const influx = createInfluxClient({
  host: config.influxHost,
  port: config.influxPort,
  database: config.influxDatabase
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
const logStream = createWriteStream(join(logDir, 'hub.log'), { flags: 'a' });

function localTimestamp(date = new Date()) {
  return date.toLocaleString('sv-SE', { hour12: false }).replace(' ', 'T');
}

function log(message) {
  const line = `[${localTimestamp()}] ${message}`;
  console.log(line);
  logStream.write(line + '\n');
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
 * POST /beats - Receive heartbeat data from relay (real-time path)
 *
 * Writes raw RR to polar_raw, runs Lipponen on 60-beat window for
 * real-time HRV, writes to polar_realtime. Post-processor handles
 * definitive rr_clean values later.
 */
app.post('/beats', async (req, res) => {
  const { source, device, timestamp, heartRate, rrIntervals, posture, rssi } = req.body;

  if (!device || !rrIntervals || !Array.isArray(rrIntervals)) {
    return res.status(400).json({ ok: false, error: 'Invalid request' });
  }

  if (rrIntervals.length === 0) {
    return res.json({ ok: true, received: 0 });
  }

  const state = getDeviceState(device);
  state.lastPosture = posture || 'unknown';

  const beatTimestamp = timestamp || Date.now();

  // Write each RR to polar_raw and push to real-time window
  let cumulativeRR = 0;
  for (let i = 0; i < rrIntervals.length; i++) {
    const rr = rrIntervals[i];
    const rrTimestamp = beatTimestamp + cumulativeRR;
    cumulativeRR += rr;

    state.totalBeats++;

    // Write raw beat to InfluxDB (no filtering — raw as received)
    writers.writeRawBeat(device, rrTimestamp, rr, heartRate, source, 'realtime');

    // Push to real-time window
    state.rrWindow.push(rr);
    if (state.rrWindow.length > REALTIME_WINDOW_SIZE) {
      state.rrWindow.shift();
    }
  }

  // Run Lipponen on the real-time window for dashboard HRV
  let hrv = { rmssd: null, sdnn: null, pnn50: null };
  let windowHR = heartRate;

  if (state.rrWindow.length >= 4) {
    const analysis = analyzeRR(state.rrWindow);
    const cleanRRs = analysis.cleanSeries;

    if (cleanRRs.length >= 2) {
      hrv = calculateHRV(cleanRRs);
      const meanCleanRR = cleanRRs.reduce((a, b) => a + b, 0) / cleanRRs.length;
      windowHR = Math.round(60000 / meanCleanRR);
    }
  }

  // Write real-time HRV to polar_realtime
  const lastRRTimestamp = beatTimestamp + cumulativeRR - rrIntervals[rrIntervals.length - 1];
  writers.writeRealtime(device, lastRRTimestamp, hrv, windowHR);

  // Track RMSSD for visualization
  if (hrv.rmssd !== null) {
    state.rmssdBuffer.push(hrv.rmssd);
    if (state.rmssdBuffer.length > 30) {
      state.rmssdBuffer.shift();
    }
  }

  // Store latest beat data for SSE broadcast
  latestBeatData.set(device, {
    heartRate: windowHR,
    rr: rrIntervals[rrIntervals.length - 1],
    hrv,
    rssi,
    source,
    timestamp: lastRRTimestamp
  });

  // Update relay state
  if (source) {
    relayState.set(source, {
      lastSeen: Date.now(),
      device,
      rssi,
      status: 'active'
    });
  }

  // Log
  const rr = rrIntervals[rrIntervals.length - 1];
  const hrvStr = hrv.rmssd !== null ? `RMSSD=${hrv.rmssd}` : 'RMSSD=-';
  const postureStr = posture && posture !== 'unknown' ? ` [${posture}]` : '';
  const rssiStr = rssi != null ? ` ${rssi}dBm` : '';
  const sourceStr = source ? `[${source}] ` : '';
  log(`${sourceStr}#${state.totalBeats} HR=${windowHR} RR=${rr.toFixed(0)}ms ${hrvStr}${rssiStr}${postureStr}`);

  // Broadcast status update to SSE clients
  broadcastStatus();

  res.json({ ok: true, received: rrIntervals.length });
});

/**
 * POST /beats/batch - Receive historical beat data from iOS app
 *
 * Deduplicates using gap detection, writes raw beats to polar_raw,
 * triggers post-processor for artifact correction.
 * heartRate is optional — batch uploads from recorded RR data may not have it.
 */
app.post('/beats/batch', async (req, res) => {
  const { source, device, beats } = req.body;

  if (!device || !beats || !Array.isArray(beats) || beats.length === 0) {
    return res.status(400).json({ ok: false, error: 'Require device and non-empty beats array' });
  }

  // Validate: each beat needs at least a timestamp
  for (let i = 0; i < beats.length; i++) {
    const beat = beats[i];
    if (typeof beat.timestamp !== 'number') {
      return res.status(400).json({ ok: false, error: `Beat ${i} missing timestamp` });
    }
  }

  // Ensure device is registered with post-processor
  await postProcessor.registerDevice(device);

  // Flatten incoming beats to individual RR points (skip beats without rrIntervals)
  const incomingPoints = [];
  for (const beat of beats) {
    if (beat.rrIntervals && beat.rrIntervals.length > 0) {
      let ts = beat.timestamp;
      for (const rr of beat.rrIntervals) {
        incomingPoints.push({
          timestamp: ts,
          rr_interval: rr,
          heart_rate: beat.heartRate || null,
          source,
          path: 'batch'
        });
        ts += rr;
      }
    }
  }

  if (incomingPoints.length === 0) {
    return res.json({ ok: true, received: 0, new: 0, duplicates: 0 });
  }

  incomingPoints.sort((a, b) => a.timestamp - b.timestamp);

  const firstTs = incomingPoints[0].timestamp;
  const lastTs = incomingPoints[incomingPoints.length - 1].timestamp;

  // Query existing points for gap-based dedup (2s padding covers any RR at boundaries)
  let existing = [];
  try {
    existing = await writers.queryRawBeats(device, firstTs - 2000, lastTs + 2000);
  } catch (err) {
    log(`Dedup query failed, writing all: ${err.message}`);
  }

  // Gap-based dedup: find gaps in existing data, only insert batch beats into gaps
  let newPoints;

  if (existing.length === 0) {
    newPoints = incomingPoints;
  } else {
    const gaps = [];
    for (let i = 0; i < existing.length - 1; i++) {
      const expectedNext = existing[i].time + (existing[i].rr_interval || 0);
      const actualNext = existing[i + 1].time;
      if (actualNext - expectedNext > GAP_THRESHOLD_MS) {
        gaps.push({ start: expectedNext, end: actualNext });
      }
    }

    // Leading edge
    if (firstTs < existing[0].time - GAP_THRESHOLD_MS) {
      gaps.unshift({ start: firstTs, end: existing[0].time });
    }

    // Trailing edge
    const lastEx = existing[existing.length - 1];
    const trailingStart = lastEx.time + (lastEx.rr_interval || 0);
    if (lastTs > trailingStart + GAP_THRESHOLD_MS) {
      gaps.push({ start: trailingStart, end: lastTs + 2000 });
    }

    newPoints = incomingPoints.filter(p =>
      gaps.some(g => p.timestamp >= g.start - GAP_THRESHOLD_MS
                  && p.timestamp <= g.end + GAP_THRESHOLD_MS));
  }

  const duplicateCount = incomingPoints.length - newPoints.length;

  // Write new points to polar_raw
  if (newPoints.length > 0) {
    try {
      await writers.writeRawBatch(device, newPoints);
    } catch (err) {
      return res.status(500).json({ ok: false, error: 'InfluxDB write failed' });
    }

    // Trigger post-processor to reprocess from the batch start
    postProcessor.triggerReprocess(device, firstTs);
  }

  // Log
  const durationMin = ((lastTs - firstTs) / 60000).toFixed(1);
  const deviceShort = device.length > 11 ? device.slice(0, 11) + '...' : device;
  const fmt = ts => localTimestamp(new Date(ts)).slice(11, 19);
  log(`Batch: ${incomingPoints.length} beats, ${newPoints.length} new, ${duplicateCount} dupes from ${source || 'unknown'} for ${deviceShort} | ${fmt(firstTs)} → ${fmt(lastTs)} (${durationMin}min)`);

  res.json({ ok: true, received: incomingPoints.length, new: newPoints.length, duplicates: duplicateCount });
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
  log(`Gap-based batch dedup threshold: ${GAP_THRESHOLD_MS}ms`);
  if (LOG_REQUESTS) log(`Request logging enabled`);
  postProcessor.start();
});
