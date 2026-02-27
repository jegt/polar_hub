/**
 * Polar Hub Server
 * 
 * Central aggregation server that receives data from multiple relays,
 * deduplicates, calculates HRV metrics, and writes to InfluxDB.
 * 
 * Run with: npm start
 */

import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { readFile } from 'fs/promises';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { calculateHRV, createRRValidator, createRRBuffer } from './hrv.js';
import { createInfluxClient, createHRVWriters } from './influx.js';

// Get directory path for serving static files
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const PORT = parseInt(process.env.PORT || '3000');
const HRV_SUMMARY_INTERVAL_MS = parseInt(process.env.HRV_SUMMARY_INTERVAL_MS || '300000');
const LOG_REQUESTS = process.argv.includes('--log-requests');
const GAP_THRESHOLD_MS = 300;

// Artifact filtering
const RR_MIN_MS = 300;
const RR_MAX_MS = 2000;
const RR_MAX_DIFF_MS = 200;
const RR_MAX_DIFF_PCT = 0.20;
const SESSION_WARMUP_BEATS = 10;

// Create components
const influx = createInfluxClient();
const writers = createHRVWriters(influx, log);

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
      rrBuffer: createRRBuffer(30),
      rmssdBuffer: [],  // Rolling buffer of last 30 RMSSD values for visualization
      summaryRRBuffer: [],  // {timestamp, rr} pairs for 5-min summaries
      totalBeats: 0,
      lastPosture: 'unknown',
      isValidRR: createRRValidator({
        minMs: RR_MIN_MS,
        maxMs: RR_MAX_MS,
        maxDiffMs: RR_MAX_DIFF_MS,
        maxDiffPct: RR_MAX_DIFF_PCT,
        warmupBeats: SESSION_WARMUP_BEATS
      })
    });
  }
  return deviceState.get(device);
}

/**
 * Log with timestamp
 */
function localTimestamp(date = new Date()) {
  return date.toLocaleString('sv-SE', { hour12: false }).replace(' ', 'T');
}

function log(message) {
  console.log(`[${localTimestamp()}] ${message}`);
}

/**
 * Write HRV summary for a device
 */
async function writeHRVSummary(device) {
  const state = deviceState.get(device);
  if (!state || state.summaryRRBuffer.length < 10) {
    return;
  }

  // Sort by timestamp and extract RR values
  const sortedRRs = state.summaryRRBuffer
    .sort((a, b) => a.timestamp - b.timestamp)
    .map(e => e.rr);
  
  const hrv = calculateHRV(sortedRRs);
  const avgHR = Math.round(60000 / (sortedRRs.reduce((a, b) => a + b, 0) / sortedRRs.length));
  
  await writers.writeSummary(hrv, avgHR, sortedRRs.length, state.lastPosture);
  state.summaryRRBuffer = [];
}

// HRV summary timer
setInterval(async () => {
  for (const device of deviceState.keys()) {
    await writeHRVSummary(device);
  }
}, HRV_SUMMARY_INTERVAL_MS);

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
      rrBuffer: state.rrBuffer.getAll(),
      rmssdBuffer: state.rmssdBuffer,
      summaryBufferSize: state.summaryRRBuffer.length,
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
 * POST /beats - Receive heartbeat data from relay
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

  // Process each RR interval
  let cumulativeRR = 0;
  for (let i = 0; i < rrIntervals.length; i++) {
    const rr = rrIntervals[i];
    const rrTimestamp = beatTimestamp + cumulativeRR;
    cumulativeRR += rr;
    
    state.totalBeats++;
    
    // Get previous RR by timestamp for artifact validation
    const previousRR = state.rrBuffer.getPreviousByTimestamp(rrTimestamp);
    const valid = state.isValidRR(rr, previousRR, state.totalBeats);
    
    if (valid) {
      state.rrBuffer.push(rr, rrTimestamp);
      state.summaryRRBuffer.push({ timestamp: rrTimestamp, rr });
    }

    const hrv = calculateHRV(state.rrBuffer.getAll());

    // Track RMSSD values for visualization (only when valid)
    if (valid && hrv.rmssd !== null) {
      state.rmssdBuffer.push(hrv.rmssd);
      if (state.rmssdBuffer.length > 30) {
        state.rmssdBuffer.shift();
      }
    }

    // Always store raw beats — artifact filter only gates HRV calculation
    writers.writeRaw(device, beatTimestamp, heartRate, rr, hrv, posture, source);

    // Store latest beat data for WebSocket broadcast
    latestBeatData.set(device, {
      heartRate,
      rr,
      hrv,
      rssi,
      source,
      timestamp: beatTimestamp,
      valid
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
    const artifact = valid ? '' : ' [ART]';
    const hrvStr = hrv.rmssd !== null ? `RMSSD=${hrv.rmssd}` : 'RMSSD=-';
    const postureStr = posture && posture !== 'unknown' ? ` [${posture}]` : '';
    const rssiStr = rssi !== null ? ` ${rssi}dBm` : '';
    const sourceStr = source ? `[${source}] ` : '';
    log(`${sourceStr}#${state.totalBeats} HR=${heartRate} RR=${rr.toFixed(0)}ms ${hrvStr}${rssiStr}${postureStr}${artifact}`);
  }

  // Broadcast status update to SSE clients
  broadcastStatus();

  res.json({ ok: true, received: rrIntervals.length });
});

/**
 * POST /beats/batch - Receive historical beat data from iOS app
 * Deduplicates using gap detection: finds gaps in existing data via ts+rr prediction,
 * only inserts batch beats where real-time coverage is missing.
 * Then recomputes per-beat RMSSD and 5-min HRV summaries.
 */
app.post('/beats/batch', async (req, res) => {
  const { source, device, beats } = req.body;

  if (!device || !beats || !Array.isArray(beats) || beats.length === 0) {
    return res.status(400).json({ ok: false, error: 'Require device and non-empty beats array' });
  }

  for (let i = 0; i < beats.length; i++) {
    const beat = beats[i];
    if (typeof beat.timestamp !== 'number' || typeof beat.heartRate !== 'number') {
      return res.status(400).json({ ok: false, error: `Beat ${i} missing timestamp or heartRate` });
    }
  }

  // Flatten incoming beats to individual RR points
  const incomingPoints = [];
  for (const beat of beats) {
    if (beat.rrIntervals && beat.rrIntervals.length > 0) {
      let ts = beat.timestamp;
      for (const rr of beat.rrIntervals) {
        incomingPoints.push({ timestamp: ts, rr_interval: rr, heart_rate: beat.heartRate });
        ts += rr;
      }
    } else {
      incomingPoints.push({ timestamp: beat.timestamp, rr_interval: null, heart_rate: beat.heartRate });
    }
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
    // No existing beats — entire range is a gap, write everything
    newPoints = incomingPoints;
  } else {
    // Find gaps between consecutive existing beats
    const gaps = [];
    for (let i = 0; i < existing.length - 1; i++) {
      const expectedNext = existing[i].time + existing[i].rr_interval;
      const actualNext = existing[i + 1].time;
      if (actualNext - expectedNext > GAP_THRESHOLD_MS) {
        gaps.push({ start: expectedNext, end: actualNext });
      }
    }

    // Leading edge: batch beats before first existing beat
    if (firstTs < existing[0].time - GAP_THRESHOLD_MS) {
      gaps.unshift({ start: firstTs, end: existing[0].time });
    }

    // Trailing edge: batch beats after last existing beat
    const lastEx = existing[existing.length - 1];
    const trailingStart = lastEx.time + lastEx.rr_interval;
    if (lastTs > trailingStart + GAP_THRESHOLD_MS) {
      gaps.push({ start: trailingStart, end: lastTs + 2000 });
    }

    // Keep only incoming beats that fall within a gap
    newPoints = incomingPoints.filter(p =>
      gaps.some(g => p.timestamp >= g.start - GAP_THRESHOLD_MS
                  && p.timestamp <= g.end + GAP_THRESHOLD_MS));
  }

  const duplicateCount = incomingPoints.length - newPoints.length;

  // Write new points to InfluxDB
  if (newPoints.length > 0) {
    try {
      const writeablePoints = newPoints.map(p => {
        const fields = { heart_rate: p.heart_rate, path: 'batch' };
        if (p.rr_interval !== null) fields.rr_interval = p.rr_interval;
        if (source) fields.source = source;
        return { fields, timestamp: p.timestamp };
      });
      await writers.writeFields(device, writeablePoints);
    } catch (err) {
      return res.status(500).json({ ok: false, error: 'InfluxDB write failed' });
    }

    // Recompute per-beat RMSSD for affected range
    try {
      const seedBeats = await writers.queryBeatsBefore(device, firstTs, 30);
      const windowBeats = await writers.queryRawBeats(device, firstTs, lastTs);
      const tailBeats = await writers.queryBeatsAfter(device, lastTs, 30);
      const allBeats = [...seedBeats, ...windowBeats, ...tailBeats];

      if (allBeats.length >= 2) {
        const rrBuffer = [];
        const updates = [];

        for (const beat of allBeats) {
          // Skip artifact beats (same thresholds as real-time filter)
          if (beat.rr_interval < RR_MIN_MS || beat.rr_interval > RR_MAX_MS) continue;

          rrBuffer.push(beat.rr_interval);
          if (rrBuffer.length > 30) rrBuffer.shift();

          // Only update beats from firstTs onward (seed beats untouched)
          if (beat.time >= firstTs && rrBuffer.length >= 2) {
            const hrv = calculateHRV([...rrBuffer]);
            if (hrv.rmssd !== null) {
              updates.push({
                fields: { rmssd: hrv.rmssd, sdnn: hrv.sdnn },
                timestamp: beat.time
              });
            }
          }
        }

        if (updates.length > 0) {
          await writers.writeFields(device, updates);
          log(`Batch RMSSD: recomputed ${updates.length} beats`);
        }
      }
    } catch (err) {
      log(`RMSSD recomputation failed: ${err.message}`);
    }

    // Recompute 5-min HRV summaries for affected windows
    try {
      const firstWindowStart = Math.floor(firstTs / HRV_SUMMARY_INTERVAL_MS) * HRV_SUMMARY_INTERVAL_MS;
      const lastWindowEnd = (Math.floor(lastTs / HRV_SUMMARY_INTERVAL_MS) + 1) * HRV_SUMMARY_INTERVAL_MS;
      const allRR = await writers.queryRawBeats(device, firstWindowStart, lastWindowEnd);

      const windows = new Map();
      for (const beat of allRR) {
        if (beat.rr_interval < RR_MIN_MS || beat.rr_interval > RR_MAX_MS) continue;
        const windowKey = Math.floor(beat.time / HRV_SUMMARY_INTERVAL_MS) * HRV_SUMMARY_INTERVAL_MS;
        if (!windows.has(windowKey)) windows.set(windowKey, []);
        windows.get(windowKey).push(beat.rr_interval);
      }

      let summariesWritten = 0;
      for (const [windowStart, rrValues] of windows) {
        if (rrValues.length < 10) continue;
        const hrv = calculateHRV(rrValues);
        if (hrv.rmssd === null) continue;
        const avgHR = Math.round(60000 / (rrValues.reduce((a, b) => a + b, 0) / rrValues.length));
        await writers.writeSummary(hrv, avgHR, rrValues.length, null, windowStart + HRV_SUMMARY_INTERVAL_MS);
        summariesWritten++;
      }
      if (summariesWritten > 0) {
        log(`Batch HRV: wrote ${summariesWritten} summaries`);
      }
    } catch (err) {
      log(`Summary recomputation failed: ${err.message}`);
    }
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
    const state = deviceState.get(device);
    if (state && state.summaryRRBuffer.length >= 10) {
      await writeHRVSummary(device);
    }
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
  // Set SSE headers
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  res.flushHeaders();

  // Add client to set
  sseClients.add(res);
  log(`SSE client connected (total: ${sseClients.size})`);

  // Send initial status
  const payload = JSON.stringify(buildStatusPayload());
  res.write(`data: ${payload}\n\n`);

  // Remove client on disconnect
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

// Start server
server.listen(PORT, () => {
  log(`Polar Hub listening on port ${PORT}`);
  log(`HRV summary interval: ${HRV_SUMMARY_INTERVAL_MS / 1000}s`);
  log(`Status page available at http://localhost:${PORT}/`);
  log(`Gap-based batch dedup threshold: ${GAP_THRESHOLD_MS}ms`);
  if (LOG_REQUESTS) log(`Request logging enabled`);
});
