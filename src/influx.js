/**
 * InfluxDB client and write/query utilities for Polar Hub
 *
 * Measurements:
 *   polar_raw        — every beat as received + post-processed clean values
 *   polar_realtime   — per-beat HRV from 60-beat sliding window
 *   polar_hrv_summary — 5-min HRV summaries from rr_clean
 *   polar_posture     — posture transition events
 *   polar_relay_status — relay status events
 */

import Influx from 'influx';

/**
 * Create an InfluxDB client
 */
export function createInfluxClient({ host, port, database }) {
  return new Influx.InfluxDB({
    host,
    port,
    database,
    requestTimeout: 5000
  });
}

/**
 * Create data writers and query functions for InfluxDB
 */
export function createWriters(influx, log = () => {}) {

  const BATCH_CHUNK_SIZE = 5000;

  // --- Helper: chunked write ---
  async function chunkedWrite(points, label) {
    try {
      for (let i = 0; i < points.length; i += BATCH_CHUNK_SIZE) {
        await influx.writePoints(points.slice(i, i + BATCH_CHUNK_SIZE), { precision: 'ms' });
      }
    } catch (err) {
      log(`InfluxDB write error (${label}): ${err.message}`);
      throw err;
    }
  }

  // --- Helper: escape device for InfluxQL ---
  function esc(device) {
    return device.replace(/'/g, "\\'");
  }

  // =========================================================================
  // polar_raw — raw beats
  // =========================================================================

  /**
   * Write a single raw beat to polar_raw
   */
  async function writeRawBeat(device, timestamp, rr, heartRate, source, path) {
    const tags = {};
    if (device) tags.device = device;

    const fields = { rr_interval: rr };
    if (heartRate != null) fields.heart_rate = heartRate;
    if (source) fields.source = source;
    if (path) fields.path = path;

    try {
      await influx.writePoints([
        { measurement: 'polar_raw', tags, fields, timestamp }
      ], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (rawBeat): ${err.message}`);
    }
  }

  /**
   * Write a batch of raw beat points to polar_raw (chunked)
   * Each point: { timestamp, rr_interval, heart_rate?, source?, path }
   */
  async function writeRawBatch(device, points) {
    const tags = device ? { device } : {};
    const influxPoints = points.map(p => {
      const fields = { rr_interval: p.rr_interval };
      if (p.heart_rate != null) fields.heart_rate = p.heart_rate;
      if (p.source) fields.source = p.source;
      if (p.path) fields.path = p.path;
      return { measurement: 'polar_raw', tags, fields, timestamp: p.timestamp };
    });
    await chunkedWrite(influxPoints, 'rawBatch');
  }

  /**
   * Merge post-processed fields into existing polar_raw points.
   * Each point: { timestamp, rr_clean?, hr_clean?, artifact_type? }
   * InfluxDB merges fields on same measurement + tags + timestamp.
   */
  async function writeCleanFields(device, points) {
    const tags = device ? { device } : {};
    const influxPoints = points.map(p => {
      const fields = {};
      if (p.rr_clean != null) fields.rr_clean = p.rr_clean;
      if (p.hr_clean != null) fields.hr_clean = p.hr_clean;
      if (p.artifact_type) fields.artifact_type = p.artifact_type;
      // For rr_clean=null (absorbed extra beats), write a sentinel
      if (p.rr_clean === null && p.artifact_type) {
        fields.artifact_type = p.artifact_type;
        fields.rr_clean = 0; // sentinel for "absorbed" — rr_clean=0 means no real beat
      }
      return { measurement: 'polar_raw', tags, fields, timestamp: p.timestamp };
    });
    await chunkedWrite(influxPoints, 'cleanFields');
  }

  /**
   * Insert a synthetic beat (e.g. for missed-beat split).
   * rr_interval is null (it's synthetic), rr_clean has the corrected value.
   */
  async function writeInsertedBeat(device, timestamp, rrClean, artifactType) {
    const tags = device ? { device } : {};
    const fields = {
      rr_clean: rrClean,
      hr_clean: Math.round(60000 / rrClean * 100) / 100,
      artifact_type: artifactType
    };
    try {
      await influx.writePoints([
        { measurement: 'polar_raw', tags, fields, timestamp }
      ], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (insertedBeat): ${err.message}`);
    }
  }

  // =========================================================================
  // polar_realtime — real-time HRV from sliding window
  // =========================================================================

  /**
   * Write a real-time HRV data point
   */
  async function writeRealtime(device, timestamp, hrv, hr) {
    const tags = device ? { device } : {};
    const fields = {};
    if (hrv.rmssd != null) fields.rmssd = hrv.rmssd;
    if (hrv.sdnn != null) fields.sdnn = hrv.sdnn;
    if (hrv.pnn50 != null) fields.pnn50 = hrv.pnn50;
    if (hr != null) fields.hr = hr;

    if (Object.keys(fields).length === 0) return; // nothing to write

    try {
      await influx.writePoints([
        { measurement: 'polar_realtime', tags, fields, timestamp }
      ], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (realtime): ${err.message}`);
    }
  }

  // =========================================================================
  // polar_hrv_summary — 5-min summaries
  // =========================================================================

  /**
   * Write a 5-min HRV summary
   */
  async function writeSummary(device, hrv, avgHR, sampleCount, artifactCount, posture, timestamp) {
    const tags = {};
    if (device) tags.device = device;
    if (posture && posture !== 'unknown') tags.posture = posture;

    const fields = {
      rmssd: hrv.rmssd,
      sdnn: hrv.sdnn,
      pnn50: hrv.pnn50,
      heart_rate: avgHR,
      sample_count: sampleCount,
      artifact_count: artifactCount
    };

    try {
      await influx.writePoints([
        { measurement: 'polar_hrv_summary', tags, fields, timestamp: timestamp ?? new Date() }
      ], { precision: 'ms' });
      log(`HRV summary: RMSSD=${hrv.rmssd} HR=${avgHR} samples=${sampleCount} artifacts=${artifactCount}`);
    } catch (err) {
      log(`InfluxDB write error (summary): ${err.message}`);
    }
  }

  // =========================================================================
  // Queries — polar_raw
  // =========================================================================

  /**
   * Query raw beats by device and time range.
   * Returns [{time, rr_interval, rr_clean}] sorted by time ASC.
   */
  async function queryRawBeats(device, startMs, endMs) {
    const results = await influx.query(`
      SELECT rr_interval, rr_clean FROM polar_raw
      WHERE "device" = '${esc(device)}' AND time >= ${startMs}ms AND time <= ${endMs}ms
      ORDER BY time ASC
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval,
      rr_clean: r.rr_clean
    }));
  }

  /**
   * Query N beats before a timestamp (context for Lipponen).
   * Returns in chronological order (oldest first).
   */
  async function queryBeatsBefore(device, beforeMs, limit = 91) {
    const results = await influx.query(`
      SELECT rr_interval FROM polar_raw
      WHERE "device" = '${esc(device)}' AND time < ${beforeMs}ms AND rr_interval > 0
      ORDER BY time DESC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval
    })).reverse();
  }

  /**
   * Query N beats after a timestamp (right context for Lipponen).
   * Returns in chronological order.
   */
  async function queryBeatsAfter(device, afterMs, limit = 91) {
    const results = await influx.query(`
      SELECT rr_interval FROM polar_raw
      WHERE "device" = '${esc(device)}' AND time > ${afterMs}ms AND rr_interval > 0
      ORDER BY time ASC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval
    }));
  }

  /**
   * Query the timestamp of the last processed beat (has rr_clean).
   * Returns epoch ms, or null if no processed beats exist.
   */
  async function queryLastProcessedTime(device) {
    try {
      const results = await influx.query(`
        SELECT last(rr_clean) FROM polar_raw
        WHERE "device" = '${esc(device)}' AND rr_clean > 0
      `);
      if (results.length > 0 && results[0].time) {
        return results[0].time.getTime();
      }
    } catch {
      // measurement may not exist yet
    }
    return null;
  }

  /**
   * Query rr_clean values for a time range (for 5-min summary computation).
   * Only returns beats where rr_clean exists and is > 0 (skips absorbed beats).
   */
  async function queryCleanBeats(device, startMs, endMs) {
    const results = await influx.query(`
      SELECT rr_clean FROM polar_raw
      WHERE "device" = '${esc(device)}' AND time >= ${startMs}ms AND time <= ${endMs}ms AND rr_clean > 0
      ORDER BY time ASC
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_clean: r.rr_clean
    }));
  }

  // =========================================================================
  // Unchanged: posture and relay status
  // =========================================================================

  async function writePostureTransition(fromPosture, toPosture, fromDurationSeconds, confidence, source = null) {
    const tags = { from_posture: fromPosture, to_posture: toPosture };
    if (source) tags.source = source;

    try {
      await influx.writePoints([{
        measurement: 'polar_posture',
        tags,
        fields: { from_duration_seconds: fromDurationSeconds, confidence },
        timestamp: new Date()
      }], { precision: 'ms' });
      log(`Posture: ${fromPosture} -> ${toPosture} (was ${fromDurationSeconds}s, confidence ${confidence})`);
    } catch (err) {
      log(`InfluxDB write error (posture): ${err.message}`);
    }
  }

  async function writeRelayStatus(category, event, source, device, fields) {
    const tags = { category, event };
    if (source) tags.source = source;
    if (device) tags.device = device;

    const writeFields = (fields && Object.keys(fields).length > 0)
      ? { ...fields }
      : { value: 1 };

    try {
      await influx.writePoints([{
        measurement: 'polar_relay_status',
        tags,
        fields: writeFields,
        timestamp: new Date()
      }], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (status): ${err.message}`);
    }
  }

  return {
    writeRawBeat,
    writeRawBatch,
    writeCleanFields,
    writeInsertedBeat,
    writeRealtime,
    writeSummary,
    writePostureTransition,
    writeRelayStatus,
    queryRawBeats,
    queryBeatsBefore,
    queryBeatsAfter,
    queryLastProcessedTime,
    queryCleanBeats
  };
}
