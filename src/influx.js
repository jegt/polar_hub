/**
 * InfluxDB client and write/query utilities for Polar Hub
 *
 * Measurements:
 *   polar_raw        — every beat as received + post-processed clean values
 *   polar_realtime   — per-beat HRV from 60-beat sliding window
 *   polar_hrv_summary — 5-min HRV summaries from rr_clean
 *   polar_quality_1m — 1-min data-quality metrics from canonical processing
 *   polar_upload_receipts — processed batch upload ids for idempotency
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

  // --- Helper: map raw beat payload to Influx fields ---
  function buildRawFields(p) {
    const fields = {};
    if (p.rr_interval != null) fields.rr_interval = p.rr_interval;
    if (p.heart_rate != null) fields.heart_rate = p.heart_rate;
    if (p.source) fields.source = p.source;
    if (p.path) fields.path = p.path;
    if (p.session_id) fields.session_id = p.session_id;
    if (p.beat_seq != null) fields.beat_seq = p.beat_seq;
    if (p.batch_type) fields.batch_type = p.batch_type;
    if (p.upload_id) fields.upload_id = p.upload_id;
    if (p.recording_id) fields.recording_id = p.recording_id;
    if (p.recording_seq != null) fields.recording_seq = p.recording_seq;
    return fields;
  }

  // =========================================================================
  // polar_raw — raw beats
  // =========================================================================

  /**
   * Write a single raw beat to polar_raw
   */
  async function writeRawBeat(device, point) {
    const tags = {};
    if (device) tags.device = device;

    const fields = buildRawFields(point);
    if (Object.keys(fields).length === 0) return;

    try {
      await influx.writePoints([
        { measurement: 'polar_raw', tags, fields, timestamp: point.timestamp }
      ], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (rawBeat): ${err.message}`);
    }
  }

  /**
   * Write a batch of raw beat points to polar_raw (chunked)
   * Each point: { timestamp, rr_interval, ...metadata }
   */
  async function writeRawBatch(device, points) {
    const tags = device ? { device } : {};
    const influxPoints = points.map(p => {
      const fields = buildRawFields(p);
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
      if (p.correction_applied != null) fields.correction_applied = p.correction_applied;
      if (p.correction_delta_ms != null) fields.correction_delta_ms = p.correction_delta_ms;
      if (p.weak_correction != null) fields.weak_correction = p.weak_correction;
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

  function buildRealtimeFields({ hrv = {}, hr = null, hr_instant = null, posture = null }) {
    const fields = {};
    if (hrv.rmssd != null) fields.rmssd = hrv.rmssd;
    if (hrv.sdnn != null) fields.sdnn = hrv.sdnn;
    if (hrv.pnn50 != null) fields.pnn50 = hrv.pnn50;
    if (hr != null) fields.hr = hr;
    if (hr_instant != null) fields.hr_instant = hr_instant;
    if (posture && posture !== 'unknown') fields.posture = posture;
    return fields;
  }

  /**
   * Write a real-time HRV data point
   */
  async function writeRealtime(device, timestamp, hrv, hr, posture, hrInstant = null) {
    const tags = device ? { device } : {};
    const fields = buildRealtimeFields({
      hrv,
      hr,
      hr_instant: hrInstant,
      posture
    });

    if (Object.keys(fields).length === 0) return; // nothing to write

    try {
      await influx.writePoints([
        { measurement: 'polar_realtime', tags, fields, timestamp }
      ], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (realtime): ${err.message}`);
    }
  }

  /**
   * Write a batch of real-time HRV points (chunked upsert).
   * Each point: { timestamp, hrv, hr, hr_instant?, posture? }
   */
  async function writeRealtimeBatch(device, points) {
    const tags = device ? { device } : {};
    const influxPoints = points.map(p => {
      const fields = {};
      if (p.hrv?.rmssd != null) fields.rmssd = p.hrv.rmssd;
      if (p.hrv?.sdnn != null) fields.sdnn = p.hrv.sdnn;
      if (p.hrv?.pnn50 != null) fields.pnn50 = p.hrv.pnn50;
      if (p.hr != null) fields.hr = p.hr;
      if (p.hr_instant != null) fields.hr_instant = p.hr_instant;
      if (p.posture && p.posture !== 'unknown') fields.posture = p.posture;
      return { measurement: 'polar_realtime', tags, fields, timestamp: p.timestamp };
    }).filter(p => Object.keys(p.fields).length > 0);

    if (influxPoints.length === 0) return;
    await chunkedWrite(influxPoints, 'realtimeBatch');
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
  // polar_quality_1m — 1-min canonical quality metrics
  // =========================================================================

  /**
   * Write one quality metrics point.
   */
  async function writeQuality(device, sample, timestamp) {
    const tags = device ? { device } : {};
    const fields = {
      sample_count: sample.sample_count ?? 0,
      artifact_count: sample.artifact_count ?? 0,
      artifact_rate: sample.artifact_rate ?? 0,
      corrected_count: sample.corrected_count ?? 0,
      correction_rate: sample.correction_rate ?? 0,
      weak_correction_count: sample.weak_correction_count ?? 0,
      max_consecutive_corrected: sample.max_consecutive_corrected ?? 0,
      correction_burst_level: sample.correction_burst_level ?? 'none',
      low_confidence: sample.low_confidence ?? false,
      unclassified_corrected_count: sample.unclassified_corrected_count ?? 0,
      out_of_order_count: sample.out_of_order_count ?? 0,
      extreme_rr_count: sample.extreme_rr_count ?? 0
    };

    try {
      await influx.writePoints([
        { measurement: 'polar_quality_1m', tags, fields, timestamp: timestamp ?? new Date() }
      ], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (quality): ${err.message}`);
    }
  }

  // =========================================================================
  // polar_upload_receipts — uploadId idempotency
  // =========================================================================

  /**
   * Store a processed batch upload receipt.
   */
  async function writeUploadReceipt(device, batchType, uploadId, stats = {}) {
    const tags = {};
    if (device) tags.device = device;
    if (batchType) tags.batch_type = batchType;
    if (uploadId) tags.upload_id = uploadId;

    const fields = {
      received: stats.received ?? 0,
      new_points: stats.new ?? 0,
      duplicate_points: stats.duplicates ?? 0
    };

    try {
      await influx.writePoints([{
        measurement: 'polar_upload_receipts',
        tags,
        fields,
        timestamp: Date.now()
      }], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (uploadReceipt): ${err.message}`);
      throw err;
    }
  }

  // =========================================================================
  // Queries — polar_raw
  // =========================================================================

  /**
   * Query raw beats by device and time range.
   * Returns beats sorted by time ASC with canonical metadata.
   */
  async function queryRawBeats(device, startMs, endMs) {
    const results = await influx.query(`
      SELECT rr_interval, rr_clean, artifact_type,
             correction_applied, correction_delta_ms, weak_correction,
             path, batch_type,
             session_id, beat_seq,
             recording_id, recording_seq
      FROM polar_raw
      WHERE "device" = '${esc(device)}' AND time >= ${startMs}ms AND time <= ${endMs}ms
      ORDER BY time ASC
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval,
      rr_clean: r.rr_clean,
      artifact_type: r.artifact_type,
      correction_applied: r.correction_applied,
      correction_delta_ms: r.correction_delta_ms,
      weak_correction: r.weak_correction,
      path: r.path,
      batch_type: r.batch_type,
      session_id: r.session_id,
      beat_seq: r.beat_seq != null ? Number(r.beat_seq) : null,
      recording_id: r.recording_id,
      recording_seq: r.recording_seq != null ? Number(r.recording_seq) : null
    }));
  }

  /**
   * Query N beats before a timestamp (context for Lipponen).
   * Returns in chronological order (oldest first).
   */
  async function queryBeatsBefore(device, beforeMs, limit = 91) {
    const results = await influx.query(`
      SELECT rr_interval, path, batch_type,
             session_id, beat_seq,
             recording_id, recording_seq
      FROM polar_raw
      WHERE "device" = '${esc(device)}' AND time < ${beforeMs}ms AND rr_interval > 0
      ORDER BY time DESC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval,
      path: r.path,
      batch_type: r.batch_type,
      session_id: r.session_id,
      beat_seq: r.beat_seq != null ? Number(r.beat_seq) : null,
      recording_id: r.recording_id,
      recording_seq: r.recording_seq != null ? Number(r.recording_seq) : null
    })).reverse();
  }

  /**
   * Query N beats before a timestamp for one session stream.
   * Returns in chronological order (oldest first).
   */
  async function queryBeatsBeforeSession(device, sessionId, beforeMs, limit = 91) {
    const results = await influx.query(`
      SELECT rr_interval, path, batch_type,
             session_id, beat_seq,
             recording_id, recording_seq
      FROM polar_raw
      WHERE "device" = '${esc(device)}'
        AND session_id = '${esc(sessionId)}'
        AND time < ${beforeMs}ms
        AND rr_interval > 0
      ORDER BY time DESC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval,
      path: r.path,
      batch_type: r.batch_type,
      session_id: r.session_id,
      beat_seq: r.beat_seq != null ? Number(r.beat_seq) : null,
      recording_id: r.recording_id,
      recording_seq: r.recording_seq != null ? Number(r.recording_seq) : null
    })).reverse();
  }

  /**
   * Query N beats before a timestamp for one recording stream.
   * Returns in chronological order (oldest first).
   */
  async function queryBeatsBeforeRecording(device, recordingId, beforeMs, limit = 91) {
    const results = await influx.query(`
      SELECT rr_interval, path, batch_type,
             session_id, beat_seq,
             recording_id, recording_seq
      FROM polar_raw
      WHERE "device" = '${esc(device)}'
        AND recording_id = '${esc(recordingId)}'
        AND time < ${beforeMs}ms
        AND rr_interval > 0
      ORDER BY time DESC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval,
      path: r.path,
      batch_type: r.batch_type,
      session_id: r.session_id,
      beat_seq: r.beat_seq != null ? Number(r.beat_seq) : null,
      recording_id: r.recording_id,
      recording_seq: r.recording_seq != null ? Number(r.recording_seq) : null
    })).reverse();
  }

  /**
   * Query N beats after a timestamp (right context for Lipponen).
   * Returns in chronological order.
   */
  async function queryBeatsAfter(device, afterMs, limit = 91) {
    const results = await influx.query(`
      SELECT rr_interval, path, batch_type,
             session_id, beat_seq,
             recording_id, recording_seq
      FROM polar_raw
      WHERE "device" = '${esc(device)}' AND time > ${afterMs}ms AND rr_interval > 0
      ORDER BY time ASC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval,
      path: r.path,
      batch_type: r.batch_type,
      session_id: r.session_id,
      beat_seq: r.beat_seq != null ? Number(r.beat_seq) : null,
      recording_id: r.recording_id,
      recording_seq: r.recording_seq != null ? Number(r.recording_seq) : null
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

  /**
   * Query clean beats with canonical metadata in a time range.
   * Includes synthetic inserted beats (rr_clean > 0).
   */
  async function queryCanonicalCleanBeats(device, startMs, endMs) {
    const results = await influx.query(`
      SELECT rr_clean, artifact_type, path, batch_type,
             session_id, beat_seq,
             recording_id, recording_seq
      FROM polar_raw
      WHERE "device" = '${esc(device)}'
        AND time >= ${startMs}ms
        AND time <= ${endMs}ms
        AND rr_clean > 0
      ORDER BY time ASC
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_clean: r.rr_clean,
      artifact_type: r.artifact_type,
      path: r.path,
      batch_type: r.batch_type,
      session_id: r.session_id,
      beat_seq: r.beat_seq != null ? Number(r.beat_seq) : null,
      recording_id: r.recording_id,
      recording_seq: r.recording_seq != null ? Number(r.recording_seq) : null
    }));
  }

  /**
   * Query clean beats before a timestamp (chronological order).
   */
  async function queryCleanBeatsBefore(device, beforeMs, limit = 59) {
    const results = await influx.query(`
      SELECT rr_clean, artifact_type, path, batch_type,
             session_id, beat_seq,
             recording_id, recording_seq
      FROM polar_raw
      WHERE "device" = '${esc(device)}'
        AND time < ${beforeMs}ms
        AND rr_clean > 0
      ORDER BY time DESC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_clean: r.rr_clean,
      artifact_type: r.artifact_type,
      path: r.path,
      batch_type: r.batch_type,
      session_id: r.session_id,
      beat_seq: r.beat_seq != null ? Number(r.beat_seq) : null,
      recording_id: r.recording_id,
      recording_seq: r.recording_seq != null ? Number(r.recording_seq) : null
    })).reverse();
  }

  /**
   * Query clean beats after a timestamp (chronological order).
   */
  async function queryCleanBeatsAfter(device, afterMs, limit = 59) {
    const results = await influx.query(`
      SELECT rr_clean, artifact_type, path, batch_type,
             session_id, beat_seq,
             recording_id, recording_seq
      FROM polar_raw
      WHERE "device" = '${esc(device)}'
        AND time > ${afterMs}ms
        AND rr_clean > 0
      ORDER BY time ASC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_clean: r.rr_clean,
      artifact_type: r.artifact_type,
      path: r.path,
      batch_type: r.batch_type,
      session_id: r.session_id,
      beat_seq: r.beat_seq != null ? Number(r.beat_seq) : null,
      recording_id: r.recording_id,
      recording_seq: r.recording_seq != null ? Number(r.recording_seq) : null
    }));
  }

  /**
   * Count artifact-marked beats in a time range.
   */
  async function queryArtifactCount(device, startMs, endMs) {
    try {
      const results = await influx.query(`
        SELECT count(artifact_type) FROM polar_raw
        WHERE "device" = '${esc(device)}'
          AND time >= ${startMs}ms
          AND time <= ${endMs}ms
          AND artifact_type <> 'none'
      `);
      if (results.length === 0 || !results[0].count) return 0;
      return Number(results[0].count) || 0;
    } catch (err) {
      if (err.message && err.message.includes('measurement not found')) return 0;
      throw err;
    }
  }

  /**
   * Return true if a live/buffered beat identity already exists.
   */
  async function queryBeatIdentityExists(device, sessionId, beatSeq) {
    try {
      const results = await influx.query(`
        SELECT beat_seq FROM polar_raw
        WHERE "device" = '${esc(device)}'
          AND session_id = '${esc(sessionId)}'
          AND beat_seq = ${beatSeq}
        LIMIT 1
      `);
      return results.length > 0;
    } catch (err) {
      if (err.message && err.message.includes('measurement not found')) return false;
      throw err;
    }
  }

  /**
   * Query existing live/buffered beat sequence numbers for a session range.
   */
  async function queryExistingBeatSeqs(device, sessionId, minSeq, maxSeq) {
    try {
      const results = await influx.query(`
        SELECT beat_seq FROM polar_raw
        WHERE "device" = '${esc(device)}'
          AND session_id = '${esc(sessionId)}'
          AND beat_seq >= ${minSeq}
          AND beat_seq <= ${maxSeq}
      `);
      return new Set(
        results
          .map(r => Number(r.beat_seq))
          .filter(v => Number.isFinite(v))
      );
    } catch (err) {
      if (err.message && err.message.includes('measurement not found')) return new Set();
      throw err;
    }
  }

  /**
   * Query existing recording sequence numbers for a recording range.
   */
  async function queryExistingRecordingSeqs(device, recordingId, minSeq, maxSeq) {
    try {
      const results = await influx.query(`
        SELECT recording_seq FROM polar_raw
        WHERE "device" = '${esc(device)}'
          AND recording_id = '${esc(recordingId)}'
          AND recording_seq >= ${minSeq}
          AND recording_seq <= ${maxSeq}
      `);
      return new Set(
        results
          .map(r => Number(r.recording_seq))
          .filter(v => Number.isFinite(v))
      );
    } catch (err) {
      if (err.message && err.message.includes('measurement not found')) return new Set();
      throw err;
    }
  }

  /**
   * Query a processed upload receipt by idempotency key.
   */
  async function queryUploadReceipt(device, batchType, uploadId) {
    try {
      const results = await influx.query(`
        SELECT last(received) AS received, last(new_points) AS new_points, last(duplicate_points) AS duplicate_points
        FROM polar_upload_receipts
        WHERE "device" = '${esc(device)}'
          AND "batch_type" = '${esc(batchType)}'
          AND "upload_id" = '${esc(uploadId)}'
      `);

      if (results.length === 0 || !results[0].time) return null;
      return {
        received: results[0].received ?? 0,
        new: results[0].new_points ?? 0,
        duplicates: results[0].duplicate_points ?? 0,
        processedAt: results[0].time.getTime()
      };
    } catch (err) {
      if (err.message && err.message.includes('measurement not found')) return null;
      throw err;
    }
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
    writeRealtimeBatch,
    writeSummary,
    writeQuality,
    writeUploadReceipt,
    writePostureTransition,
    writeRelayStatus,
    queryRawBeats,
    queryBeatsBefore,
    queryBeatsBeforeSession,
    queryBeatsBeforeRecording,
    queryBeatsAfter,
    queryLastProcessedTime,
    queryCleanBeats,
    queryCanonicalCleanBeats,
    queryCleanBeatsBefore,
    queryCleanBeatsAfter,
    queryArtifactCount,
    queryBeatIdentityExists,
    queryExistingBeatSeqs,
    queryExistingRecordingSeqs,
    queryUploadReceipt
  };
}
