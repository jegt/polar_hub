/**
 * InfluxDB client and write utilities for Polar Hub
 */

import Influx from 'influx';

/**
 * Create an InfluxDB client
 * @param {Object} options - Connection options (host, port, database)
 * @returns {Object} InfluxDB client instance
 */
export function createInfluxClient({ host, port, database }) {

  return new Influx.InfluxDB({ 
    host, 
    port, 
    database,
    requestTimeout: 5000  // 5 second timeout to prevent blocking
  });
}

/**
 * Create HRV data writers for InfluxDB
 * @param {Object} influx - InfluxDB client instance
 * @param {Function} log - Logging function (optional)
 * @returns {Object} Writer functions
 */
export function createHRVWriters(influx, log = () => {}) {
  
  /**
   * Write raw HRV data point
   */
  async function writeRaw(device, timestamp, heartRate, rr, hrv, posture = null, source = null) {
    const tags = {};
    if (device) {
      tags.device = device;
    }

    const fields = {
      heart_rate: heartRate,
      rr_interval: rr,
      path: 'realtime'
    };
    if (posture && posture !== 'unknown') {
      fields.posture = posture;
    }
    if (source) {
      fields.source = source;
    }
    if (hrv.rmssd !== null) {
      fields.rmssd = hrv.rmssd;
      fields.sdnn = hrv.sdnn;
    }

    try {
      await influx.writePoints([
        {
          measurement: 'polar_hrv_raw',
          tags,
          fields,
          timestamp
        }
      ], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (raw): ${err.message}`);
    }
  }

  /**
   * Write a batch of beats to InfluxDB
   * Points are identified by measurement + tags + timestamp, so re-uploading
   * the same batch is idempotent (InfluxDB overwrites identical points).
   */
  const BATCH_CHUNK_SIZE = 5000;

  async function writeBatch(device, beats, source = null) {
    const tags = device ? { device } : {};
    const points = [];
    for (const beat of beats) {
      if (beat.rrIntervals && beat.rrIntervals.length > 0) {
        let ts = beat.timestamp;
        for (const rr of beat.rrIntervals) {
          const fields = { heart_rate: beat.heartRate, rr_interval: rr };
          if (source) fields.source = source;
          points.push({ measurement: 'polar_hrv_raw', tags, fields, timestamp: ts });
          ts += rr;
        }
      } else {
        const fields = { heart_rate: beat.heartRate };
        if (source) fields.source = source;
        points.push({ measurement: 'polar_hrv_raw', tags, fields, timestamp: beat.timestamp });
      }
    }

    try {
      for (let i = 0; i < points.length; i += BATCH_CHUNK_SIZE) {
        const chunk = points.slice(i, i + BATCH_CHUNK_SIZE);
        await influx.writePoints(chunk, { precision: 'ms' });
      }
    } catch (err) {
      log(`InfluxDB write error (batch): ${err.message}`);
      throw err;
    }
  }

  /**
   * Query raw beats by device and time range.
   * Returns [{time (epoch ms), rr_interval}] sorted by time ASC.
   */
  async function queryRawBeats(device, startMs, endMs) {
    const dev = device.replace(/'/g, "\\'");
    const results = await influx.query(`
      SELECT rr_interval FROM polar_hrv_raw
      WHERE "device" = '${dev}' AND time >= ${startMs}ms AND time <= ${endMs}ms
      ORDER BY time ASC
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval
    }));
  }

  /**
   * Query N beats before a timestamp (for RMSSD seed).
   * Returns in chronological order (oldest first).
   */
  async function queryBeatsBefore(device, beforeMs, limit = 30) {
    const dev = device.replace(/'/g, "\\'");
    const results = await influx.query(`
      SELECT rr_interval FROM polar_hrv_raw
      WHERE "device" = '${dev}' AND time < ${beforeMs}ms
      ORDER BY time DESC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval
    })).reverse();
  }

  /**
   * Query N beats after a timestamp (for RMSSD tail).
   */
  async function queryBeatsAfter(device, afterMs, limit = 30) {
    const dev = device.replace(/'/g, "\\'");
    const results = await influx.query(`
      SELECT rr_interval FROM polar_hrv_raw
      WHERE "device" = '${dev}' AND time > ${afterMs}ms
      ORDER BY time ASC LIMIT ${limit}
    `);
    return results.map(r => ({
      time: r.time.getTime(),
      rr_interval: r.rr_interval
    }));
  }

  /**
   * Write/merge specific fields to existing points.
   * InfluxDB merges fields on same measurement + tags + timestamp.
   */
  async function writeFields(device, points) {
    const tags = device ? { device } : {};
    const influxPoints = points.map(p => ({
      measurement: 'polar_hrv_raw',
      tags,
      fields: p.fields,
      timestamp: p.timestamp
    }));
    try {
      for (let i = 0; i < influxPoints.length; i += BATCH_CHUNK_SIZE) {
        await influx.writePoints(influxPoints.slice(i, i + BATCH_CHUNK_SIZE), { precision: 'ms' });
      }
    } catch (err) {
      log(`InfluxDB write error (writeFields): ${err.message}`);
      throw err;
    }
  }

  /**
   * Write HRV summary
   */
  async function writeSummary(hrv, avgHR, sampleCount, posture = null, timestamp = null) {
    const tags = {};
    if (posture && posture !== 'unknown') {
      tags.posture = posture;
    }

    try {
      await influx.writePoints([
        {
          measurement: 'polar_hrv',
          tags,
          fields: {
            rmssd: hrv.rmssd,
            sdnn: hrv.sdnn,
            pnn50: hrv.pnn50,
            heart_rate: avgHR,
            sample_count: sampleCount
          },
          timestamp: timestamp ?? new Date()
        }
      ], { precision: 'ms' });
      
      const postureStr = posture && posture !== 'unknown' ? ` [${posture}]` : '';
      log(`HRV summary written: RMSSD=${hrv.rmssd}ms, HR=${avgHR}bpm, samples=${sampleCount}${postureStr}`);
    } catch (err) {
      log(`InfluxDB write error (summary): ${err.message}`);
    }
  }

  /**
   * Write posture transition event
   */
  async function writePostureTransition(fromPosture, toPosture, fromDurationSeconds, confidence, source = null) {
    const tags = {
      from_posture: fromPosture,
      to_posture: toPosture
    };
    if (source) {
      tags.source = source;
    }

    try {
      await influx.writePoints([
        {
          measurement: 'polar_posture',
          tags,
          fields: {
            from_duration_seconds: fromDurationSeconds,
            confidence
          },
          timestamp: new Date()
        }
      ], { precision: 'ms' });
      
      log(`Posture: ${fromPosture} -> ${toPosture} (was ${fromDurationSeconds}s, confidence ${confidence})`);
    } catch (err) {
      log(`InfluxDB write error (posture): ${err.message}`);
    }
  }

  /**
   * Write relay status event
   */
  async function writeRelayStatus(category, event, source, device, fields) {
    const tags = { category, event };
    if (source) tags.source = source;
    if (device) tags.device = device;

    const writeFields = (fields && Object.keys(fields).length > 0)
      ? { ...fields }
      : { value: 1 };

    try {
      await influx.writePoints([
        {
          measurement: 'polar_relay_status',
          tags,
          fields: writeFields,
          timestamp: new Date()
        }
      ], { precision: 'ms' });
    } catch (err) {
      log(`InfluxDB write error (status): ${err.message}`);
    }
  }

  return {
    writeRaw,
    writeBatch,
    writeFields,
    writeSummary,
    writePostureTransition,
    writeRelayStatus,
    queryRawBeats,
    queryBeatsBefore,
    queryBeatsAfter
  };
}
