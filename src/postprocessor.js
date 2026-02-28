/**
 * Post-processor: timer-based Lipponen artifact correction
 *
 * Runs every 60s, processes beats older than 120s (to ensure full
 * Lipponen look-ahead context). Writes rr_clean, hr_clean, artifact_type
 * back to polar_raw, and recomputes 5-min HRV summaries.
 */

import { analyzeRR } from './lipponen.js';
import { calculateHRV } from './hrv.js';

const PROCESS_INTERVAL_MS = 60_000;  // run every 60s
const PROCESS_BUFFER_MS = 120_000;   // don't process beats newer than 120s
const CONTEXT_BEATS = 91;            // Lipponen context window

/**
 * Create a post-processor instance.
 *
 * @param {Object} writers - InfluxDB writer functions from influx.js
 * @param {Function} log - Logging function
 * @param {Object} options - { summaryIntervalMs }
 * @returns {{ start(), registerDevice(device), triggerReprocess(device, fromMs) }}
 */
export function createPostProcessor(writers, log, { summaryIntervalMs = 300_000 } = {}) {
  const SUMMARY_INTERVAL_MS = summaryIntervalMs;
  // Per-device state: last timestamp we've processed up to
  const deviceState = new Map();
  let timer = null;

  /**
   * Register a device so the post-processor knows about it.
   * On first registration, queries InfluxDB for where we left off.
   */
  async function registerDevice(device) {
    if (deviceState.has(device)) return;

    const lastProcessed = await writers.queryLastProcessedTime(device);
    deviceState.set(device, {
      lastProcessedMs: lastProcessed ?? Date.now()
    });
    log(`Post-processor: registered device ${device} (last processed: ${lastProcessed ? new Date(lastProcessed).toISOString() : 'none'})`);
  }

  /**
   * Trigger reprocessing from a specific timestamp (e.g. when batch data arrives).
   * Moves lastProcessedMs backward if the batch starts before it.
   */
  function triggerReprocess(device, fromMs) {
    const state = deviceState.get(device);
    if (!state) return;

    if (fromMs < state.lastProcessedMs) {
      log(`Post-processor: reprocessing ${device} from ${new Date(fromMs).toISOString()}`);
      state.lastProcessedMs = fromMs;
    }
  }

  /**
   * Process one device: detect artifacts, correct, write back, update summaries.
   */
  async function processDevice(device) {
    const state = deviceState.get(device);
    if (!state) return;

    const now = Date.now();
    const cutoff = now - PROCESS_BUFFER_MS;

    // Nothing to process if lastProcessed is already at or past the cutoff
    if (state.lastProcessedMs >= cutoff) return;

    const targetStart = state.lastProcessedMs;
    const targetEnd = cutoff;

    // Query: left context + target range + right context
    const [leftContext, targetBeats, rightContext] = await Promise.all([
      writers.queryBeatsBefore(device, targetStart, CONTEXT_BEATS),
      writers.queryRawBeats(device, targetStart, targetEnd),
      writers.queryBeatsAfter(device, targetEnd, CONTEXT_BEATS)
    ]);

    // Filter to beats with rr_interval (skip synthetic inserted beats)
    const targetWithRR = targetBeats.filter(b => b.rr_interval != null && b.rr_interval > 0);
    if (targetWithRR.length === 0) {
      state.lastProcessedMs = targetEnd;
      return;
    }

    // Build full array for Lipponen: left context + target + right context
    const leftRR = leftContext.map(b => b.rr_interval);
    const targetRR = targetWithRR.map(b => b.rr_interval);
    const rightRR = rightContext.map(b => b.rr_interval).filter(rr => rr > 0);

    const fullRR = [...leftRR, ...targetRR, ...rightRR];

    if (fullRR.length < 4) {
      state.lastProcessedMs = targetEnd;
      return;
    }

    // Run Lipponen on the full array
    const analysis = analyzeRR(fullRR);

    // Extract results for target range only (skip left/right context)
    const targetStartIdx = leftRR.length;
    const targetEndIdx = targetStartIdx + targetRR.length;

    const cleanPoints = [];
    const insertedPoints = [];

    for (let i = targetStartIdx; i < targetEndIdx; i++) {
      const beat = targetWithRR[i - targetStartIdx];
      const result = analysis.results[i];

      if (result.artifact_type === 'missed') {
        // Original beat: rr_clean = RR/2
        const halfRR = result.rr_clean; // already set to rr/2 by analyzeRR
        cleanPoints.push({
          timestamp: beat.time,
          rr_clean: halfRR,
          hr_clean: Math.round(60000 / halfRR * 100) / 100,
          artifact_type: 'missed'
        });
        // Inserted beat: timestamp at original + RR/2
        insertedPoints.push({
          timestamp: beat.time + halfRR,
          rr_clean: halfRR,
          artifact_type: 'missed_inserted'
        });
      } else if (result.artifact_type === 'extra_absorbed') {
        // Absorbed: rr_clean=0 (sentinel), artifact_type marks it
        cleanPoints.push({
          timestamp: beat.time,
          rr_clean: 0,
          hr_clean: 0,
          artifact_type: 'extra_absorbed'
        });
      } else if (result.rr_clean != null) {
        // Normal, ectopic, extra (merged), longshort — all have a corrected value
        cleanPoints.push({
          timestamp: beat.time,
          rr_clean: result.rr_clean,
          hr_clean: Math.round(60000 / result.rr_clean * 100) / 100,
          artifact_type: result.artifact_type
        });
      }
    }

    // Write clean fields back to polar_raw
    if (cleanPoints.length > 0) {
      await writers.writeCleanFields(device, cleanPoints);
    }

    // Insert synthetic beats for missed-beat splits
    for (const p of insertedPoints) {
      await writers.writeInsertedBeat(device, p.timestamp, p.rr_clean, p.artifact_type);
    }

    // Log results
    const targetResults = analysis.results.slice(targetStartIdx, targetEndIdx);
    const artifactCount = targetResults.filter(r => r.artifact_type !== 'none').length;
    if (artifactCount > 0 || targetRR.length > 0) {
      log(`Post-processor: ${device.slice(0, 11)} | ${targetRR.length} beats, ${artifactCount} artifacts, ${insertedPoints.length} inserted`);
    }

    // Update lastProcessedMs
    state.lastProcessedMs = targetEnd;

    // Recompute 5-min summaries for affected windows
    await recomputeSummaries(device, targetStart, targetEnd);
  }

  /**
   * Recompute 5-min HRV summaries for any windows overlapping the processed range.
   */
  async function recomputeSummaries(device, startMs, endMs) {
    const windowStart = Math.floor(startMs / SUMMARY_INTERVAL_MS) * SUMMARY_INTERVAL_MS;
    const windowEnd = (Math.floor(endMs / SUMMARY_INTERVAL_MS) + 1) * SUMMARY_INTERVAL_MS;

    // Only compute summaries for complete windows (whose end is in the past)
    const now = Date.now();
    let summariesWritten = 0;

    for (let ws = windowStart; ws < windowEnd; ws += SUMMARY_INTERVAL_MS) {
      const we = ws + SUMMARY_INTERVAL_MS;
      if (we > now) continue; // window not yet complete

      const cleanBeats = await writers.queryCleanBeats(device, ws, we);
      if (cleanBeats.length < 10) continue;

      const rrValues = cleanBeats.map(b => b.rr_clean);
      const hrv = calculateHRV(rrValues);
      if (hrv.rmssd === null) continue;

      const avgHR = Math.round(60000 / (rrValues.reduce((a, b) => a + b, 0) / rrValues.length));

      // Count artifacts in this window from the original processing
      // (we'd need to query artifact_type, but for now just use 0 — can enhance later)
      await writers.writeSummary(device, hrv, avgHR, rrValues.length, 0, null, we);
      summariesWritten++;
    }

    if (summariesWritten > 0) {
      log(`Post-processor: wrote ${summariesWritten} summaries for ${device.slice(0, 11)}`);
    }
  }

  /**
   * Run one processing cycle for all registered devices.
   */
  async function tick() {
    for (const device of deviceState.keys()) {
      try {
        await processDevice(device);
      } catch (err) {
        log(`Post-processor error (${device.slice(0, 11)}): ${err.message}`);
      }
    }
  }

  /**
   * Start the post-processor timer.
   */
  function start() {
    if (timer) return;
    timer = setInterval(tick, PROCESS_INTERVAL_MS);
    log(`Post-processor: started (interval=${PROCESS_INTERVAL_MS / 1000}s, buffer=${PROCESS_BUFFER_MS / 1000}s)`);
  }

  /**
   * Stop the post-processor timer.
   */
  function stop() {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  }

  return { start, stop, registerDevice, triggerReprocess };
}
