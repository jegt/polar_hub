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
const QUALITY_INTERVAL_MS = 60_000;  // write quality metrics per minute
const CONTEXT_BEATS = 91;            // Lipponen context window
const REPROCESS_OVERLAP_MS = 3_000;  // keep small overlap to re-evaluate boundary beats
const REPROCESS_CONTEXT_BEATS = 91;   // rewind by this many beats when possible
const MAX_REPROCESS_BACKTRACK_MS = 10 * 60_000; // cap rewind to 10 minutes
const RR_HARD_MIN_MS = 300;
const RR_HARD_MAX_MS = 2200;
const CORRECTION_TOLERANCE_MS = 0.5;
const WEAK_LONGSHORT_DELTA_MS = 5;
const REALTIME_WINDOW_BEATS = 60;
const REALTIME_MIN_HRV_BEATS = 6;
const MAX_LIPPONEN_PASSES = 20;
const BURST_WARNING_CORRECTION_RATE = 0.20;
const BURST_CRITICAL_CORRECTION_RATE = 0.35;
const BURST_WARNING_STREAK = 4;
const BURST_CRITICAL_STREAK = 8;

/**
 * Create a post-processor instance.
 *
 * @param {Object} writers - InfluxDB writer functions from influx.js
 * @param {Function} log - Logging function
 * @param {Object} options - { summaryIntervalMs }
 * @returns {{ start(), registerDevice(device), triggerReprocess(device, fromMs, streamHints) }}
 */
export function createPostProcessor(writers, log, { summaryIntervalMs = 300_000 } = {}) {
  const SUMMARY_INTERVAL_MS = summaryIntervalMs;
  // Per-device state: last timestamp we've processed up to
  const deviceState = new Map();
  let timer = null;

  function median(values) {
    if (values.length === 0) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0
      ? sorted[mid]
      : (sorted[mid - 1] + sorted[mid]) / 2;
  }

  function streamIdentity(beat) {
    if (beat.recording_id && Number.isFinite(beat.recording_seq)) {
      return { key: `rec:${beat.recording_id}`, seq: beat.recording_seq };
    }
    if (beat.session_id && Number.isFinite(beat.beat_seq)) {
      return { key: `sess:${beat.session_id}`, seq: beat.beat_seq };
    }
    return null;
  }

  function sortBeatsForAnalysis(beats) {
    return [...beats].sort((a, b) => {
      const sa = streamIdentity(a);
      const sb = streamIdentity(b);
      if (sa && sb && sa.key === sb.key && sa.seq !== sb.seq) {
        return sa.seq - sb.seq;
      }
      return a.time - b.time;
    });
  }

  function estimateReplacement(rrValues, idx) {
    const nearby = [];
    const radius = 5;
    for (let i = Math.max(0, idx - radius); i <= Math.min(rrValues.length - 1, idx + radius); i++) {
      if (i === idx) continue;
      const v = rrValues[i];
      if (v >= RR_HARD_MIN_MS && v <= RR_HARD_MAX_MS) nearby.push(v);
    }

    let est = median(nearby);
    if (est == null) {
      const global = rrValues.filter(v => v >= RR_HARD_MIN_MS && v <= RR_HARD_MAX_MS);
      est = median(global);
    }
    if (est == null) return Math.min(Math.max(rrValues[idx], RR_HARD_MIN_MS), RR_HARD_MAX_MS);
    return est;
  }

  function applySafetyRails(rrValues) {
    const safe = [...rrValues];
    const forced = new Map();
    for (let i = 0; i < rrValues.length; i++) {
      const rr = rrValues[i];
      if (rr < RR_HARD_MIN_MS || rr > RR_HARD_MAX_MS) {
        const replacement = estimateReplacement(rrValues, i);
        safe[i] = replacement;
        forced.set(i, replacement);
      }
    }
    return { safe, forced };
  }

  function valuesDiffer(a, b, tolerance = 0.000001) {
    if (a.length !== b.length) return true;
    for (let i = 0; i < a.length; i++) {
      if (Math.abs(a[i] - b[i]) > tolerance) return true;
    }
    return false;
  }

  function runConsistentAnalysis(rrInput) {
    let current = [...rrInput];
    let analysis = analyzeRR(current);

    for (let pass = 0; pass < MAX_LIPPONEN_PASSES; pass++) {
      if (current.length < 4) break;

      const next = analysis.results.map((r, i) =>
        r.rr_clean != null && r.rr_clean > 0 ? r.rr_clean : current[i]
      );

      if (!valuesDiffer(next, current)) break;

      const nextAnalysis = analyzeRR(next);
      if (nextAnalysis.stats.artifacts > analysis.stats.artifacts) break;

      current = next;
      analysis = nextAnalysis;
      if (analysis.stats.artifacts === 0) break;
    }

    const refined = analysis.results.map((r, i) =>
      r.rr_clean != null && r.rr_clean > 0 ? r.rr_clean : current[i]
    );

    return { analysis, refined };
  }

  function computeOutOfOrderCount(beats) {
    const sorted = [...beats].sort((a, b) => a.time - b.time);
    const lastSeqByStream = new Map();
    let count = 0;

    for (const beat of sorted) {
      const stream = streamIdentity(beat);
      if (!stream) continue;

      const prev = lastSeqByStream.get(stream.key);
      if (prev != null && stream.seq <= prev) count++;
      if (prev == null || stream.seq > prev) {
        lastSeqByStream.set(stream.key, stream.seq);
      }
    }
    return count;
  }

  function computeMaxConsecutiveCorrected(beats, isCorrected) {
    let maxRun = 0;
    let run = 0;
    for (const beat of beats) {
      if (isCorrected(beat)) {
        run++;
        if (run > maxRun) maxRun = run;
      } else {
        run = 0;
      }
    }
    return maxRun;
  }

  function classifyCorrectionBurst(correctionRate, maxConsecutiveCorrected) {
    if (correctionRate >= BURST_CRITICAL_CORRECTION_RATE || maxConsecutiveCorrected >= BURST_CRITICAL_STREAK) {
      return 'critical';
    }
    if (correctionRate >= BURST_WARNING_CORRECTION_RATE || maxConsecutiveCorrected >= BURST_WARNING_STREAK) {
      return 'warning';
    }
    return 'none';
  }

  function round2(value) {
    return Math.round(value * 100) / 100;
  }

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

  async function deriveReprocessStartMs(device, fromMs, streamHints = []) {
    const maxBacktrackFloor = Math.max(0, fromMs - MAX_REPROCESS_BACKTRACK_MS);
    let derivedStart = fromMs;

    if (Array.isArray(streamHints) && streamHints.length > 0) {
      for (const hint of streamHints) {
        if (!hint || !hint.id) continue;
        let context = [];
        if (hint.type === 'session') {
          context = await writers.queryBeatsBeforeSession(device, hint.id, fromMs, REPROCESS_CONTEXT_BEATS);
        } else if (hint.type === 'recording') {
          context = await writers.queryBeatsBeforeRecording(device, hint.id, fromMs, REPROCESS_CONTEXT_BEATS);
        }
        if (context.length > 0) {
          derivedStart = Math.min(derivedStart, context[0].time);
        }
      }
    } else {
      const context = await writers.queryBeatsBefore(device, fromMs, REPROCESS_CONTEXT_BEATS);
      if (context.length > 0) {
        derivedStart = Math.min(derivedStart, context[0].time);
      }
    }

    if (derivedStart < maxBacktrackFloor) {
      return maxBacktrackFloor;
    }
    return derivedStart;
  }

  /**
   * Trigger reprocessing from a specific timestamp (e.g. when batch data arrives).
   * Rewinds by beat context where possible, with bounded backtracking.
   */
  async function triggerReprocess(device, fromMs, streamHints = []) {
    const state = deviceState.get(device);
    if (!state) return;

    let reprocessFrom = fromMs;
    try {
      reprocessFrom = await deriveReprocessStartMs(device, fromMs, streamHints);
    } catch (err) {
      log(`Post-processor: context rewind fallback for ${device.slice(0, 11)} (${err.message})`);
    }

    if (reprocessFrom < state.lastProcessedMs) {
      log(`Post-processor: reprocessing ${device} from ${new Date(reprocessFrom).toISOString()}`);
      state.lastProcessedMs = reprocessFrom;
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
    const advanceCursor = () => {
      state.lastProcessedMs = Math.max(targetStart, targetEnd - REPROCESS_OVERLAP_MS);
    };

    // Query: left context + target range + right context
    const [leftContext, targetBeats, rightContext] = await Promise.all([
      writers.queryBeatsBefore(device, targetStart, CONTEXT_BEATS),
      writers.queryRawBeats(device, targetStart, targetEnd),
      writers.queryBeatsAfter(device, targetEnd, CONTEXT_BEATS)
    ]);

    // Filter to beats with rr_interval (skip synthetic inserted beats) and
    // sort by canonical stream order where sequence metadata exists.
    const leftWithRR = sortBeatsForAnalysis(leftContext.filter(b => b.rr_interval != null && b.rr_interval > 0));
    const targetWithRR = sortBeatsForAnalysis(targetBeats.filter(b => b.rr_interval != null && b.rr_interval > 0));
    const rightWithRR = sortBeatsForAnalysis(rightContext.filter(b => b.rr_interval != null && b.rr_interval > 0));
    if (targetWithRR.length === 0) {
      advanceCursor();
      return;
    }

    // Build full array for Lipponen: left context + target + right context
    const leftRR = leftWithRR.map(b => b.rr_interval);
    const targetRR = targetWithRR.map(b => b.rr_interval);
    const rightRR = rightWithRR.map(b => b.rr_interval);

    const fullRR = [...leftRR, ...targetRR, ...rightRR];

    if (fullRR.length < 4) {
      advanceCursor();
      return;
    }

    // Hard safety rails first. Use initial classification for labels and
    // iterative refinement for rr_clean, then enforce no silent corrections.
    const { safe: safeRR, forced: forcedSafetyCorrections } = applySafetyRails(fullRR);
    const initialAnalysis = analyzeRR(safeRR);
    const { refined: refinedRRs } = runConsistentAnalysis(safeRR);

    // Extract results for target range only (skip left/right context)
    const targetStartIdx = leftRR.length;
    const targetEndIdx = targetStartIdx + targetRR.length;

    const cleanPoints = [];
    const insertedPoints = [];
    let artifactCount = 0;

    for (let i = targetStartIdx; i < targetEndIdx; i++) {
      const beat = targetWithRR[i - targetStartIdx];
      const classification = initialAnalysis.results[i];
      let refinedRR = refinedRRs[i];
      const rawRR = beat.rr_interval;
      let artifactType = classification.artifact_type || 'none';
      let weakCorrection = false;

      if (forcedSafetyCorrections.has(i) && artifactType === 'none') {
        artifactType = 'longshort';
      }

      let correctionDelta = Number.isFinite(refinedRR)
        ? Math.round((refinedRR - rawRR) * 1000) / 1000
        : null;

      if (artifactType === 'none' && Math.abs(refinedRR - rawRR) > CORRECTION_TOLERANCE_MS) {
        // No more "silent corrections": ensure non-none label if rr_clean changed materially.
        artifactType = 'longshort';
      }

      // Demote tiny longshort adjustments to non-artifacts to avoid labeling jitter.
      if (
        artifactType === 'longshort' &&
        !forcedSafetyCorrections.has(i) &&
        Number.isFinite(correctionDelta) &&
        Math.abs(correctionDelta) < WEAK_LONGSHORT_DELTA_MS &&
        rawRR >= RR_HARD_MIN_MS &&
        rawRR <= RR_HARD_MAX_MS
      ) {
        artifactType = 'none';
        weakCorrection = true;
        refinedRR = rawRR;
        correctionDelta = 0;
      }

      const correctionApplied = artifactType !== 'none'
        || (Number.isFinite(refinedRR) && Math.abs(refinedRR - rawRR) > CORRECTION_TOLERANCE_MS);

      if (artifactType === 'missed') {
        artifactCount++;
        cleanPoints.push({
          timestamp: beat.time,
          rr_clean: refinedRR,
          hr_clean: Math.round(60000 / refinedRR * 100) / 100,
          artifact_type: 'missed',
          correction_applied: true,
          correction_delta_ms: correctionDelta,
          weak_correction: false
        });
        // Inserted beat: timestamp at original + RR/2 (rounded to integer ms)
        insertedPoints.push({
          timestamp: Math.round(beat.time + refinedRR),
          rr_clean: refinedRR,
          artifact_type: 'missed_inserted'
        });
      } else if (artifactType === 'extra_absorbed') {
        artifactCount++;
        // Absorbed: rr_clean=0 (sentinel), artifact_type marks it
        cleanPoints.push({
          timestamp: beat.time,
          rr_clean: 0,
          hr_clean: 0,
          artifact_type: 'extra_absorbed',
          correction_applied: true,
          correction_delta_ms: Math.round((0 - rawRR) * 1000) / 1000,
          weak_correction: false
        });
      } else if (refinedRR > 0) {
        if (artifactType !== 'none') artifactCount++;
        // Normal, ectopic, extra (merged), longshort — use refined correction
        cleanPoints.push({
          timestamp: beat.time,
          rr_clean: refinedRR,
          hr_clean: Math.round(60000 / refinedRR * 100) / 100,
          artifact_type: artifactType,
          correction_applied: correctionApplied,
          correction_delta_ms: correctionDelta,
          weak_correction: weakCorrection
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
    if (artifactCount > 0 || targetRR.length > 0) {
      log(`Post-processor: ${device.slice(0, 11)} | ${targetRR.length} beats, ${artifactCount} artifacts, ${insertedPoints.length} inserted`);
    }

    // Update lastProcessedMs
    advanceCursor();

    // Recompute per-beat realtime dashboard metrics for affected beats.
    await recomputeRealtime(device, targetWithRR, insertedPoints);
    // Recompute 5-min summaries for affected windows
    await recomputeSummaries(device, targetStart, targetEnd);
    // Recompute 1-min quality metrics over affected windows
    await recomputeQualityMetrics(device, targetStart, targetEnd);
  }

  /**
   * Recompute and upsert polar_realtime points for beats affected by this pass.
   * Uses canonical clean beats (rr_clean) with window spillover so downstream
   * timestamps that still include corrected beats are updated too.
   */
  async function recomputeRealtime(device, targetBeats, insertedPoints) {
    const changedTimestamps = [
      ...targetBeats.map(b => b.time),
      ...insertedPoints.map(p => p.timestamp)
    ].filter(t => Number.isFinite(t));

    if (changedTimestamps.length === 0) return;

    const changedStart = Math.min(...changedTimestamps);
    const changedEnd = Math.max(...changedTimestamps);
    const lookback = REALTIME_WINDOW_BEATS - 1;

    const [before, targetClean, after] = await Promise.all([
      writers.queryCleanBeatsBefore(device, changedStart, lookback),
      writers.queryCanonicalCleanBeats(device, changedStart, changedEnd),
      writers.queryCleanBeatsAfter(device, changedEnd, lookback)
    ]);

    const full = sortBeatsForAnalysis([...before, ...targetClean, ...after]
      .filter(b => b.rr_clean != null && b.rr_clean > 0));
    if (full.length === 0) return;

    const targetKeys = new Set(targetClean.map(b => `${b.time}:${b.session_id || ''}:${b.beat_seq ?? ''}:${b.recording_id || ''}:${b.recording_seq ?? ''}`));
    let firstTargetIdx = full.findIndex(b => targetKeys.has(`${b.time}:${b.session_id || ''}:${b.beat_seq ?? ''}:${b.recording_id || ''}:${b.recording_seq ?? ''}`));
    let lastTargetIdx = -1;
    if (firstTargetIdx >= 0) {
      for (let i = full.length - 1; i >= firstTargetIdx; i--) {
        const key = `${full[i].time}:${full[i].session_id || ''}:${full[i].beat_seq ?? ''}:${full[i].recording_id || ''}:${full[i].recording_seq ?? ''}`;
        if (targetKeys.has(key)) {
          lastTargetIdx = i;
          break;
        }
      }
    }

    // If every changed beat in range was absorbed (rr_clean<=0), anchor rewrite
    // at the first remaining beat at/after changedStart so windows still converge.
    if (firstTargetIdx < 0 || lastTargetIdx < 0) {
      firstTargetIdx = full.findIndex(b => b.time >= changedStart);
      if (firstTargetIdx < 0) return;
      lastTargetIdx = firstTargetIdx;
    }

    const lastOutputIdx = Math.min(full.length - 1, lastTargetIdx + lookback);
    const rewritePoints = [];

    for (let i = firstTargetIdx; i <= lastOutputIdx; i++) {
      const windowStart = Math.max(0, i - lookback);
      const window = full.slice(windowStart, i + 1);
      const rrWindow = window.map(b => b.rr_clean).filter(v => v != null && v > 0);
      if (rrWindow.length === 0) continue;

      const currentRR = rrWindow[rrWindow.length - 1];
      const hrInstant = round2(60000 / currentRR);
      const meanRR = rrWindow.reduce((sum, v) => sum + v, 0) / rrWindow.length;
      const hrTrend = Math.round(60000 / meanRR);

      let hrv = { rmssd: null, sdnn: null, pnn50: null };
      if (rrWindow.length >= REALTIME_MIN_HRV_BEATS) {
        hrv = calculateHRV(rrWindow);
      }

      rewritePoints.push({
        timestamp: full[i].time,
        hrv,
        hr: hrTrend,
        hr_instant: hrInstant
      });
    }

    if (rewritePoints.length === 0) return;

    await writers.writeRealtimeBatch(device, rewritePoints);
    log(`Post-processor: rewrote ${rewritePoints.length} realtime points for ${device.slice(0, 11)}`);
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

      const artifactCount = await writers.queryArtifactCount(device, ws, we);
      await writers.writeSummary(device, hrv, avgHR, rrValues.length, artifactCount, null, we);
      summariesWritten++;
    }

    if (summariesWritten > 0) {
      log(`Post-processor: wrote ${summariesWritten} summaries for ${device.slice(0, 11)}`);
    }
  }

  /**
   * Recompute per-minute quality metrics for windows overlapping the processed range.
   */
  async function recomputeQualityMetrics(device, startMs, endMs) {
    const windowStart = Math.floor(startMs / QUALITY_INTERVAL_MS) * QUALITY_INTERVAL_MS;
    const windowEnd = (Math.floor(endMs / QUALITY_INTERVAL_MS) + 1) * QUALITY_INTERVAL_MS;
    const now = Date.now();
    let written = 0;

    for (let ws = windowStart; ws < windowEnd; ws += QUALITY_INTERVAL_MS) {
      const we = ws + QUALITY_INTERVAL_MS;
      if (we > now) continue;

      const beats = await writers.queryRawBeats(device, ws, we);
      const rawBeats = beats.filter(b => b.rr_interval != null && b.rr_interval > 0);
      if (rawBeats.length === 0) continue;

      const sampleCount = rawBeats.length;
      const orderedBeats = sortBeatsForAnalysis(rawBeats);
      const isStrongCorrected = (beat) => beat.correction_applied === true && beat.weak_correction !== true;
      const artifactCount = rawBeats.filter(b => b.artifact_type && b.artifact_type !== 'none').length;
      const correctedCount = orderedBeats.filter(isStrongCorrected).length;
      const weakCorrectionCount = orderedBeats.filter(b => b.weak_correction === true).length;
      const extremeRRCount = rawBeats.filter(b => b.rr_interval < RR_HARD_MIN_MS || b.rr_interval > RR_HARD_MAX_MS).length;
      const unclassifiedCorrectedCount = rawBeats.filter(b => {
        if (b.artifact_type && b.artifact_type !== 'none') return false;
        if (b.rr_clean == null || b.rr_clean <= 0) return false;
        return Math.abs(b.rr_clean - b.rr_interval) > CORRECTION_TOLERANCE_MS;
      }).length;
      const outOfOrderCount = computeOutOfOrderCount(rawBeats);
      const artifactRate = sampleCount > 0 ? artifactCount / sampleCount : 0;
      const correctionRate = sampleCount > 0 ? correctedCount / sampleCount : 0;
      const maxConsecutiveCorrected = computeMaxConsecutiveCorrected(orderedBeats, isStrongCorrected);
      const correctionBurstLevel = classifyCorrectionBurst(correctionRate, maxConsecutiveCorrected);
      const lowConfidence = correctionBurstLevel !== 'none';

      await writers.writeQuality(device, {
        sample_count: sampleCount,
        artifact_count: artifactCount,
        artifact_rate: artifactRate,
        corrected_count: correctedCount,
        correction_rate: correctionRate,
        weak_correction_count: weakCorrectionCount,
        max_consecutive_corrected: maxConsecutiveCorrected,
        correction_burst_level: correctionBurstLevel,
        low_confidence: lowConfidence,
        unclassified_corrected_count: unclassifiedCorrectedCount,
        out_of_order_count: outOfOrderCount,
        extreme_rr_count: extremeRRCount
      }, we);
      written++;
    }

    if (written > 0) {
      log(`Post-processor: wrote ${written} quality points for ${device.slice(0, 11)}`);
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
