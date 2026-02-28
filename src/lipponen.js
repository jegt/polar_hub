/**
 * Lipponen & Tarvainen (2019) RR interval artifact detection and correction
 *
 * Classifies artifacts by dRR shape patterns (not magnitude thresholds),
 * then applies type-specific corrections.
 *
 * Reference: PubMed 31314618
 * Based on: NeuroKit2 signal_fixpeaks.py
 */

// Algorithm constants from the paper
const THRESHOLD_FACTOR = 5.2; // α for adaptive threshold (covers 99.95% of normal beats)
const QD_WINDOW = 91;         // window size for quartile deviation
const MEDIAN_WINDOW = 11;     // window size for local median RR
const C1 = 0.13;              // ectopic boundary slope
const C2 = 0.17;              // ectopic boundary intercept
const MIN_THRESHOLD = 50;     // floor when QD ≈ 0 (ms)

// --- Helper functions ---

function median(arr) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

function quantile(arr, q) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (base + 1 < sorted.length) {
    return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
  }
  return sorted[base];
}

function quartileDeviation(arr) {
  return (quantile(arr, 0.75) - quantile(arr, 0.25)) / 2;
}

/**
 * Apply a centered rolling window function to an array.
 * At edges, the window shrinks to whatever is available.
 */
function rollingCentered(arr, windowSize, fn) {
  const half = Math.floor(windowSize / 2);
  return arr.map((_, i) => {
    const start = Math.max(0, i - half);
    const end = Math.min(arr.length, i + half + 1);
    return fn(arr.slice(start, end));
  });
}

/**
 * Analyze RR intervals using Lipponen & Tarvainen (2019) algorithm.
 *
 * @param {number[]} rrIntervals - Sequential RR intervals in ms
 * @returns {{
 *   results: Array<{rr_clean: number|null, artifact_type: string}>,
 *   cleanSeries: number[],
 *   stats: {total: number, artifacts: number, ectopic: number, missed: number, extra: number, longshort: number}
 * }}
 *
 * results: one entry per input beat (same length, 1:1 index correspondence)
 *   - rr_clean: corrected value, or null for absorbed extra beats
 *   - artifact_type: 'none', 'ectopic', 'missed', 'extra', 'extra_absorbed', 'longshort'
 *
 * cleanSeries: corrected RR values suitable for HRV calculation.
 *   May differ in length from input (missed beats are split, extra beats are merged).
 */
export function analyzeRR(rrIntervals) {
  const n = rrIntervals.length;

  // Need at least 4 beats for meaningful dRR analysis
  if (n < 4) {
    return {
      results: rrIntervals.map(rr => ({ rr_clean: rr, artifact_type: 'none' })),
      cleanSeries: [...rrIntervals],
      stats: { total: n, artifacts: 0, ectopic: 0, missed: 0, extra: 0, longshort: 0 }
    };
  }

  const rr = [...rrIntervals];

  // --- 1. Compute dRR series (successive differences) ---
  const dRR = new Array(n);
  for (let i = 1; i < n; i++) {
    dRR[i] = rr[i] - rr[i - 1];
  }
  // First element: mean of the rest (NeuroKit2 convention — avoids edge bias)
  let drrSum = 0;
  for (let i = 1; i < n; i++) drrSum += dRR[i];
  dRR[0] = drrSum / (n - 1);

  // --- 2. Adaptive threshold on dRR ---
  const th1 = rollingCentered(dRR, QD_WINDOW, window => {
    const qd = quartileDeviation(window);
    return Math.max(THRESHOLD_FACTOR * qd, MIN_THRESHOLD);
  });

  // --- 3. Local median RR ---
  const medrr = rollingCentered(rr, MEDIAN_WINDOW, median);

  // --- 4. mRR = RR - medrr, negative values doubled ---
  const mRR = rr.map((r, i) => {
    let m = r - medrr[i];
    if (m < 0) m *= 2;
    return m;
  });

  // --- 5. Adaptive threshold on mRR ---
  const th2 = rollingCentered(mRR, QD_WINDOW, window => {
    const qd = quartileDeviation(window);
    return Math.max(THRESHOLD_FACTOR * qd, MIN_THRESHOLD);
  });

  // --- 6. Normalize ---
  const drrs = dRR.map((d, i) => d / th1[i]);
  const mrrs = mRR.map((m, i) => m / th2[i]);

  // --- 7. Subspace projections ---
  // s12: max/min of immediate neighbors (detects sharp reversal → ectopic)
  const s12 = new Array(n).fill(0);
  for (let i = 1; i < n - 1; i++) {
    if (drrs[i] > 0) {
      s12[i] = Math.max(drrs[i - 1], drrs[i + 1]);
    } else if (drrs[i] < 0) {
      s12[i] = Math.min(drrs[i - 1], drrs[i + 1]);
    }
  }

  // s22: min/max of look-ahead neighbors (detects extra/missed patterns)
  const s22 = new Array(n).fill(0);
  for (let i = 0; i < n - 2; i++) {
    if (drrs[i] >= 0) {
      s22[i] = Math.min(drrs[i + 1], drrs[i + 2]);
    } else {
      s22[i] = Math.max(drrs[i + 1], drrs[i + 2]);
    }
  }

  // --- 8. Classification ---
  // Track ectopic pairs: when drrs[i] flags ectopic, the pair is (i-1, i)
  // because dRR[i] = rr[i] - rr[i-1], so both intervals flank the misplaced peak.
  const classifications = new Array(n).fill('none');
  const ectopicPairs = []; // [{a, b}] index pairs to average

  let i = 0;
  while (i < n - 2) {
    if (Math.abs(drrs[i]) <= 1) {
      i++;
      continue;
    }

    // Ectopic test: sharp reversal in s12 subspace
    const isEctopic =
      (drrs[i] > 1 && s12[i] < -C1 * drrs[i] - C2) ||
      (drrs[i] < -1 && s12[i] > -C1 * drrs[i] + C2);

    if (isEctopic) {
      // dRR[i] is large → the pair is rr[i-1] and rr[i]
      if (i > 0) {
        ectopicPairs.push({ a: i - 1, b: i });
      } else {
        // Edge case: ectopic at first beat, correct with median
        classifications[i] = 'longshort';
      }
      i += 2; // skip — the pattern spans two dRR transitions
      continue;
    }

    // Long/short candidate: large dRR or large deviation from median
    if (Math.abs(drrs[i]) > 1 || Math.abs(mrrs[i]) > 3) {
      // Build candidate list: always test i, also test i+1 if it's "calmer"
      const candidates = [i];
      if (i + 2 < n && Math.abs(drrs[i + 1]) < Math.abs(drrs[i + 2])) {
        candidates.push(i + 1);
      }

      let found = false;
      for (const j of candidates) {
        // Extra beat test: short beat, two shorts sum to one normal
        if (j < n - 1 && drrs[j] < -1 && s22[j] > 1) {
          if (Math.abs(rr[j] + rr[j + 1] - medrr[j]) < th2[j]) {
            classifications[j] = 'extra';
            classifications[j + 1] = 'extra_absorbed';
            i = j + 2;
            found = true;
            break;
          }
        }

        // Missed beat test: long beat ≈ 2× median
        if (drrs[j] > 1 && s22[j] < -1) {
          if (Math.abs(rr[j] / 2 - medrr[j]) < th2[j]) {
            classifications[j] = 'missed';
            i = j + 2;
            found = true;
            break;
          }
        }
      }

      if (!found) {
        classifications[i] = 'longshort';
        i++;
      }
      continue;
    }

    i++;
  }

  // --- 9. Correction ---
  const results = rr.map((r, idx) => ({
    rr_clean: r,
    artifact_type: classifications[idx]
  }));

  // Apply non-ectopic corrections first
  for (let idx = 0; idx < n; idx++) {
    switch (classifications[idx]) {
      case 'missed':
        results[idx].rr_clean = rr[idx] / 2;
        break;
      case 'extra':
        if (idx + 1 < n) {
          results[idx].rr_clean = rr[idx] + rr[idx + 1];
        }
        break;
      case 'extra_absorbed':
        results[idx].rr_clean = null;
        break;
      case 'longshort':
        results[idx].rr_clean = medrr[idx];
        break;
    }
  }

  // Apply ectopic corrections last (overrides any longshort at the paired index)
  for (const { a, b } of ectopicPairs) {
    const avg = (rr[a] + rr[b]) / 2;
    results[a].rr_clean = avg;
    results[a].artifact_type = 'ectopic';
    results[b].rr_clean = avg;
    results[b].artifact_type = 'ectopic';
  }

  // --- 10. Build clean series for HRV calculation ---
  const cleanSeries = [];
  for (let idx = 0; idx < n; idx++) {
    const r = results[idx];
    if (r.rr_clean === null) continue; // skip absorbed beats
    if (r.artifact_type === 'missed') {
      // Split: two equal beats
      cleanSeries.push(r.rr_clean);
      cleanSeries.push(r.rr_clean);
    } else {
      cleanSeries.push(r.rr_clean);
    }
  }

  // --- Stats (from final results, after ectopic overrides) ---
  const stats = { total: n, artifacts: 0, ectopic: 0, missed: 0, extra: 0, longshort: 0 };
  for (const r of results) {
    if (r.artifact_type === 'ectopic') { stats.ectopic++; stats.artifacts++; }
    else if (r.artifact_type === 'missed') { stats.missed++; stats.artifacts++; }
    else if (r.artifact_type === 'extra') { stats.extra++; stats.artifacts++; }
    else if (r.artifact_type === 'longshort') { stats.longshort++; stats.artifacts++; }
  }

  return { results, cleanSeries, stats };
}
