/**
 * HRV calculation utilities
 *
 * Pure math — takes pre-filtered RR intervals, returns metrics.
 * Artifact detection/correction is handled by lipponen.js.
 */

/**
 * Calculate HRV metrics from RR intervals
 * @param {number[]} rrIntervals - Array of RR intervals in ms (should be clean/corrected)
 * @returns {Object} HRV metrics { rmssd, sdnn, pnn50 }
 */
export function calculateHRV(rrIntervals) {
  if (rrIntervals.length < 2) {
    return { rmssd: null, sdnn: null, pnn50: null };
  }

  // Calculate successive differences
  const successiveDiffs = [];
  for (let i = 1; i < rrIntervals.length; i++) {
    successiveDiffs.push(rrIntervals[i] - rrIntervals[i - 1]);
  }

  // RMSSD: Root Mean Square of Successive Differences
  const squaredDiffs = successiveDiffs.map(d => d * d);
  const meanSquaredDiff = squaredDiffs.reduce((a, b) => a + b, 0) / squaredDiffs.length;
  const rmssd = Math.sqrt(meanSquaredDiff);

  // SDNN: Standard Deviation of NN intervals
  const meanRR = rrIntervals.reduce((a, b) => a + b, 0) / rrIntervals.length;
  const squaredDeviations = rrIntervals.map(rr => Math.pow(rr - meanRR, 2));
  const variance = squaredDeviations.reduce((a, b) => a + b, 0) / rrIntervals.length;
  const sdnn = Math.sqrt(variance);

  // pNN50: Percentage of successive differences > 50ms
  const nn50Count = successiveDiffs.filter(d => Math.abs(d) > 50).length;
  const pnn50 = (nn50Count / successiveDiffs.length) * 100;

  return {
    rmssd: Math.round(rmssd * 100) / 100,
    sdnn: Math.round(sdnn * 100) / 100,
    pnn50: Math.round(pnn50 * 100) / 100
  };
}
