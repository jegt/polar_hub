/**
 * HRV calculation utilities
 */

// Default artifact filtering thresholds
export const DEFAULT_RR_MIN_MS = 300;   // 200 bpm max
export const DEFAULT_RR_MAX_MS = 2000;  // 30 bpm min
export const DEFAULT_RR_MAX_DIFF_MS = 200;
export const DEFAULT_RR_MAX_DIFF_PCT = 0.20;

/**
 * Calculate HRV metrics from RR intervals
 * @param {number[]} rrIntervals - Array of RR intervals in ms
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

/**
 * Create an RR interval validator with configurable thresholds
 * @param {Object} options - Validation options
 * @returns {Function} Validator function (rr, lastValidRR, beatCount) => boolean
 */
export function createRRValidator(options = {}) {
  const {
    minMs = DEFAULT_RR_MIN_MS,
    maxMs = DEFAULT_RR_MAX_MS,
    maxDiffMs = DEFAULT_RR_MAX_DIFF_MS,
    maxDiffPct = DEFAULT_RR_MAX_DIFF_PCT,
    warmupBeats = 0,
    cascadeResetCount = 3
  } = options;

  // Track consecutive rejected-but-in-range beats for cascade detection
  let rejectedRun = [];

  return function isValidRR(rr, lastValidRR, beatCount) {
    // Basic range check
    if (rr < minMs || rr > maxMs) {
      rejectedRun = [];
      return false;
    }

    // Skip warmup beats at session start
    if (beatCount <= warmupBeats) {
      return false;
    }

    // Successive difference check (if we have a previous valid RR)
    if (lastValidRR !== null) {
      const diff = Math.abs(rr - lastValidRR);
      const pctDiff = diff / lastValidRR;
      if (diff > maxDiffMs || pctDiff > maxDiffPct) {
        // Failed diff check — track for cascade detection
        if (rejectedRun.length === 0) {
          rejectedRun.push(rr);
        } else {
          // Check if this beat is consistent with the other rejected beats
          const prevRejected = rejectedRun[rejectedRun.length - 1];
          const rejDiff = Math.abs(rr - prevRejected);
          const rejPct = rejDiff / prevRejected;
          if (rejDiff <= maxDiffMs && rejPct <= maxDiffPct) {
            rejectedRun.push(rr);
          } else {
            rejectedRun = [rr];
          }
        }

        // Cascade breaker: N consecutive rejected beats agree with each other
        if (rejectedRun.length >= cascadeResetCount) {
          rejectedRun = [];
          return true;  // accept — baseline has shifted
        }

        return false;
      }
    }

    rejectedRun = [];
    return true;
  };
}

/**
 * Create a rolling buffer for RR intervals with timestamp support
 * Stores {timestamp, rr} pairs sorted by timestamp
 * @param {number} maxSize - Maximum buffer size
 * @returns {Object} Buffer with push(), getAll(), getPreviousByTimestamp() methods
 */
export function createRRBuffer(maxSize = 30) {
  const buffer = [];
  
  return {
    /**
     * Insert RR interval at correct position by timestamp
     */
    push(rr, timestamp) {
      const entry = { timestamp, rr };
      
      // Find insertion point (binary search for efficiency)
      let left = 0;
      let right = buffer.length;
      while (left < right) {
        const mid = (left + right) >> 1;
        if (buffer[mid].timestamp < timestamp) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      
      buffer.splice(left, 0, entry);
      
      // Remove oldest if over max size
      if (buffer.length > maxSize) {
        buffer.shift();
      }
    },
    
    /**
     * Get all RR values in timestamp order
     */
    getAll() {
      return buffer.map(e => e.rr);
    },
    
    /**
     * Get the RR interval that came immediately before the given timestamp
     */
    getPreviousByTimestamp(timestamp) {
      for (let i = buffer.length - 1; i >= 0; i--) {
        if (buffer[i].timestamp < timestamp) {
          return buffer[i].rr;
        }
      }
      return null;
    },
    
    get length() {
      return buffer.length;
    },
    
    clear() {
      buffer.length = 0;
    }
  };
}
