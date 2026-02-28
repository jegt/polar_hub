# RR Interval Artifact Filtering — Reference

## Background

ECG chest straps (Polar H10 and similar) produce RR interval artifacts at ~0.5-2% rate.
This is normal — published studies report 0.56-1% for H10 at rest/moderate activity.
The 5% threshold is where researchers consider recordings unreliable.

Artifacts are devastating for HRV metrics: ~1% artifact rate can spike RMSSD/SDNN by 10×,
making time-domain HRV graphs unusable.

## Artifact Types

| Type | Cause | RR Pattern | dRR Pattern |
|------|-------|------------|-------------|
| Missed beat | R-peak not detected (dry electrodes, movement, poor contact) | One long interval ≈ 2× normal | PN (positive then negative) |
| Extra beat | Spurious detection (T-wave misread as R) | One short interval ≈ 0.5× normal | NP (negative then positive) |
| Ectopic beat | Premature ventricular/atrial contraction (physiological) | Short then long (compensatory pause) | NPN or PNP |
| Alternating | Missed+double beat pairs | High/low/high/low: 2397→441→2606→458 | Oscillating |

Key limitation of RR-only data (no raw ECG): cannot distinguish ectopic beats (real cardiac
events) from sensor noise. Both look identical in RR intervals.

## Simple Algorithms (and Why They Fail)

### Fixed Range Filter (300-2000ms)
Reject RR outside physiological bounds.
- Catches extreme artifacts (10,450ms, 261ms)
- Misses moderate artifacts (1,600-2,000ms at resting HR where normal RR is ~1,000ms)
- No false positives, but low sensitivity

### Kamath & Fallen (1995) — Asymmetric Beat-to-Beat
Reject if RR increases >32.5% or decreases >24.5% from previous beat.
- Asymmetry accounts for ectopic short-then-long pattern
- **Fails**: rejects first beat of any rapid real HR change (dive reflex, exercise onset)
- Beat-to-beat only — no context

### Malik — Symmetric Beat-to-Beat (±20%)
Simpler version of Kamath.
- Even more aggressive false positives on real HR changes
- Widely used in consumer apps despite known limitations

### Rolling Window Average ± Threshold
Compare each RR to mean/median of last N beats. Reject if deviation > X%.
- ±30% tested on our data: rejects entire cold plunge dive response (128→65 BPM in 10s)
- First beat of dive response is exactly 30.2% — right at threshold boundary
- The window "freezes" once first beat is rejected, cascading rejection of all subsequent beats

### HR-Ratio Method
Compare `rr / (60000/hr)` — uses H10's device-computed HR as reference.
- Clean separation for normal beats (p99 = 1.18) vs extreme artifacts (>1.5×)
- **Fails at 1.3× threshold**: H10's HR is a rolling average that lags behind actual RR
  during rapid transitions. Dive response beats show ratio 1.3-1.4× for several beats
  as HR catches up — indistinguishable from moderate artifacts
- Only works for realtime path (H10 provides HR). Batch path computes HR from the
  artifact-polluted RR values themselves

### Cascade Breaker (Current Server Implementation)
Track consecutive rejected-but-consistent beats. Accept after N consistent rejects as
"baseline shift."
- Accepts clusters of ~2,000ms artifacts as real (they're consistent with each other)
- Poisons the HRV buffer with artifact values

## Gold Standard: Lipponen & Tarvainen (2019) — dRR Pattern Detection

The algorithm used by **Kubios** (the reference HRV analysis tool). Implemented in
**NeuroKit2** (`signal_fixpeaks.py`) and **systole** (Python).

### Core Insight

Don't filter on RR magnitude. Compute the **dRR series** (successive differences between
consecutive RR intervals) and classify artifacts by their *shape*.

```
dRR[i] = RR[i] - RR[i-1]
```

### Why Shape Works

**Real rapid HR change** (dive reflex, 128→65 BPM):
```
RR:  468, 608, 686, 834, 925, 944, 929, 897, 879
dRR:    +140, +78, +148, +91, +19, -15, -32, -18
```
dRR is a series of moderate positive values (monotonic increase). No sharp reversal.

**Ectopic beat** at 100 BPM (600ms normal RR):
```
RR:  605, 612, 380, 850, 598, 610
dRR:     +7, -232, +470, -252, +12
```
dRR shows NPN pattern: large negative, large positive, return to small. Sharp reversal.

**Missed beat** at 100 BPM:
```
RR:  605, 612, 1210, 598, 610
dRR:     +7, +598, -612, +12
```
dRR shows PN pattern: large positive, large negative. The long interval is ≈ 2× median.

### Algorithm Details

**Adaptive threshold**:
```
Th = 5.2 × QD(dRR over ~90 surrounding beats)
```
Where QD = quartile deviation = (Q3 - Q1) / 2. Factor 5.2 covers 99.95% of normal beats
assuming Gaussian distribution. Threshold is time-varying — recalculated locally.

**Pattern classification**:

1. Compute normalized dRR: `drrs[i] = dRR[i] / Th`
2. Compute deviation from local median: `mrrs[i] = (RR[i] - medRR) / Th2`
3. If `|drrs[i]| <= 1`: normal beat, skip
4. Check ectopic: `drrs[i] > 1 AND s12[i] < -c1*drrs[i] - c2` (linear boundary in dRR subspace)
5. Check missed beat: `|RR[i]/2 - medRR| < 2×Th` (long interval ≈ 2× normal)
6. Check extra beat: `|RR[i] + RR[i+1] - medRR| < 2×Th` (two short = one normal)

Constants: `c1=0.13`, `c2=0.17`, median window=11 beats, threshold window=91 beats.

**Performance**: 96.96% sensitivity, 99.94% specificity.

### Limitations

- Requires look-ahead (uses neighbors on both sides) — not directly causal/real-time
- Needs ~90-beat window for adaptive threshold (60-90 seconds at rest)
- Designed for post-hoc analysis of complete recordings

## Real-Time Approaches

### Windowed Batch (HeartMath Approach)

The dominant approach in HRV biofeedback apps.

1. Buffer 30-64 seconds of incoming beats
2. Run artifact rejection on the full window (look-ahead free within window)
3. Compute HRV metrics (RMSSD, coherence) on clean beats in window
4. Slide window forward every 5 seconds, recompute
5. User sees "real-time" updates with ~5-10 second effective latency

HeartMath specifically: 64-second window, 5-second update interval, FFT-based coherence.

**Advantages**:
- Can use Lipponen-style pattern detection within each window
- Smooths out single-beat noise in HRV output
- Well-proven in commercial products

**Disadvantages**:
- HRV metrics lag real-time by half the window length
- Window must be long enough for meaningful HRV (≥20 beats minimum)

### Bounded Lookahead (Barbieri & Brown 2006 — Point Process)

Most theoretically rigorous real-time method.

- Models each RR as time-varying inverse Gaussian distribution
- 60-second sliding window with exponential weighting for parameter estimation
- Evaluates 5 competing hypotheses per beat: Normal, Extra, Missed, Misplaced, Ectopic
- Defers classification by 1-3 beats (Q=3 future beats for confirmation)
- Bootstrap phase (first 60s): 7×MAD from median as fallback rule

**Advantages**:
- Explicitly handles "is this better explained as artifact or real beat?"
- Real physiological changes fit the normal hypothesis (model adapts)
- Bounded delay (max 3 beats)

**Disadvantages**:
- Computationally heavier
- 60-second initialization period
- Complex to implement

### Trailing Median + Threshold (Simplest)

Used by most consumer apps.

```
local_median = median(last 8-20 clean beats)
if abs(rr - local_median) / local_median > threshold:
    reject
```

**Advantages**: Trivial to implement, truly per-beat, no lookahead.
**Disadvantages**: Same failure mode as rolling window — rejects rapid real HR changes.

## Correction Strategies

| Strategy | Description | When to Use |
|----------|-------------|-------------|
| Discard | Remove artifact, compute metrics on remaining clean beats | RMSSD/SDNN (don't need even spacing) |
| Interpolate | Replace artifact with estimated value (linear, cubic spline) | Frequency-domain (FFT needs even spacing) |
| Replace | Substitute with local median | Simple, preserves beat count |

For time-domain HRV (RMSSD, SDNN, pNN50), discarding is simplest and doesn't introduce
synthetic data. RMSSD is a mean of squared successive differences — gaps don't break it.

## Key Sources

- Lipponen & Tarvainen (2019) — "A robust algorithm for HRV time series artifact correction" (PubMed 31314618)
- Barbieri & Brown (2006) — Real-time point process method (PMC3523127)
- Kamath & Fallen (1995) — Asymmetric threshold (referenced in PMC3232430)
- ESC/NASPE Task Force (1996) — HRV standards (PubMed 8598068)
- Polar H10 validity: PMC9459793, PubMed 31004219
- NeuroKit2 implementation: github.com/neuropsychology/NeuroKit — `signal_fixpeaks.py`
- Kubios preprocessing: kubios.com/blog/preprocessing-of-hrv-data/
- HeartMath coherence: PMC9214473
