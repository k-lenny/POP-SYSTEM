function formatTime(t) {
  if (t == null) return null;
  try {
    const ms = typeof t === 'number' && t < 1e12 ? t * 1000 : t;
    return new Date(ms).toISOString();
  } catch (e) { return null; }
}

function resolveFormattedTime(candle) {
  return candle.formattedTime || formatTime(candle.time);
}

function candleRef(candle, fallbackIndex) {
  return {
    index:         candle.index ?? fallbackIndex,
    time:          candle.time,
    formattedTime: resolveFormattedTime(candle),
    price:         candle.close,
    open:          candle.open,
    high:          candle.high,
    low:           candle.low,
    close:         candle.close,
  };
}

function extremaIn(candles, startIdx, endIdx) {
  let hiIdx = startIdx, loIdx = startIdx;
  for (let k = startIdx + 1; k <= endIdx; k++) {
    if (candles[k].high > candles[hiIdx].high) hiIdx = k;
    if (candles[k].low  < candles[loIdx].low)  loIdx = k;
  }
  const hi = candles[hiIdx];
  const lo = candles[loIdx];
  return {
    highest: {
      index:         hi.index ?? hiIdx,
      price:         hi.high,
      formattedTime: resolveFormattedTime(hi),
    },
    lowest: {
      index:         lo.index ?? loIdx,
      price:         lo.low,
      formattedTime: resolveFormattedTime(lo),
    },
  };
}

function buildZone(candles, startIdx, endIdx, scenario) {
  const { highest, lowest } = extremaIn(candles, startIdx, endIdx);
  return {
    scenario,
    candleCount: endIdx - startIdx + 1,
    start:       candleRef(candles[startIdx], startIdx),
    end:         candleRef(candles[endIdx],   endIdx),
    highest,
    lowest,
  };
}

// Helper: every adjacent pair in the window shares price overlap (no gaps).
function allAdjacentOverlap(window) {
  for (let k = 0; k < window.length - 1; k++) {
    const x = window[k], y = window[k + 1];
    if (Math.min(x.high, y.high) < Math.max(x.low, y.low)) return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Scenario 1 — 5-candle stair-step recovery (A,B,C,D,E)
//
// Sample window:
//   A: O 13149.60  H 13149.75  L 13139.61  C 13140.10
//   B: O 13139.80  H 13159.83  L 13139.80  C 13147.09
//   C: O 13147.60  H 13156.79  L 13138.76  C 13149.06
//   D: O 13148.92  H 13157.71  L 13146.01  C 13156.08
//   E: O 13156.35  H 13161.35  L 13146.77  C 13160.86
//
// Rules that held across the window:
//   1) B is a local-high pivot on the highs:  H(B) > H(A) AND H(B) > H(C)
//   2) C is a local-low pivot on the lows:    L(C) < L(B) AND L(C) < L(D)
//   3) Directional recovery after the dip:    C(D) > C(C) AND C(E) > C(D)
//   4) Opens step upward from C onward:       O(C) < O(D) < O(E)
//   5) Adjacent candles overlap (no gaps)
// ---------------------------------------------------------------------------
function matchesScenario1(candles, i) {
  const A = candles[i];
  const B = candles[i + 1];
  const C = candles[i + 2];
  const D = candles[i + 3];
  const E = candles[i + 4];

  const bHighPivot    = B.high  > A.high  && B.high  > C.high;
  const cLowPivot     = C.low   < B.low   && C.low   < D.low;
  const closesRecover = D.close > C.close && E.close > D.close;
  const opensStepUp   = C.open  < D.open  && D.open  < E.open;
  const overlapping   = allAdjacentOverlap([A, B, C, D, E]);

  return bHighPivot && cLowPivot && closesRecover && opensStepUp && overlapping;
}

// ---------------------------------------------------------------------------
// Scenario 2 — 3-candle compression with close-pivot recovery (A,B,C)
//
// Sample window:
//   A: O 13159.9340  H 13166.4630  L 13150.6460  C 13153.5560
//   B: O 13153.3700  H 13162.8240  L 13149.4650  C 13161.0440
//   C: O 13161.0050  H 13161.1160  L 13144.1880  C 13157.4910
//
// Rules that held across the window:
//   1) Descending highs (compression from the top):
//        H(A) > H(B) > H(C)
//   2) Descending lows (probing lower):
//        L(A) > L(B) > L(C)
//   3) Alternating candle directions (bearish, bullish, bearish):
//        C(A) < O(A)  AND  C(B) > O(B)  AND  C(C) < O(C)
//   4) B is the close-pivot high:
//        C(B) > C(A)  AND  C(B) > C(C)
//   5) Net upward drift — C still closes above A's close:
//        C(C) > C(A)
//   6) V-shape in opens around B:
//        O(B) < O(A)  AND  O(C) > O(B)
//   7) Adjacent candles overlap (no gaps)
// ---------------------------------------------------------------------------
function matchesScenario2(candles, i) {
  const A = candles[i];
  const B = candles[i + 1];
  const C = candles[i + 2];

  const descendingHighs  = A.high > B.high && B.high > C.high;
  const descendingLows   = A.low  > B.low  && B.low  > C.low;
  const alternatingDirs  = A.close < A.open && B.close > B.open && C.close < C.open;
  const bClosePivot      = B.close > A.close && B.close > C.close;
  const netUpwardDrift   = C.close > A.close;
  const vShapeOpens      = B.open  < A.open  && C.open  > B.open;
  const overlapping      = allAdjacentOverlap([A, B, C]);

  return descendingHighs
      && descendingLows
      && alternatingDirs
      && bClosePivot
      && netUpwardDrift
      && vShapeOpens
      && overlapping;
}

// ---------------------------------------------------------------------------
// Scenario 3 — 3-candle failed-push distribution (A,B,C)
//
// Sample window:
//   A: O 13157.7190  H 13169.3790  L 13156.4460  C 13167.8640
//   B: O 13168.3180  H 13173.5380  L 13156.8490  C 13165.9440
//   C: O 13166.2230  H 13168.8710  L 13155.3590  C 13161.1900
//
// This is the mirror of Scenario 2: B pushes the high but fails, and
// closes fade down through the window (distribution/fade).
//
// Rules that held across the window:
//   1) B is the high-pivot on the highs:
//        H(B) > H(A)  AND  H(B) > H(C)
//   2) C is the low-pivot on the lows:
//        L(C) < L(A)  AND  L(C) < L(B)
//   3) Closes descend monotonically (sustained selling pressure):
//        C(A) > C(B) > C(C)
//   4) B opens the highest — gap-up attempt that fails:
//        O(B) > O(A)  AND  O(B) > O(C)
//   5) Directions are bullish, bearish, bearish:
//        C(A) > O(A)  AND  C(B) < O(B)  AND  C(C) < O(C)
//   6) Adjacent candles overlap (no gaps)
// ---------------------------------------------------------------------------
function matchesScenario3(candles, i) {
  const A = candles[i];
  const B = candles[i + 1];
  const C = candles[i + 2];

  const bHighPivot      = B.high  > A.high  && B.high  > C.high;
  const cLowPivot       = C.low   < A.low   && C.low   < B.low;
  const closesDescend   = A.close > B.close && B.close > C.close;
  const bOpenHighest    = B.open  > A.open  && B.open  > C.open;
  const dirsBullBearBear =
        A.close > A.open &&
        B.close < B.open &&
        C.close < C.open;
  const overlapping     = allAdjacentOverlap([A, B, C]);

  return bHighPivot
      && cLowPivot
      && closesDescend
      && bOpenHighest
      && dirsBullBearBear
      && overlapping;
}

// ---------------------------------------------------------------------------
// Scenario 4 — 4-candle capped V-recovery (A,B,C,D)
//
// Sample window:
//   A: O 13152.8460  H 13154.3820  L 13124.9510  C 13125.8950
//   B: O 13125.4540  H 13134.9330  L 13113.8000  C 13130.3620
//   C: O 13130.7040  H 13133.8440  L 13120.6200  C 13120.7010
//   D: O 13120.6280  H 13146.5390  L 13120.3720  C 13142.3790
//
// The shape: A is a big sell bar that prints the window ceiling. B probes
// the absolute low then pulls back up. C fades again but holds above B's
// low. D is the recovery bar — it pushes back above B and C but stays
// capped under A's high, and closes as the strongest close in the window.
//
// Rules that held across the window:
//   1) A is the window-high on highs:
//        H(A) > H(B)  AND  H(A) > H(C)  AND  H(A) > H(D)
//   2) Highs step down A -> B -> C:
//        H(A) > H(B) > H(C)
//   3) D recovers above B and C but stays capped under A:
//        H(D) > H(B)  AND  H(D) > H(C)  AND  H(D) < H(A)
//   4) B is the window-low on lows:
//        L(B) < L(A)  AND  L(B) < L(C)  AND  L(B) < L(D)
//   5) Alternating directions bear / bull / bear / bull:
//        C(A) < O(A)  AND  C(B) > O(B)  AND  C(C) < O(C)  AND  C(D) > O(D)
//   6) Zigzag closes with C as trough, D as peak:
//        C(A) < C(B)  AND  C(B) > C(C)  AND  C(C) < C(D)
//        and C(D) is the highest close, C(C) is the lowest close.
//   7) Opens slide: A highest open, D lowest open:
//        O(A) > O(B)  AND  O(D) < O(C)
//   8) Net recovery — D closes above A:
//        C(D) > C(A)
//   9) Adjacent candles overlap (no gaps)
// ---------------------------------------------------------------------------
function matchesScenario4(candles, i) {
  const A = candles[i];
  const B = candles[i + 1];
  const C = candles[i + 2];
  const D = candles[i + 3];

  const aWindowHigh     = A.high > B.high && A.high > C.high && A.high > D.high;
  const highsStepDown   = A.high > B.high && B.high > C.high;
  const dRecoversCapped = D.high > B.high && D.high > C.high && D.high < A.high;

  const bWindowLow      = B.low < A.low && B.low < C.low && B.low < D.low;

  const dirsAlternate =
        A.close < A.open &&
        B.close > B.open &&
        C.close < C.open &&
        D.close > D.open;

  const zigzagCloses  = A.close < B.close && B.close > C.close && C.close < D.close;
  const dClosePeak    = D.close > A.close && D.close > B.close && D.close > C.close;
  const cCloseTrough  = C.close < A.close && C.close < B.close && C.close < D.close;

  const opensSlide    = A.open > B.open && D.open < C.open;
  const netRecovery   = D.close > A.close;
  const overlapping   = allAdjacentOverlap([A, B, C, D]);

  return aWindowHigh
      && highsStepDown
      && dRecoversCapped
      && bWindowLow
      && dirsAlternate
      && zigzagCloses
      && dClosePeak
      && cCloseTrough
      && opensSlide
      && netRecovery
      && overlapping;
}

// Slides all windows across the array and reports every match.
// Returns zones tagged with which scenario fired.
function findConsolidations(candles) {
  if (!Array.isArray(candles) || candles.length < 3) return [];

  const zones = [];

  // Scenario 1: 5-candle window
  if (candles.length >= 5) {
    for (let i = 0; i + 4 < candles.length; i++) {
      if (matchesScenario1(candles, i)) {
        zones.push(buildZone(candles, i, i + 4, 'scenario1_5candle_stairstep'));
      }
    }
  }

  // Scenario 2: 3-candle window (compression recovery)
  for (let i = 0; i + 2 < candles.length; i++) {
    if (matchesScenario2(candles, i)) {
      zones.push(buildZone(candles, i, i + 2, 'scenario2_3candle_compression'));
    }
  }

  // Scenario 3: 3-candle window (failed-push distribution)
  for (let i = 0; i + 2 < candles.length; i++) {
    if (matchesScenario3(candles, i)) {
      zones.push(buildZone(candles, i, i + 2, 'scenario3_3candle_failed_push'));
    }
  }

  // Scenario 4: 4-candle window (capped V-recovery)
  if (candles.length >= 4) {
    for (let i = 0; i + 3 < candles.length; i++) {
      if (matchesScenario4(candles, i)) {
        zones.push(buildZone(candles, i, i + 3, 'scenario4_4candle_capped_v_recovery'));
      }
    }
  }

  // Scenario 5: 4-candle window (rising-wedge top with D-failure)
  if (candles.length >= 4) {
    for (let i = 0; i + 3 < candles.length; i++) {
      if (matchesScenario5(candles, i)) {
        zones.push(buildZone(candles, i, i + 3, 'scenario5_4candle_rising_wedge_failure'));
      }
    }
  }

  // Scenario 6: anchor-range containment (body-contained, wick-tolerant)
  // Every candle is tested as a potential anchor. Each valid zone is
  // reported independently, so overlapping zones from adjacent anchors
  // are expected and kept.
  if (candles.length >= 4) {
    for (let i = 0; i < candles.length; i++) {
      const endIdx = findAnchorRangeEnd(candles, i);
      if (endIdx !== null) {
        zones.push(buildZone(candles, i, endIdx, 'scenario6_anchor_range_containment'));
      }
    }
  }

  // Scenario 7: anchor-range with straddle-break on the 3rd following candle
  // (body breaks one side by a little, opposite wick probes the other side).
  if (candles.length >= 4) {
    for (let i = 0; i + 3 < candles.length; i++) {
      if (matchesScenario7(candles, i)) {
        zones.push(buildZone(candles, i, i + 3, 'scenario7_anchor_straddle_break'));
      }
    }
  }

  return zones;
}

// ---------------------------------------------------------------------------
// Scenario 5 — 4-candle rising-wedge top with D-failure (A,B,C,D)
//
// Sample window:
//   A: O 92.2082  H 92.5710  L 92.1411  C 92.5357
//   B: O 92.5336  H 92.5923  L 92.3150  C 92.4355
//   C: O 92.4456  H 92.6012  L 92.2826  C 92.3973
//   D: O 92.3895  H 92.4026  L 91.9160  C 92.2528
//
// The shape: highs grind up A -> B -> C (buyers keep pressing) but closes
// fade the whole way. D is the breakdown bar — lowest high AND lowest low
// of the window, with the largest downside range. Distribution completes.
//
// Rules that held across the window:
//   1) Ascending highs across the first three:
//        H(A) < H(B) < H(C)
//   2) C is the window-high on highs:
//        H(C) > H(A)  AND  H(C) > H(B)  AND  H(C) > H(D)
//   3) D fails — prints the lowest high of the window:
//        H(D) < H(A)  AND  H(D) < H(B)  AND  H(D) < H(C)
//   4) Lows form an inverted V — rise into B, then fall:
//        L(A) < L(B)  AND  L(B) > L(C)  AND  L(C) > L(D)
//   5) D prints the window-low on lows:
//        L(D) < L(A)  AND  L(D) < L(B)  AND  L(D) < L(C)
//   6) Monotonic descending closes — distribution from the first bar:
//        C(A) > C(B) > C(C) > C(D)
//   7) Directions: one bull then three consecutive bears:
//        C(A) > O(A)  AND  C(B) < O(B)  AND  C(C) < O(C)  AND  C(D) < O(D)
//   8) Opens: gap up at B, then slide B -> C -> D:
//        O(A) < O(B)  AND  O(B) > O(C)  AND  O(C) > O(D)
//   9) Adjacent candles overlap (no gaps)
// ---------------------------------------------------------------------------
function matchesScenario5(candles, i) {
  const A = candles[i];
  const B = candles[i + 1];
  const C = candles[i + 2];
  const D = candles[i + 3];

  const highsAscend     = A.high < B.high && B.high < C.high;
  const cWindowHigh     = C.high > A.high && C.high > B.high && C.high > D.high;
  const dLowestHigh     = D.high < A.high && D.high < B.high && D.high < C.high;

  const lowsInvertedV   = A.low < B.low && B.low > C.low && C.low > D.low;
  const dWindowLow      = D.low < A.low && D.low < B.low && D.low < C.low;

  const closesDescend   = A.close > B.close && B.close > C.close && C.close > D.close;

  const dirsBullBearBearBear =
        A.close > A.open &&
        B.close < B.open &&
        C.close < C.open &&
        D.close < D.open;

  const opensSpikeThenSlide =
        A.open < B.open &&
        B.open > C.open &&
        C.open > D.open;

  const overlapping     = allAdjacentOverlap([A, B, C, D]);

  return highsAscend
      && cWindowHigh
      && dLowestHigh
      && lowsInvertedV
      && dWindowLow
      && closesDescend
      && dirsBullBearBearBear
      && opensSpikeThenSlide
      && overlapping;
}

// ---------------------------------------------------------------------------
// Scenario 6 — anchor-range containment (body-contained, wick-tolerant)
//
// Concept:
//   Pick any candle as the "anchor". Its high and low define a range box.
//   Walk forward through the following candles. A candle is "contained" if
//   both its OPEN and CLOSE (the body) sit within [anchor.low, anchor.high],
//   inclusive on both ends. Wicks are free to pierce above or below — a
//   candle with a high above the anchor's high or a low below it is still
//   contained so long as its body hasn't crossed.
//
//   The zone ends at the first candle whose open OR close falls outside
//   the anchor's range — that candle is the "break" and is NOT part of the
//   zone. If the series ends before a break, the zone runs to the last
//   candle.
//
//   Minimum size: at least 3 contained candles after the anchor, so the
//   smallest valid zone is 4 candles total (anchor + 3).
//
// This detector is run from every index as a potential anchor, so a single
// long calm stretch can produce overlapping zones from consecutive anchors.
// That's by design — each anchor tells a different story about what range
// is being defended.
//
// Returns the end index of the zone (inclusive), or null if no valid zone
// starts at this anchor.
// ---------------------------------------------------------------------------
function findAnchorRangeEnd(candles, anchorIdx) {
  const anchor = candles[anchorIdx];
  const hi = anchor.high;
  const lo = anchor.low;

  let contained = 0;
  let lastContainedIdx = anchorIdx;

  for (let k = anchorIdx + 1; k < candles.length; k++) {
    const c = candles[k];

    const bodyInside =
      c.open  >= lo && c.open  <= hi &&
      c.close >= lo && c.close <= hi;

    if (bodyInside) {
      contained++;
      lastContainedIdx = k;
    } else {
      break; // body broke out — zone ends before this candle
    }
  }

  return contained >= 3 ? lastContainedIdx : null;
}

// ---------------------------------------------------------------------------
// Scenario 7 — anchor-range with straddle-break on the 3rd following candle
//
// Concept:
//   Same anchor + containment idea as Scenario 6, but the 3rd candle after
//   the anchor (index i+3) is a "straddle" candle that closes the zone:
//
//   Setup (candles i, i+1, i+2, i+3):
//     - i     : anchor — defines the box [anchor.low, anchor.high]
//     - i+1   : body-contained inside the box (wicks may pierce)
//     - i+2   : body-contained inside the box (wicks may pierce)
//     - i+3   : STRADDLE — one of open/close breaks out by a LITTLE on one
//               side, the other of open/close stays inside, and the WICK on
//               the OPPOSITE side probes (either touches near the boundary
//               from inside, or pokes out by a little from outside).
//
//   Variant A (body breaks TOP, wick probes BOTTOM):
//     - One of {open, close} on candle i+3 is strictly ABOVE anchor.high,
//       but by no more than SLIGHTLY_NEAR_PCT of the anchor range.
//     - The other of {open, close} is INSIDE the anchor range.
//     - The candle's LOW sits "slightly near" anchor.low — either just
//       inside (within SLIGHTLY_NEAR_PCT of the range above lo) or just
//       outside (within SLIGHTLY_NEAR_PCT of the range below lo).
//
//   Variant B (body breaks BOTTOM, wick probes TOP) — mirror of A:
//     - One of {open, close} is strictly BELOW anchor.low by no more than
//       SLIGHTLY_NEAR_PCT of the anchor range.
//     - The other of {open, close} is INSIDE the anchor range.
//     - The candle's HIGH sits "slightly near" anchor.high.
//
// Tolerance:
//   "Slightly near" is defined as a fraction of the anchor's range
//   (anchor.high - anchor.low). The default SLIGHTLY_NEAR_PCT of 0.15 (15%)
//   means the break side can extend out by up to 15% of the anchor range,
//   and the wick side can sit within 15% of the range on either side of
//   the opposite boundary. Tune this constant if the threshold feels off
//   for your data — it's the one knob that decides "slight" vs "big".
//
//   Boundary touches (open or close exactly == high or low) are treated as
//   INSIDE, consistent with scenario 6.
//
// The zone ends at candle i+3. No further candles are consumed.
// ---------------------------------------------------------------------------
const SLIGHTLY_NEAR_PCT = 0.15;

function matchesScenario7(candles, i) {
  const anchor = candles[i];
  const c1 = candles[i + 1];
  const c2 = candles[i + 2];
  const s  = candles[i + 3]; // straddle candle

  const hi = anchor.high;
  const lo = anchor.low;
  const range = hi - lo;
  if (range <= 0) return false; // degenerate anchor
  const tol = range * SLIGHTLY_NEAR_PCT;

  // --- c1 and c2: body-contained (opens and closes inside anchor range) ---
  const bodyInside = (c) =>
    c.open  >= lo && c.open  <= hi &&
    c.close >= lo && c.close <= hi;

  if (!bodyInside(c1) || !bodyInside(c2)) return false;

  // --- straddle candle analysis ---
  const sOpenInside  = s.open  >= lo && s.open  <= hi;
  const sCloseInside = s.close >= lo && s.close <= hi;

  const sOpenAbove   = s.open  >  hi;
  const sCloseAbove  = s.close >  hi;
  const sOpenBelow   = s.open  <  lo;
  const sCloseBelow  = s.close <  lo;

  // --- Variant A: body breaks TOP by a little, wick probes BOTTOM ---
  //   exactly one body-endpoint strictly above hi (by <= tol), the other inside,
  //   and s.low is "slightly near" lo (either just inside or just below).
  const oneAboveByLittle =
        (sOpenAbove  && sCloseInside && (s.open  - hi) <= tol) ||
        (sCloseAbove && sOpenInside  && (s.close - hi) <= tol);

  const lowNearLo =
        (s.low >= lo && s.low <= lo + tol) ||   // just inside, touching near lo
        (s.low <  lo && (lo - s.low) <= tol);    // just below by a little

  const variantA = oneAboveByLittle && lowNearLo;

  // --- Variant B: body breaks BOTTOM by a little, wick probes TOP ---
  const oneBelowByLittle =
        (sOpenBelow  && sCloseInside && (lo - s.open)  <= tol) ||
        (sCloseBelow && sOpenInside  && (lo - s.close) <= tol);

  const highNearHi =
        (s.high <= hi && s.high >= hi - tol) ||  // just inside, touching near hi
        (s.high >  hi && (s.high - hi) <= tol);   // just above by a little

  const variantB = oneBelowByLittle && highNearHi;

  return variantA || variantB;
}

module.exports = { findConsolidations };