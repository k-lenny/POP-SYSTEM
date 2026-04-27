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

// How far past the consolidation's end candle we'll scan looking for the
// breakout candle. If no candle in the next BREAKOUT_SCAN_LIMIT bars
// triggers a breakout (open or close beyond the zone boundary), the
// breakout side is reported as null. This keeps the detector from
// associating a zone with a "breakout" that happened many bars later
// when the zone is no longer relevant.
const BREAKOUT_SCAN_LIMIT = 10;

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

function findBreakoutAbove(candles, endIdx, hiPrice) {
  const stopIdx = Math.min(candles.length - 1, endIdx + BREAKOUT_SCAN_LIMIT);
  for (let k = endIdx + 1; k <= stopIdx; k++) {
    const c = candles[k];
    const openAbove  = c.open  > hiPrice;
    const closeAbove = c.close > hiPrice;
    if (openAbove || closeAbove) {
      const trigger = openAbove && closeAbove ? 'open_and_close'
                    : openAbove ? 'open' : 'close';
      return { ...candleRef(c, k), trigger, brokenLevel: hiPrice };
    }
  }
  return null;
}

function findBreakoutBelow(candles, endIdx, loPrice) {
  const stopIdx = Math.min(candles.length - 1, endIdx + BREAKOUT_SCAN_LIMIT);
  for (let k = endIdx + 1; k <= stopIdx; k++) {
    const c = candles[k];
    const openBelow  = c.open  < loPrice;
    const closeBelow = c.close < loPrice;
    if (openBelow || closeBelow) {
      const trigger = openBelow && closeBelow ? 'open_and_close'
                    : openBelow ? 'open' : 'close';
      return { ...candleRef(c, k), trigger, brokenLevel: loPrice };
    }
  }
  return null;
}

function buildZone(candles, startIdx, endIdx, scenario) {
  const { highest, lowest } = extremaIn(candles, startIdx, endIdx);
  const breakoutAbove = findBreakoutAbove(candles, endIdx, highest.price);
  const breakoutBelow = findBreakoutBelow(candles, endIdx, lowest.price);
  return {
    scenario,
    candleCount: endIdx - startIdx + 1,
    start:       candleRef(candles[startIdx], startIdx),
    end:         candleRef(candles[endIdx],   endIdx),
    highest,
    lowest,
    breakout: {
      above: breakoutAbove,
      below: breakoutBelow,
    },
    retest: findRetest(candles, highest.price, lowest.price, breakoutAbove, breakoutBelow),
  };
}

// ---------------------------------------------------------------------------
// Retest
//
// After a breakout, the market often pushes further in the breakout
// direction (the "follow-through") and then comes back to retest the zone.
// The retest is what we capture here.
//
// BREAKOUT ABOVE flow:
//   1. Find the FOLLOW-THROUGH candle: the first candle after the breakout
//      whose HIGH strictly exceeds the breakout candle's HIGH. Wick is
//      enough; we don't require a body break.
//   2. After the follow-through candle, find the RETEST candle: the first
//      candle that EITHER
//        a) crosses below the consolidation's low (low < zoneLo, OR
//           open < zoneLo, OR close < zoneLo), OR
//        b) returns into the consolidation range (any OHLC inside
//           [zoneLo, zoneHi]).
//      First match wins. If a single candle satisfies both on the same
//      bar, "cross below low" takes priority — we report that case.
//
// BREAKOUT BELOW: mirror.
//   1. Follow-through: first candle with LOW strictly below breakout's LOW.
//   2. Retest: first candle that EITHER
//        a) crosses above zoneHi (high/open/close > zoneHi), OR
//        b) returns into [zoneLo, zoneHi] (any OHLC inside).
//      Cross-above-high takes priority on a same-bar tie.
//
// Scan range: unbounded — we scan from after the follow-through to the
// end of the data. Either we find a retest candle or we return null.
//
// If no breakout exists, or the breakout never gets a follow-through,
// retest is null.
//
// If both breakout sides exist (above and below), preference goes to the
// first one (chronologically) whose follow-through can be found. If both
// trigger from the same direction in time, we go with `above` first.
//
// Return shape (or null):
//   {
//     direction:   'above' | 'below',     // breakout direction this retest belongs to
//     followThrough: { ...candleRef },    // the candle that pushed past breakout high/low
//     candle:        { ...candleRef },    // the retest candle itself
//     mode:          'cross' | 'return',  // 'cross' = pierced opposite zone boundary;
//                                         //   'return' = came back into [zoneLo, zoneHi]
//   }
// ---------------------------------------------------------------------------
// Find the extreme V-shape candle strictly between the breakout and the
// retest. For an 'above' breakout this is the highest-high candle in that
// span (the peak of the V); for 'below' it's the lowest-low (the trough).
// Returns null if there are no candles strictly between the two indices.
function findVShapeExtreme(candles, brkIdx, retestIdx, direction) {
  const from = brkIdx + 1;
  const to   = retestIdx - 1;
  if (from > to) return null;

  let extIdx = from;
  if (direction === 'above') {
    for (let k = from + 1; k <= to; k++) {
      if (candles[k].high > candles[extIdx].high) extIdx = k;
    }
    return { ...candleRef(candles[extIdx], extIdx), extremePrice: candles[extIdx].high };
  } else {
    for (let k = from + 1; k <= to; k++) {
      if (candles[k].low < candles[extIdx].low) extIdx = k;
    }
    return { ...candleRef(candles[extIdx], extIdx), extremePrice: candles[extIdx].low };
  }
}

function findRetest(candles, zoneHi, zoneLo, breakoutAbove, breakoutBelow) {
  // Resolve a breakout's array index given its candle ref.
  const arrIdxOf = (ref) =>
    ref ? candles.findIndex((c, k) => (c.index ?? k) === ref.index) : -1;

  // Helper: find the index of the first candle in [from..end] satisfying
  // predicate. Returns -1 if not found.
  const firstIdx = (from, pred) => {
    for (let k = from; k < candles.length; k++) {
      if (pred(candles[k])) return k;
    }
    return -1;
  };

  // ---- ABOVE breakout retest path ----
  const tryAbove = () => {
    const brkArr = arrIdxOf(breakoutAbove);
    if (brkArr < 0) return null;
    const brkHigh = breakoutAbove.high;

    // 1. Follow-through: first candle after the breakout whose high > brk.high.
    const ftIdx = firstIdx(brkArr + 1, (c) => c.high > brkHigh);
    if (ftIdx < 0) return null;

    // 2. Retest after follow-through: first candle that either crosses
    //    below zoneLo or returns into [zoneLo, zoneHi]. Cross wins on ties.
    for (let k = ftIdx + 1; k < candles.length; k++) {
      const c = candles[k];

      const crossBelow =
        c.low   < zoneLo ||
        c.open  < zoneLo ||
        c.close < zoneLo;

      if (crossBelow) {
        return {
          direction:     'above',
          followThrough: candleRef(candles[ftIdx], ftIdx),
          candle:        candleRef(c, k),
          mode:          'cross',
          vShapeExtreme: findVShapeExtreme(candles, brkArr, k, 'above'),
        };
      }

      const inRange = (v) => v >= zoneLo && v <= zoneHi;
      const returned =
        inRange(c.open)  ||
        inRange(c.high)  ||
        inRange(c.low)   ||
        inRange(c.close);

      if (returned) {
        return {
          direction:     'above',
          followThrough: candleRef(candles[ftIdx], ftIdx),
          candle:        candleRef(c, k),
          mode:          'return',
          vShapeExtreme: findVShapeExtreme(candles, brkArr, k, 'above'),
        };
      }
    }
    return null;
  };

  // ---- BELOW breakout retest path ----
  const tryBelow = () => {
    const brkArr = arrIdxOf(breakoutBelow);
    if (brkArr < 0) return null;
    const brkLow = breakoutBelow.low;

    const ftIdx = firstIdx(brkArr + 1, (c) => c.low < brkLow);
    if (ftIdx < 0) return null;

    for (let k = ftIdx + 1; k < candles.length; k++) {
      const c = candles[k];

      const crossAbove =
        c.high  > zoneHi ||
        c.open  > zoneHi ||
        c.close > zoneHi;

      if (crossAbove) {
        return {
          direction:     'below',
          followThrough: candleRef(candles[ftIdx], ftIdx),
          candle:        candleRef(c, k),
          mode:          'cross',
          vShapeExtreme: findVShapeExtreme(candles, brkArr, k, 'below'),
        };
      }

      const inRange = (v) => v >= zoneLo && v <= zoneHi;
      const returned =
        inRange(c.open)  ||
        inRange(c.high)  ||
        inRange(c.low)   ||
        inRange(c.close);

      if (returned) {
        return {
          direction:     'below',
          followThrough: candleRef(candles[ftIdx], ftIdx),
          candle:        candleRef(c, k),
          mode:          'return',
          vShapeExtreme: findVShapeExtreme(candles, brkArr, k, 'below'),
        };
      }
    }
    return null;
  };

  // If both breakout directions exist with valid retest paths, prefer
  // whichever retest candle is chronologically earliest. If neither has a
  // retest, return null. If only one has one, return that.
  const above = tryAbove();
  const below = tryBelow();
  if (above && below) {
    return above.candle.index <= below.candle.index ? above : below;
  }
  return above || below || null;
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
// (rules unchanged — see prior version for the sample window and notes)
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

// ---------------------------------------------------------------------------
// Scenario 5 — 4-candle rising-wedge top with D-failure (A,B,C,D)
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
      break;
    }
  }

  return contained >= 2 ? lastContainedIdx : null;
}

// ---------------------------------------------------------------------------
// Scenario 7 — anchor-range with straddle-break on the 3rd following candle
// ---------------------------------------------------------------------------
const SLIGHTLY_NEAR_PCT = 0.15;

function matchesScenario7(candles, i) {
  const anchor = candles[i];
  const c1 = candles[i + 1];
  const c2 = candles[i + 2];
  const s  = candles[i + 3];

  const hi = anchor.high;
  const lo = anchor.low;
  const range = hi - lo;
  if (range <= 0) return false;
  const tol = range * SLIGHTLY_NEAR_PCT;

  const bodyInside = (c) =>
    c.open  >= lo && c.open  <= hi &&
    c.close >= lo && c.close <= hi;

  if (!bodyInside(c1) || !bodyInside(c2)) return false;

  const sOpenInside  = s.open  >= lo && s.open  <= hi;
  const sCloseInside = s.close >= lo && s.close <= hi;

  const sOpenAbove   = s.open  >  hi;
  const sCloseAbove  = s.close >  hi;
  const sOpenBelow   = s.open  <  lo;
  const sCloseBelow  = s.close <  lo;

  const oneAboveByLittle =
        (sOpenAbove  && sCloseInside && (s.open  - hi) <= tol) ||
        (sCloseAbove && sOpenInside  && (s.close - hi) <= tol);

  const lowNearLo =
        (s.low >= lo && s.low <= lo + tol) ||
        (s.low <  lo && (lo - s.low) <= tol);

  const variantA = oneAboveByLittle && lowNearLo;

  const oneBelowByLittle =
        (sOpenBelow  && sCloseInside && (lo - s.open)  <= tol) ||
        (sCloseBelow && sOpenInside  && (lo - s.close) <= tol);

  const highNearHi =
        (s.high <= hi && s.high >= hi - tol) ||
        (s.high >  hi && (s.high - hi) <= tol);

  const variantB = oneBelowByLittle && highNearHi;

  return variantA || variantB;
}

// ---------------------------------------------------------------------------
// Top-level entry — slide all scenarios across the candle array
// ---------------------------------------------------------------------------
function findConsolidations(candles) {
  if (!Array.isArray(candles) || candles.length < 3) return [];

  const zones = [];

  if (candles.length >= 5) {
    for (let i = 0; i + 4 < candles.length; i++) {
      if (matchesScenario1(candles, i)) {
        zones.push(buildZone(candles, i, i + 4, 'scenario1_5candle_stairstep'));
      }
    }
  }

  for (let i = 0; i + 2 < candles.length; i++) {
    if (matchesScenario2(candles, i)) {
      zones.push(buildZone(candles, i, i + 2, 'scenario2_3candle_compression'));
    }
  }

  for (let i = 0; i + 2 < candles.length; i++) {
    if (matchesScenario3(candles, i)) {
      zones.push(buildZone(candles, i, i + 2, 'scenario3_3candle_failed_push'));
    }
  }

  if (candles.length >= 4) {
    for (let i = 0; i + 3 < candles.length; i++) {
      if (matchesScenario4(candles, i)) {
        zones.push(buildZone(candles, i, i + 3, 'scenario4_4candle_capped_v_recovery'));
      }
    }
  }

  if (candles.length >= 4) {
    for (let i = 0; i + 3 < candles.length; i++) {
      if (matchesScenario5(candles, i)) {
        zones.push(buildZone(candles, i, i + 3, 'scenario5_4candle_rising_wedge_failure'));
      }
    }
  }

  if (candles.length >= 4) {
    for (let i = 0; i < candles.length; i++) {
      const endIdx = findAnchorRangeEnd(candles, i);
      if (endIdx !== null) {
        zones.push(buildZone(candles, i, endIdx, 'scenario6_anchor_range_containment'));
      }
    }
  }

  if (candles.length >= 4) {
    for (let i = 0; i + 3 < candles.length; i++) {
      if (matchesScenario7(candles, i)) {
        zones.push(buildZone(candles, i, i + 3, 'scenario7_anchor_straddle_break'));
      }
    }
  }

  zones.sort((a, b) => {
    const sa = a.start.index ?? 0;
    const sb = b.start.index ?? 0;
    if (sa !== sb) return sa - sb;
    return (a.end.index ?? 0) - (b.end.index ?? 0);
  });

  return zones;
}

module.exports = { findConsolidations };