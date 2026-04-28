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

// ---------------------------------------------------------------------------
// Opposite-cross helpers: first candle after the breakout whose body crossed
// the OPPOSITE consolidation boundary. Used for post-retest invalidation.
// ---------------------------------------------------------------------------
function findOppositeCrossAbove(candles, breakoutIdx, zoneHi) {
  for (let k = breakoutIdx + 1; k < candles.length; k++) {
    const c = candles[k];
    if (c.open > zoneHi || c.close > zoneHi) return k;
  }
  return -1;
}

function findOppositeCrossBelow(candles, breakoutIdx, zoneLo) {
  for (let k = breakoutIdx + 1; k < candles.length; k++) {
    const c = candles[k];
    if (c.open < zoneLo || c.close < zoneLo) return k;
  }
  return -1;
}

// ---------------------------------------------------------------------------
// Retest breakout finders — scan after retest for body break of vShapeExtreme.
//
// invalidationLevel race: if a candle's body crosses invalidationLevel in
// the wrong direction before the vShapeExtreme body break, return
// { invalidated: true }. Null invalidationLevel means race never fires.
// ---------------------------------------------------------------------------
function findRetestBreakoutAbove(candles, startIdx, vPrice, invalidationLevel) {
  for (let k = startIdx + 1; k < candles.length; k++) {
    const c = candles[k];
    if (invalidationLevel != null && (c.open < invalidationLevel || c.close < invalidationLevel)) {
      return { invalidated: true };
    }
    if (c.open > vPrice || c.close > vPrice) {
      const trigger = c.open > vPrice && c.close > vPrice ? 'open_and_close'
                    : c.open > vPrice ? 'open' : 'close';
      return { ...candleRef(c, k), trigger, brokenLevel: vPrice };
    }
  }
  return null;
}

function findRetestBreakoutBelow(candles, startIdx, vPrice, invalidationLevel) {
  for (let k = startIdx + 1; k < candles.length; k++) {
    const c = candles[k];
    if (invalidationLevel != null && (c.open > invalidationLevel || c.close > invalidationLevel)) {
      return { invalidated: true };
    }
    if (c.open < vPrice || c.close < vPrice) {
      const trigger = c.open < vPrice && c.close < vPrice ? 'open_and_close'
                    : c.open < vPrice ? 'open' : 'close';
      return { ...candleRef(c, k), trigger, brokenLevel: vPrice };
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// V-shape extreme — locked at the peak/trough of the up/down push BEFORE the
// first pullback qualifier appears. Scans forward from ftIdx; vShape is the
// running highest high (above) / lowest low (below) until a Type A/B/C
// qualifier is encountered, at which point vShape is locked.
//
// The retest is then selected as the extreme low/high AFTER vShape and
// BEFORE retestBreakout.
// ---------------------------------------------------------------------------
function findVShapeLockAbove(candles, ftIdx, zoneHi, zoneLo) {
  let vIdx = ftIdx;
  for (let k = ftIdx + 1; k < candles.length; k++) {
    const c = candles[k];
    if (c.high > candles[vIdx].high) { vIdx = k; continue; }
    const qualifies =
      c.open < zoneLo || c.close < zoneLo ||  // Type C
      c.low  < zoneLo ||                      // Type B
      c.low  <= zoneHi;                       // Type A
    if (qualifies) break;
  }
  return { ...candleRef(candles[vIdx], vIdx), extremePrice: candles[vIdx].high };
}

function findVShapeLockBelow(candles, ftIdx, zoneHi, zoneLo) {
  let vIdx = ftIdx;
  for (let k = ftIdx + 1; k < candles.length; k++) {
    const c = candles[k];
    if (c.low < candles[vIdx].low) { vIdx = k; continue; }
    const qualifies =
      c.open > zoneHi || c.close > zoneHi ||  // Type C
      c.high > zoneHi ||                      // Type B
      c.high >= zoneLo;                       // Type A
    if (qualifies) break;
  }
  return { ...candleRef(candles[vIdx], vIdx), extremePrice: candles[vIdx].low };
}

// ---------------------------------------------------------------------------
// Retest candle selection — Type A / B / C classification.
//
// The retest is the extreme low (above) / extreme high (below) found
// BETWEEN the vShape extreme and the retest breakout. The scan window is
// (startIdx, endIdx]; startIdx is vShape's index, endIdx is one before
// retestBreakout (or last candle if no retestBreakout exists).
//
// BREAKOUT ABOVE — pick candle with the LOWEST LOW among qualifying types:
//
//   Type A (in-range):    low >= zoneLo AND low <= zoneHi
//   Type B (wick below):  low < zoneLo AND open >= zoneLo AND close >= zoneLo
//   Type C (body below):  open < zoneLo OR close < zoneLo
//
// INVALIDATION:
//   • Running extreme is Type C and a later body in the window breaks
//     below its low → { invalidated: true } — retest is dead.
//
// BREAKOUT BELOW — mirror: highest high, conditions flipped to zoneHi.
//   Type A: high <= zoneHi AND high >= zoneLo
//   Type B: high > zoneHi AND open <= zoneHi AND close <= zoneHi
//   Type C: open > zoneHi OR close > zoneHi
// ---------------------------------------------------------------------------
function selectRetestAbove(candles, startIdx, endIdx, zoneHi, zoneLo) {
  let extremeIdx  = -1;
  let extremeType = null; // 'A', 'B', or 'C'

  for (let k = startIdx + 1; k <= endIdx; k++) {
    const c = candles[k];

    // Type C invalidation: running C extreme body-broken later in window.
    if (extremeIdx >= 0 && extremeType === 'C') {
      const extLow = candles[extremeIdx].low;
      if (c.open < extLow || c.close < extLow) {
        return { extremeIdx: -1, invalidated: true };
      }
    }

    // Classify. Priority: C > B > A (most specific first).
    let type;
    if (c.open < zoneLo || c.close < zoneLo) {
      type = 'C';
    } else if (c.low < zoneLo) {
      type = 'B';
    } else if (c.low <= zoneHi) {
      type = 'A';
    } else {
      continue;
    }

    if (extremeIdx < 0 || c.low < candles[extremeIdx].low) {
      extremeIdx  = k;
      extremeType = type;
    }
  }

  return { extremeIdx, invalidated: false };
}

function selectRetestBelow(candles, startIdx, endIdx, zoneHi, zoneLo) {
  let extremeIdx  = -1;
  let extremeType = null;

  for (let k = startIdx + 1; k <= endIdx; k++) {
    const c = candles[k];

    if (extremeIdx >= 0 && extremeType === 'C') {
      const extHigh = candles[extremeIdx].high;
      if (c.open > extHigh || c.close > extHigh) {
        return { extremeIdx: -1, invalidated: true };
      }
    }

    let type;
    if (c.open > zoneHi || c.close > zoneHi) {
      type = 'C';
    } else if (c.high > zoneHi) {
      type = 'B';
    } else if (c.high >= zoneLo) {
      type = 'A';
    } else {
      continue;
    }

    if (extremeIdx < 0 || c.high > candles[extremeIdx].high) {
      extremeIdx  = k;
      extremeType = type;
    }
  }

  return { extremeIdx, invalidated: false };
}

// ---------------------------------------------------------------------------
// findRetest — top-level retest resolver.
// ---------------------------------------------------------------------------
function findRetest(candles, zoneHi, zoneLo, breakoutAbove, breakoutBelow) {
  const arrIdxOf = (ref) =>
    ref ? candles.findIndex((c, k) => (c.index ?? k) === ref.index) : -1;

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

    const ftIdx = firstIdx(brkArr + 1, (c) => c.high > brkHigh);
    if (ftIdx < 0) return null;

    // PRE-FOLLOWTHROUGH INVALIDATION: any candle in (brkArr, ftIdx]
    // whose body crossed below zoneLo → above-retest is dead.
    for (let k = brkArr + 1; k <= ftIdx; k++) {
      const c = candles[k];
      if (c.open < zoneLo || c.close < zoneLo) return null;
    }

    // Lock vShape FIRST (highest high after ftIdx until first pullback qualifier).
    const vShape = findVShapeLockAbove(candles, ftIdx, zoneHi, zoneLo);
    const vIdx   = candles.findIndex((c, k) => (c.index ?? k) === vShape.index);

    // Find retestBreakout BEFORE retest. Invalidation race: first opposite
    // body-cross of zoneLo after the original breakout sets the level whose
    // low must not be body-crossed before vShape is body-broken.
    const oppCrossIdx     = findOppositeCrossBelow(candles, brkArr, zoneLo);
    const invalidationLvl = oppCrossIdx >= 0 ? candles[oppCrossIdx].low : null;

    const retestBrk = findRetestBreakoutAbove(candles, vIdx, vShape.extremePrice, invalidationLvl);
    if (retestBrk && retestBrk.invalidated) return null;

    // Retest = extreme low (Type A/B/C) BETWEEN vShape and retestBreakout.
    const brkIdx = retestBrk
      ? candles.findIndex((c, k) => (c.index ?? k) === retestBrk.index)
      : candles.length;
    const endIdx = brkIdx - 1;

    const { extremeIdx, invalidated } = selectRetestAbove(candles, vIdx, endIdx, zoneHi, zoneLo);
    if (invalidated || extremeIdx < 0) return null;

    const ext  = candles[extremeIdx];
    const mode = ext.low < zoneLo ? 'cross' : 'return';

    return {
      direction:      'above',
      followThrough:  candleRef(candles[ftIdx], ftIdx),
      candle:         candleRef(ext, extremeIdx),
      mode,
      vShapeExtreme:  vShape,
      retestBreakout: retestBrk,
    };
  };

  // ---- BELOW breakout retest path ----
  const tryBelow = () => {
    const brkArr = arrIdxOf(breakoutBelow);
    if (brkArr < 0) return null;
    const brkLow = breakoutBelow.low;

    const ftIdx = firstIdx(brkArr + 1, (c) => c.low < brkLow);
    if (ftIdx < 0) return null;

    // PRE-FOLLOWTHROUGH INVALIDATION: any candle in (brkArr, ftIdx]
    // whose body crossed above zoneHi → below-retest is dead.
    for (let k = brkArr + 1; k <= ftIdx; k++) {
      const c = candles[k];
      if (c.open > zoneHi || c.close > zoneHi) return null;
    }

    // Lock vShape FIRST (lowest low after ftIdx until first pullback qualifier).
    const vShape = findVShapeLockBelow(candles, ftIdx, zoneHi, zoneLo);
    const vIdx   = candles.findIndex((c, k) => (c.index ?? k) === vShape.index);

    // Find retestBreakout BEFORE retest. Invalidation race: first opposite
    // body-cross of zoneHi after the original breakout sets the level whose
    // high must not be body-crossed before vShape is body-broken.
    const oppCrossIdx     = findOppositeCrossAbove(candles, brkArr, zoneHi);
    const invalidationLvl = oppCrossIdx >= 0 ? candles[oppCrossIdx].high : null;

    const retestBrk = findRetestBreakoutBelow(candles, vIdx, vShape.extremePrice, invalidationLvl);
    if (retestBrk && retestBrk.invalidated) return null;

    // Retest = extreme high (Type A/B/C) BETWEEN vShape and retestBreakout.
    const brkIdx = retestBrk
      ? candles.findIndex((c, k) => (c.index ?? k) === retestBrk.index)
      : candles.length;
    const endIdx = brkIdx - 1;

    const { extremeIdx, invalidated } = selectRetestBelow(candles, vIdx, endIdx, zoneHi, zoneLo);
    if (invalidated || extremeIdx < 0) return null;

    const ext  = candles[extremeIdx];
    const mode = ext.high > zoneHi ? 'cross' : 'return';

    return {
      direction:      'below',
      followThrough:  candleRef(candles[ftIdx], ftIdx),
      candle:         candleRef(ext, extremeIdx),
      mode,
      vShapeExtreme:  vShape,
      retestBreakout: retestBrk,
    };
  };

  const above = tryAbove();
  const below = tryBelow();
  if (above && below) {
    return above.candle.index <= below.candle.index ? above : below;
  }
  return above || below || null;
}

// ---------------------------------------------------------------------------
// Helper: every adjacent pair shares price overlap (no gaps).
// ---------------------------------------------------------------------------
function allAdjacentOverlap(window) {
  for (let k = 0; k < window.length - 1; k++) {
    const x = window[k], y = window[k + 1];
    if (Math.min(x.high, y.high) < Math.max(x.low, y.low)) return false;
  }
  return true;
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

function matchesScenario1(candles, i) {
  const A = candles[i], B = candles[i+1], C = candles[i+2], D = candles[i+3], E = candles[i+4];
  const bHighPivot    = B.high > A.high && B.high > C.high;
  const cLowPivot     = C.low < B.low && C.low < D.low;
  const closesRecover = D.close > C.close && E.close > D.close;
  const opensStepUp   = C.open < D.open && D.open < E.open;
  const overlapping   = allAdjacentOverlap([A,B,C,D,E]);
  return bHighPivot && cLowPivot && closesRecover && opensStepUp && overlapping;
}

function matchesScenario2(candles, i) {
  const A = candles[i], B = candles[i+1], C = candles[i+2];
  const descendingHighs  = A.high > B.high && B.high > C.high;
  const descendingLows   = A.low > B.low && B.low > C.low;
  const alternatingDirs  = A.close < A.open && B.close > B.open && C.close < C.open;
  const bClosePivot      = B.close > A.close && B.close > C.close;
  const netUpwardDrift   = C.close > A.close;
  const vShapeOpens      = B.open < A.open && C.open > B.open;
  const overlapping      = allAdjacentOverlap([A,B,C]);
  return descendingHighs && descendingLows && alternatingDirs && bClosePivot && netUpwardDrift && vShapeOpens && overlapping;
}

function matchesScenario3(candles, i) {
  const A = candles[i], B = candles[i+1], C = candles[i+2];
  const bHighPivot       = B.high > A.high && B.high > C.high;
  const cLowPivot        = C.low < A.low && C.low < B.low;
  const closesDescend    = A.close > B.close && B.close > C.close;
  const bOpenHighest     = B.open > A.open && B.open > C.open;
  const dirsBullBearBear = A.close > A.open && B.close < B.open && C.close < C.open;
  const overlapping      = allAdjacentOverlap([A,B,C]);
  return bHighPivot && cLowPivot && closesDescend && bOpenHighest && dirsBullBearBear && overlapping;
}

function matchesScenario4(candles, i) {
  const A = candles[i], B = candles[i+1], C = candles[i+2], D = candles[i+3];
  const aWindowHigh     = A.high > B.high && A.high > C.high && A.high > D.high;
  const highsStepDown   = A.high > B.high && B.high > C.high;
  const dRecoversCapped = D.high > B.high && D.high > C.high && D.high < A.high;
  const bWindowLow      = B.low < A.low && B.low < C.low && B.low < D.low;
  const dirsAlternate   = A.close < A.open && B.close > B.open && C.close < C.open && D.close > D.open;
  const zigzagCloses    = A.close < B.close && B.close > C.close && C.close < D.close;
  const dClosePeak      = D.close > A.close && D.close > B.close && D.close > C.close;
  const cCloseTrough    = C.close < A.close && C.close < B.close && C.close < D.close;
  const opensSlide      = A.open > B.open && D.open < C.open;
  const netRecovery     = D.close > A.close;
  const overlapping     = allAdjacentOverlap([A,B,C,D]);
  return aWindowHigh && highsStepDown && dRecoversCapped && bWindowLow && dirsAlternate && zigzagCloses && dClosePeak && cCloseTrough && opensSlide && netRecovery && overlapping;
}

function matchesScenario5(candles, i) {
  const A = candles[i], B = candles[i+1], C = candles[i+2], D = candles[i+3];
  const highsAscend          = A.high < B.high && B.high < C.high;
  const cWindowHigh          = C.high > A.high && C.high > B.high && C.high > D.high;
  const dLowestHigh          = D.high < A.high && D.high < B.high && D.high < C.high;
  const lowsInvertedV        = A.low < B.low && B.low > C.low && C.low > D.low;
  const dWindowLow           = D.low < A.low && D.low < B.low && D.low < C.low;
  const closesDescend        = A.close > B.close && B.close > C.close && C.close > D.close;
  const dirsBullBearBearBear = A.close > A.open && B.close < B.open && C.close < C.open && D.close < D.open;
  const opensSpikeThenSlide  = A.open < B.open && B.open > C.open && C.open > D.open;
  const overlapping          = allAdjacentOverlap([A,B,C,D]);
  return highsAscend && cWindowHigh && dLowestHigh && lowsInvertedV && dWindowLow && closesDescend && dirsBullBearBearBear && opensSpikeThenSlide && overlapping;
}

function findAnchorRangeEnd(candles, anchorIdx) {
  const anchor = candles[anchorIdx];
  const hi = anchor.high, lo = anchor.low;
  let contained = 0, lastContainedIdx = anchorIdx;
  for (let k = anchorIdx + 1; k < candles.length; k++) {
    const c = candles[k];
    const bodyInside = c.open >= lo && c.open <= hi && c.close >= lo && c.close <= hi;
    if (bodyInside) { contained++; lastContainedIdx = k; } else break;
  }
  return contained >= 2 ? lastContainedIdx : null;
}

const SLIGHTLY_NEAR_PCT = 0.15;
function matchesScenario7(candles, i) {
  const anchor = candles[i], c1 = candles[i+1], c2 = candles[i+2], s = candles[i+3];
  const hi = anchor.high, lo = anchor.low;
  const range = hi - lo;
  if (range <= 0) return false;
  const tol = range * SLIGHTLY_NEAR_PCT;
  const bodyInside = (c) => c.open >= lo && c.open <= hi && c.close >= lo && c.close <= hi;
  if (!bodyInside(c1) || !bodyInside(c2)) return false;
  const oneAboveByLittle = (s.open > hi && s.close >= lo && s.close <= hi && (s.open - hi) <= tol) ||
                           (s.close > hi && s.open >= lo && s.open <= hi && (s.close - hi) <= tol);
  const lowNearLo        = (s.low >= lo && s.low <= lo + tol) || (s.low < lo && (lo - s.low) <= tol);
  const variantA         = oneAboveByLittle && lowNearLo;
  const oneBelowByLittle = (s.open < lo && s.close >= lo && s.close <= hi && (lo - s.open) <= tol) ||
                           (s.close < lo && s.open >= lo && s.open <= hi && (lo - s.close) <= tol);
  const highNearHi       = (s.high <= hi && s.high >= hi - tol) || (s.high > hi && (s.high - hi) <= tol);
  const variantB         = oneBelowByLittle && highNearHi;
  return variantA || variantB;
}

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

  // Attach confirmationConsolidation: earliest other zone (by start, then end)
  // whose range ends at or after the retest breakout candle. The retest
  // breakout itself can be part of that confirmation consolidation.
  for (const z of zones) {
    if (!z.retest || !z.retest.retestBreakout) continue;
    const retestBrkIdx = z.retest.retestBreakout.index;

    let best = null;
    for (const c of zones) {
      if (c === z) continue;
      const cEnd = c.end.index ?? 0;
      if (cEnd < retestBrkIdx) continue;
      if (!best) { best = c; continue; }
      const cStart = c.start.index ?? 0;
      const bStart = best.start.index ?? 0;
      const bEnd   = best.end.index ?? 0;
      if (cStart < bStart || (cStart === bStart && cEnd < bEnd)) best = c;
    }

    if (best) {
      const dirBrk = z.retest.direction === 'above'
        ? (best.breakout && best.breakout.above) || null
        : (best.breakout && best.breakout.below) || null;
      z.retest.confirmationConsolidation = {
        start:    best.start,
        end:      best.end,
        breakout: dirBrk,
      };
    } else {
      z.retest.confirmationConsolidation = null;
    }
  }

  return zones;
}

module.exports = { findConsolidations };