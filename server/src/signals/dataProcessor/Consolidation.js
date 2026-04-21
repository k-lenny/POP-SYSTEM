function bodyTop(c)    { return Math.max(c.open, c.close); }
function bodyBottom(c) { return Math.min(c.open, c.close); }

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

// Middle-pivot rule: B (middle) wick pokes through A/C bodies but stays inside A/C wicks.
function classifyMiddle(B, A, C) {
  const highSide =
    B.high > bodyTop(A)    && B.high > bodyTop(C) &&
    B.high < A.high        && B.high < C.high;

  const lowSide =
    B.low  < bodyBottom(A) && B.low  < bodyBottom(C) &&
    B.low  > A.low         && B.low  > C.low;

  if (highSide && lowSide) return 'both';
  if (highSide)            return 'high';
  if (lowSide)             return 'low';
  return null;
}

// Forward-pivot rule: A (first) wick pokes above B/C bodies but stays below B's wick.
// Low-side is the mirror using bodyBottom / B.low.
function classifyForward(A, B, C) {
  const highSide =
    A.high > bodyTop(B)    && A.high < B.high &&
    A.high > bodyTop(C);

  const lowSide =
    A.low  < bodyBottom(B) && A.low  > B.low &&
    A.low  < bodyBottom(C);

  if (highSide && lowSide) return 'both';
  if (highSide)            return 'high';
  if (lowSide)             return 'low';
  return null;
}

// Combined check on 3-candle window [s, s+1, s+2].
function classifyWindow(A, B, C) {
  const m = classifyMiddle(B, A, C);
  const f = classifyForward(A, B, C);
  if (!m && !f) return null;
  if (m && f)   return m === f ? m : 'both';
  return m || f;
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

function chainTypes(a, b) {
  if (a === b) return a;
  return 'both';
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

// Merge a chain of zones that share boundary candles (end of one = start of next).
function mergeZones(candles, chain) {
  const startIdx = chain[0].startIdx;
  const endIdx   = chain[chain.length - 1].endIdx;

  let overallType = chain[0].type;
  for (const z of chain) overallType = chainTypes(overallType, z.type);

  const { highest, lowest } = extremaIn(candles, startIdx, endIdx);

  return {
    type:        overallType,
    candleCount: endIdx - startIdx + 1,
    zoneCount:   chain.length,
    start:       candleRef(candles[startIdx], startIdx),
    end:         candleRef(candles[endIdx],   endIdx),
    highest,
    lowest,
  };
}

function findConsolidations(candles) {
  if (!Array.isArray(candles) || candles.length < 3) return [];

  const raw  = [];
  const last = candles.length - 3; // max valid window start

  for (let s = 0; s <= last; s++) {
    const type = classifyWindow(candles[s], candles[s + 1], candles[s + 2]);
    if (!type) continue;

    raw.push({
      type,
      startIdx: s,         // A
      endIdx:   s + 2,     // C
    });
  }

  // ── chain zones whose end falls inside the span of the next zone ──
  const zones = [];
  let k = 0;
  while (k < raw.length) {
    const chain = [raw[k]];
    while (
      k + 1 < raw.length &&
      raw[k].endIdx >= raw[k + 1].startIdx &&
      raw[k].endIdx <= raw[k + 1].endIdx
    ) {
      chain.push(raw[k + 1]);
      k++;
    }
    zones.push(mergeZones(candles, chain));
    k++;
  }

  return zones;
}

module.exports = { findConsolidations };
