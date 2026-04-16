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

// Returns 'high' | 'low' | 'both' | null
function classify(B, A, C) {
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

  const raw = [];
  let i = 1;

  while (i < candles.length - 1) {
    const type = classify(candles[i], candles[i - 1], candles[i + 1]);
    if (!type) { i++; continue; }

    const firstMid = i;
    const types    = [type];
    let j = i + 1;
    while (j < candles.length - 1) {
      const t = classify(candles[j], candles[j - 1], candles[j + 1]);
      if (!t) break;
      types.push(t);
      j++;
    }
    const lastMid = j - 1;

    const uniq = Array.from(new Set(types));
    raw.push({
      type:     uniq.length === 1 ? uniq[0] : 'both',
      startIdx: firstMid - 1,
      endIdx:   lastMid + 1,
    });

    i = j + 1;
  }

  // ── chain zones where end of one is start of next ──
  const zones = [];
  let k = 0;
  while (k < raw.length) {
    const chain = [raw[k]];
    while (
      k + 1 < raw.length &&
      raw[k].endIdx === raw[k + 1].startIdx
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
