// server/src/signals/dataProcessor/candle.js
//
// Candle pattern detector.
// Scans OHLC candles for classical patterns and adds breakout confirmation.

const signalEngine = require('../signalEngine');
const { calculateBodyPercentage } = require('./CandleData');

// ───────────────────────────────────────────────────────────────
// Shared helpers
// ───────────────────────────────────────────────────────────────

function bodySize(c)   { return Math.abs(c.close - c.open); }
function totalRange(c) { return Math.abs(c.high - c.low); }
function upperWick(c)  { return c.high - Math.max(c.open, c.close); }
function lowerWick(c)  { return Math.min(c.open, c.close) - c.low; }

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
    open:          candle.open,
    high:          candle.high,
    low:           candle.low,
    close:         candle.close,
  };
}

function calcBodyPct(c) {
  try { return calculateBodyPercentage(c); } catch (e) { return undefined; }
}

// ───────────────────────────────────────────────────────────────
// Stage 1 – find breakout candle (body break) with dynamic wick rule
//
// invalidationLevel: the pattern's opposite extreme.
// For bullish breakouts (looking for price to go above the highest high):
//   invalidationLevel = lowest low — if price opens or closes below it first → null.
// For bearish breakouts (looking for price to go below the lowest low):
//   invalidationLevel = highest high — if price opens or closes above it first → null.
// ───────────────────────────────────────────────────────────────

function findBreakoutCandle(allCandles, startIdx, initialLevel, direction, invalidationLevel) {
  let dynamicLevel = initialLevel;
  for (let i = startIdx; i < allCandles.length; i++) {
    const c = allCandles[i];
    if (direction === 'bullish') {
      // Invalidate if price opens or closes below the pattern's lowest low
      if (invalidationLevel != null &&
          (c.open < invalidationLevel || c.close < invalidationLevel)) {
        return null;
      }
      if (c.open > dynamicLevel || c.close > dynamicLevel) {
        return { candle: candleRef(c, i), index: i };
      } else if (c.high > dynamicLevel) {
        dynamicLevel = c.high;
      }
    } else { // bearish
      // Invalidate if price opens or closes above the pattern's highest high
      if (invalidationLevel != null &&
          (c.open > invalidationLevel || c.close > invalidationLevel)) {
        return null;
      }
      if (c.open < dynamicLevel || c.close < dynamicLevel) {
        return { candle: candleRef(c, i), index: i };
      } else if (c.low < dynamicLevel) {
        dynamicLevel = c.low;
      }
    }
  }
  return null;
}

// ───────────────────────────────────────────────────────────────
// Stage 2 – push confirmation (candleConfirmation) with dynamic wick rule
//
// invalidationLevel: the pattern's lowest low (bullish) or highest high
// (bearish).  If any candle opens or closes beyond it before the confirmation
// push is found, we return null immediately so candleConfirmation stays null.
// ───────────────────────────────────────────────────────────────

function findConfirmationPush(allCandles, startIdx, breakoutCandle, direction, invalidationLevel) {
  if (startIdx >= allCandles.length) return null;

  let dynamicLevel = direction === 'bullish'
    ? breakoutCandle.high
    : breakoutCandle.low;

  for (let i = startIdx; i < allCandles.length; i++) {
    const c = allCandles[i];
    if (direction === 'bullish') {
      // Invalidate if price crosses below the pattern's lowest low
      if (invalidationLevel != null &&
          (c.open < invalidationLevel || c.close < invalidationLevel)) {
        return null;
      }
      if (c.open > dynamicLevel || c.close > dynamicLevel) {
        const ref = candleRef(c, i);
        return { candle: ref, index: i, bodyPercentage: calcBodyPct(ref) };
      } else if (c.high > dynamicLevel) {
        dynamicLevel = c.high;
      }
    } else { // bearish
      // Invalidate if price crosses above the pattern's highest high
      if (invalidationLevel != null &&
          (c.open > invalidationLevel || c.close > invalidationLevel)) {
        return null;
      }
      if (c.open < dynamicLevel || c.close < dynamicLevel) {
        const ref = candleRef(c, i);
        return { candle: ref, index: i, bodyPercentage: calcBodyPct(ref) };
      } else if (c.low < dynamicLevel) {
        dynamicLevel = c.low;
      }
    }
  }
  return null;
}

// ───────────────────────────────────────────────────────────────
// Retest + V‑shape logic
// ───────────────────────────────────────────────────────────────

function addRetestAndVshape(allCandles, baseResult, direction) {
  baseResult.candleConfirmationRetest       = null;
  baseResult.candleConfirmationRetestVshape = null;

  const confCandle = baseResult.candleConfirmation;
  if (!confCandle || confCandle.index == null) return baseResult;

  const startIdx = confCandle.index + 1;
  if (startIdx >= allCandles.length) return baseResult;

  const isBullish = direction === 'bullish';
  const confHigh  = confCandle.high;
  const confLow   = confCandle.low;

  let vshapeCandle = null;
  let vshapeIdx    = -1;
  let retestCandle = null;
  let retestIdx    = -1;

  for (let i = startIdx; i < allCandles.length; i++) {
    const c = allCandles[i];

    if (isBullish) {
      if (c.open > confHigh || c.close > confHigh) return baseResult;
    } else {
      if (c.open < confLow  || c.close < confLow)  return baseResult;
    }

    if (retestCandle && i > retestIdx) {
      const broke = isBullish
        ? (c.open < vshapeCandle.low  || c.close < vshapeCandle.low)
        : (c.open > vshapeCandle.high || c.close > vshapeCandle.high);

      if (broke) {
        baseResult.candleConfirmationRetest       = retestCandle;
        baseResult.candleConfirmationRetestVshape = vshapeCandle;
        return baseResult;
      }
    }

    if (!vshapeCandle) {
      vshapeCandle = candleRef(c, i);
      vshapeIdx    = i;
    } else if (isBullish ? (c.low < vshapeCandle.low) : (c.high > vshapeCandle.high)) {
      vshapeCandle = candleRef(c, i);
      vshapeIdx    = i;
      retestCandle = null;
      retestIdx    = -1;
    }

    if (i > vshapeIdx) {
      const inRange = isBullish
        ? (c.low  >= confLow && c.low  <= confHigh)
        : (c.high >= confLow && c.high <= confHigh);

      if (inRange) {
        const moreExtreme = !retestCandle || (
          isBullish ? (c.low  < retestCandle.low)
                    : (c.high > retestCandle.high)
        );
        if (moreExtreme) {
          retestCandle = candleRef(c, i);
          retestIdx    = i;
        }
      }
    }
  }

  return baseResult;
}

// ───────────────────────────────────────────────────────────────
// Build breakout for directional patterns (with retest fields)
// ───────────────────────────────────────────────────────────────

function buildBreakout(patternCandles, direction, allCandles, firstIdx) {
  const level = direction === 'bullish'
    ? Math.max(...patternCandles.map(c => c.high))
    : Math.min(...patternCandles.map(c => c.low));

  const invalidationLevel = direction === 'bullish'
    ? Math.min(...patternCandles.map(c => c.low))
    : Math.max(...patternCandles.map(c => c.high));

  const breakoutDirection = direction === 'bullish' ? 'above' : 'below';

  if (!Array.isArray(allCandles) || firstIdx >= allCandles.length) {
    return {
      breakout: {
        level,
        direction: breakoutDirection,
        confirmed: null,
        confirmingCandle: null,
        candlesChecked: 0,
        confirmationCandlesChecked: 0,
      },
      candleConfirmation: null,
      candleConfirmationRetest: null,
      candleConfirmationRetestVshape: null,
    };
  }

  const stage1 = findBreakoutCandle(allCandles, firstIdx, level, direction, invalidationLevel);
  const confirmingCandle = stage1 ? stage1.candle : null;
  const confirmed = !!confirmingCandle;
  const candlesChecked = stage1
    ? stage1.index - firstIdx + 1
    : allCandles.length - firstIdx;

  let pushResult = null;
  let confCandlesChecked = 0;
  if (stage1 && stage1.index + 1 < allCandles.length) {
    pushResult = findConfirmationPush(allCandles, stage1.index + 1, confirmingCandle, direction, invalidationLevel);
    if (pushResult) {
      confCandlesChecked = stage1.index + 1;
      for (let k = stage1.index + 1; k <= pushResult.index; k++) {
        confCandlesChecked++;
      }
    } else {
      confCandlesChecked = allCandles.length - (stage1.index + 1);
    }
  }

  const confirmingBodyPct = confirmingCandle ? calcBodyPct(confirmingCandle) : undefined;

  const baseResult = {
    breakout: {
      level,
      direction: breakoutDirection,
      confirmed,
      confirmingCandle,
      candlesChecked,
      confirmationCandlesChecked: confCandlesChecked,
      confirmingCandleBodyPercentage: confirmingBodyPct,
    },
    candleConfirmation: pushResult ? pushResult.candle : null,
    candleConfirmationBodyPercentage: pushResult ? pushResult.bodyPercentage : undefined,
  };

  return addRetestAndVshape(allCandles, baseResult, direction);
}

// ───────────────────────────────────────────────────────────────
// Neutral breakout (Doji, Spinning Top)
// ───────────────────────────────────────────────────────────────

function findNeutralBreakoutCandle(allCandles, startIdx, bullLevel, bearLevel) {
  let dynBull = bullLevel;
  let dynBear = bearLevel;
  for (let i = startIdx; i < allCandles.length; i++) {
    const c = allCandles[i];
    if (c.open > dynBull || c.close > dynBull) {
      return { candle: candleRef(c, i), index: i, direction: 'above' };
    }
    if (c.open < dynBear || c.close < dynBear) {
      return { candle: candleRef(c, i), index: i, direction: 'below' };
    }
    if (c.high > dynBull) dynBull = c.high;
    if (c.low < dynBear)  dynBear = c.low;
  }
  return null;
}

function buildNeutralBreakout(patternCandles, allCandles, firstIdx) {
  const bullLevel = Math.max(...patternCandles.map(c => c.high));
  const bearLevel = Math.min(...patternCandles.map(c => c.low));

  if (!Array.isArray(allCandles) || firstIdx >= allCandles.length) {
    return {
      breakout: {
        level: bullLevel,
        direction: 'above',
        confirmed: null,
        confirmingCandle: null,
        candlesChecked: 0,
        confirmationCandlesChecked: 0,
        altLevel: bearLevel,
      },
      candleConfirmation: null,
      candleConfirmationRetest: null,
      candleConfirmationRetestVshape: null,
    };
  }

  const stage1 = findNeutralBreakoutCandle(allCandles, firstIdx, bullLevel, bearLevel);
  const confirmingCandle = stage1 ? stage1.candle : null;
  const confirmed = !!confirmingCandle;
  const direction = stage1 ? stage1.direction : 'above';
  const candlesChecked = stage1
    ? stage1.index - firstIdx + 1
    : allCandles.length - firstIdx;

  let pushResult = null;
  let confCandlesChecked = 0;
  if (stage1 && stage1.index + 1 < allCandles.length) {
    const invalidationLevel = direction === 'above' ? bearLevel : bullLevel;
    pushResult = findConfirmationPush(
      allCandles,
      stage1.index + 1,
      confirmingCandle,
      direction === 'above' ? 'bullish' : 'bearish',
      invalidationLevel
    );
    if (pushResult) {
      confCandlesChecked = stage1.index + 1;
      for (let k = stage1.index + 1; k <= pushResult.index; k++) {
        confCandlesChecked++;
      }
    } else {
      confCandlesChecked = allCandles.length - (stage1.index + 1);
    }
  }

  const confirmingBodyPct = confirmingCandle ? calcBodyPct(confirmingCandle) : undefined;

  const baseResult = {
    breakout: {
      level: direction === 'above' ? bullLevel : bearLevel,
      direction,
      confirmed,
      confirmingCandle,
      candlesChecked,
      confirmationCandlesChecked: confCandlesChecked,
      altLevel: direction === 'above' ? bearLevel : bullLevel,
      confirmingCandleBodyPercentage: confirmingBodyPct,
    },
    candleConfirmation: pushResult ? pushResult.candle : null,
    candleConfirmationBodyPercentage: pushResult ? pushResult.bodyPercentage : undefined,
  };

  return addRetestAndVshape(allCandles, baseResult, direction === 'above' ? 'bullish' : 'bearish');
}

// ───────────────────────────────────────────────────────────────
// Doji
// ───────────────────────────────────────────────────────────────

const DOJI_DEFAULTS = {
  bodyRatio:     0.1,
  minWickRatio:  0.2,
};

function isDoji(candle, opts = {}) {
  const { bodyRatio, minWickRatio } = { ...DOJI_DEFAULTS, ...opts };
  const range = totalRange(candle);
  if (range <= 0) return false;
  const body = bodySize(candle);
  if (body / range > bodyRatio) return false;
  const up = upperWick(candle);
  const lo = lowerWick(candle);
  if (up <= 0 || lo <= 0) return false;
  if (up / range < minWickRatio) return false;
  if (lo / range < minWickRatio) return false;
  return true;
}

function describeDoji(candle, fallbackIndex, allCandles, firstIdx) {
  const range = totalRange(candle);
  const body  = bodySize(candle);
  const up    = upperWick(candle);
  const lo    = lowerWick(candle);
  const result = buildNeutralBreakout([candle], allCandles, firstIdx);

  return {
    pattern: 'doji',
    patternLength: 1,
    meaning: 'Indecision — buyers and sellers are balanced; momentum may be fading.',
    candle:  candleRef(candle, fallbackIndex),
    metrics: {
      range,
      body,
      upperWick:      up,
      lowerWick:      lo,
      bodyToRange:    +(body / range).toFixed(4),
      upperToRange:   +(up   / range).toFixed(4),
      lowerToRange:   +(lo   / range).toFixed(4),
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Hammer
// ───────────────────────────────────────────────────────────────

const HAMMER_DEFAULTS = {
  wickToBody:         2,
  maxUpperWickRatio:  0.15,
  lowerBodyShareMin:  0.55,
  requireDowntrend:   true,
  lookback:           5,
};

function isDowntrendBefore(candles, i, lookback) {
  if (i < lookback) return false;
  const start = i - lookback;
  const first = candles[start];
  const prev  = candles[i - 1];
  if (first.close <= prev.close) return false;
  let lowestLow = candles[i].low;
  for (let k = start; k <= i; k++) {
    if (candles[k].low < lowestLow) return false;
  }
  return true;
}

function isUptrendBefore(candles, i, lookback) {
  if (i < lookback) return false;
  const start = i - lookback;
  const first = candles[start];
  const prev  = candles[i - 1];
  if (first.close >= prev.close) return false;
  const candleHigh = candles[i].high;
  for (let k = start; k <= i; k++) {
    if (candles[k].high > candleHigh) return false;
  }
  return true;
}

function isHammerShape(candle, opts = {}) {
  const { wickToBody, maxUpperWickRatio, lowerBodyShareMin } =
    { ...HAMMER_DEFAULTS, ...opts };
  const range = totalRange(candle);
  if (range <= 0) return false;
  const body = bodySize(candle);
  if (body <= 0) return false;
  const up = upperWick(candle);
  const lo = lowerWick(candle);
  if (lo < wickToBody * body)         return false;
  if (up > maxUpperWickRatio * range) return false;
  const bodyBottomFromLow = Math.min(candle.open, candle.close) - candle.low;
  if (bodyBottomFromLow / range < lowerBodyShareMin) return false;
  return true;
}

function isHammer(candles, i, opts = {}) {
  const merged = { ...HAMMER_DEFAULTS, ...opts };
  const candle = Array.isArray(candles) ? candles[i] : candles;
  if (!candle) return false;
  if (!isHammerShape(candle, merged)) return false;
  if (merged.requireDowntrend) {
    if (!Array.isArray(candles)) return false;
    if (!isDowntrendBefore(candles, i, merged.lookback)) return false;
  }
  return true;
}

function describeHammer(candles, i, opts = {}) {
  const merged = { ...HAMMER_DEFAULTS, ...opts };
  const candle = candles[i];
  const range  = totalRange(candle);
  const body   = bodySize(candle);
  const up     = upperWick(candle);
  const lo     = lowerWick(candle);
  const downtrend = Array.isArray(candles) ? isDowntrendBefore(candles, i, merged.lookback) : false;
  const result = buildBreakout([candle], 'bullish', candles, i + 1);

  return {
    pattern: 'hammer',
    patternLength: 1,
    meaning: 'Bullish reversal — sellers drove price down but buyers closed it back near the high.',
    candle:  candleRef(candle, i),
    metrics: {
      range,
      body,
      upperWick:    up,
      lowerWick:    lo,
      wickToBody:   +(lo / body).toFixed(4),
      bodyToRange:  +(body / range).toFixed(4),
      upperToRange: +(up   / range).toFixed(4),
      lowerToRange: +(lo   / range).toFixed(4),
    },
    context: {
      downtrend,
      lookback: merged.lookback,
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Hanging Man
// ───────────────────────────────────────────────────────────────

const HANGING_MAN_DEFAULTS = {
  ...HAMMER_DEFAULTS,
  requireDowntrend: false,
  requireUptrend:   true,
};

function isHangingMan(candles, i, opts = {}) {
  const merged = { ...HANGING_MAN_DEFAULTS, ...opts };
  const candle = Array.isArray(candles) ? candles[i] : candles;
  if (!candle) return false;
  if (!isHammerShape(candle, merged)) return false;
  if (merged.requireUptrend) {
    if (!Array.isArray(candles)) return false;
    if (!isUptrendBefore(candles, i, merged.lookback)) return false;
  }
  return true;
}

function describeHangingMan(candles, i, opts = {}) {
  const merged = { ...HANGING_MAN_DEFAULTS, ...opts };
  const candle = candles[i];
  const range  = totalRange(candle);
  const body   = bodySize(candle);
  const up     = upperWick(candle);
  const lo     = lowerWick(candle);
  const uptrend = Array.isArray(candles) ? isUptrendBefore(candles, i, merged.lookback) : false;
  const result = buildBreakout([candle], 'bearish', candles, i + 1);

  return {
    pattern: 'hangingMan',
    patternLength: 1,
    meaning: 'Bearish reversal — long lower wick after a rally signals that sellers are starting to push back.',
    candle:  candleRef(candle, i),
    metrics: {
      range,
      body,
      upperWick:    up,
      lowerWick:    lo,
      wickToBody:   +(lo / body).toFixed(4),
      bodyToRange:  +(body / range).toFixed(4),
      upperToRange: +(up   / range).toFixed(4),
      lowerToRange: +(lo   / range).toFixed(4),
    },
    context: {
      uptrend,
      lookback: merged.lookback,
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Shooting Star
// ───────────────────────────────────────────────────────────────

const SHOOTING_STAR_DEFAULTS = {
  wickToBody:         2,
  maxLowerWickRatio:  0.15,
  upperBodyShareMin:  0.55,
  requireUptrend:     true,
  lookback:           5,
};

function isShootingStarShape(candle, opts = {}) {
  const { wickToBody, maxLowerWickRatio, upperBodyShareMin } =
    { ...SHOOTING_STAR_DEFAULTS, ...opts };
  const range = totalRange(candle);
  if (range <= 0) return false;
  const body = bodySize(candle);
  if (body <= 0) return false;
  const up = upperWick(candle);
  const lo = lowerWick(candle);
  if (up < wickToBody * body)         return false;
  if (lo > maxLowerWickRatio * range) return false;
  const bodyTopFromHigh = candle.high - Math.max(candle.open, candle.close);
  if (bodyTopFromHigh / range < upperBodyShareMin) return false;
  return true;
}

function isShootingStar(candles, i, opts = {}) {
  const merged = { ...SHOOTING_STAR_DEFAULTS, ...opts };
  const candle = Array.isArray(candles) ? candles[i] : candles;
  if (!candle) return false;
  if (!isShootingStarShape(candle, merged)) return false;
  if (merged.requireUptrend) {
    if (!Array.isArray(candles)) return false;
    if (!isUptrendBefore(candles, i, merged.lookback)) return false;
  }
  return true;
}

function describeShootingStar(candles, i, opts = {}) {
  const merged = { ...SHOOTING_STAR_DEFAULTS, ...opts };
  const candle = candles[i];
  const range  = totalRange(candle);
  const body   = bodySize(candle);
  const up     = upperWick(candle);
  const lo     = lowerWick(candle);
  const uptrend = Array.isArray(candles) ? isUptrendBefore(candles, i, merged.lookback) : false;
  const result = buildBreakout([candle], 'bearish', candles, i + 1);

  return {
    pattern: 'shootingStar',
    patternLength: 1,
    meaning: 'Bearish reversal — price rallied hard but collapsed back, handing control to sellers.',
    candle:  candleRef(candle, i),
    metrics: {
      range,
      body,
      upperWick:    up,
      lowerWick:    lo,
      wickToBody:   +(up / body).toFixed(4),
      bodyToRange:  +(body / range).toFixed(4),
      upperToRange: +(up   / range).toFixed(4),
      lowerToRange: +(lo   / range).toFixed(4),
    },
    context: {
      uptrend,
      lookback: merged.lookback,
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Marubozu
// ───────────────────────────────────────────────────────────────

const MARUBOZU_DEFAULTS = {
  minBodyRatio: 0.95,
  maxWickRatio: 0.025,
};

function isMarubozu(candle, opts = {}) {
  const { minBodyRatio, maxWickRatio } = { ...MARUBOZU_DEFAULTS, ...opts };
  const range = totalRange(candle);
  if (range <= 0) return false;
  const body = bodySize(candle);
  if (body <= 0) return false;
  if (body / range < minBodyRatio) return false;
  const up = upperWick(candle);
  const lo = lowerWick(candle);
  if (up / range > maxWickRatio) return false;
  if (lo / range > maxWickRatio) return false;
  return true;
}

function describeMarubozu(candle, fallbackIndex, allCandles, firstIdx) {
  const range = totalRange(candle);
  const body  = bodySize(candle);
  const up    = upperWick(candle);
  const lo    = lowerWick(candle);
  const direction = candle.close > candle.open ? 'bullish' : 'bearish';
  const meaning = direction === 'bullish'
    ? 'Bullish momentum — buyers controlled the candle from open to close with no pushback.'
    : 'Bearish momentum — sellers controlled the candle from open to close with no pushback.';
  const result = buildBreakout([candle], direction, allCandles, firstIdx);

  return {
    pattern: 'marubozu',
    patternLength: 1,
    direction,
    meaning,
    candle:  candleRef(candle, fallbackIndex),
    metrics: {
      range,
      body,
      upperWick:    up,
      lowerWick:    lo,
      bodyToRange:  +(body / range).toFixed(4),
      upperToRange: +(up   / range).toFixed(4),
      lowerToRange: +(lo   / range).toFixed(4),
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Spinning Top
// ───────────────────────────────────────────────────────────────

const SPINNING_TOP_DEFAULTS = {
  bodyMinRatio:      0.10,
  bodyMaxRatio:      0.35,
  minWickRatio:      0.25,
  wickBalanceRatio:  0.20,
};

function isSpinningTop(candle, opts = {}) {
  const { bodyMinRatio, bodyMaxRatio, minWickRatio, wickBalanceRatio } =
    { ...SPINNING_TOP_DEFAULTS, ...opts };
  const range = totalRange(candle);
  if (range <= 0) return false;
  const body = bodySize(candle);
  if (body <= 0) return false;
  const bodyShare = body / range;
  if (bodyShare <= bodyMinRatio) return false;
  if (bodyShare >  bodyMaxRatio) return false;
  const up = upperWick(candle);
  const lo = lowerWick(candle);
  if (up / range < minWickRatio) return false;
  if (lo / range < minWickRatio) return false;
  if (Math.abs(up - lo) / range > wickBalanceRatio) return false;
  return true;
}

function describeSpinningTop(candle, fallbackIndex, allCandles, firstIdx) {
  const range = totalRange(candle);
  const body  = bodySize(candle);
  const up    = upperWick(candle);
  const lo    = lowerWick(candle);
  const result = buildNeutralBreakout([candle], allCandles, firstIdx);

  return {
    pattern: 'spinningTop',
    patternLength: 1,
    meaning: 'Indecision — small body with balanced wicks suggests the market is consolidating.',
    candle:  candleRef(candle, fallbackIndex),
    metrics: {
      range,
      body,
      upperWick:      up,
      lowerWick:      lo,
      bodyToRange:    +(body / range).toFixed(4),
      upperToRange:   +(up   / range).toFixed(4),
      lowerToRange:   +(lo   / range).toFixed(4),
      wickImbalance:  +(Math.abs(up - lo) / range).toFixed(4),
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Engulfing (Bullish / Bearish)
// ───────────────────────────────────────────────────────────────

const ENGULFING_DEFAULTS = {
  minEngulfRatio:   1.0,
  minPrevBodyRatio: 0.10,
  lookback:         5,
};

function engulfingDirection(prev, curr, opts = {}) {
  const { minEngulfRatio, minPrevBodyRatio } = { ...ENGULFING_DEFAULTS, ...opts };
  const prevRange = totalRange(prev);
  const prevBody  = bodySize(prev);
  const currBody  = bodySize(curr);
  if (prevRange <= 0 || prevBody <= 0 || currBody <= 0) return null;
  if (prevBody / prevRange < minPrevBodyRatio) return null;
  if (currBody < minEngulfRatio * prevBody)    return null;
  const prevBullish = prev.close > prev.open;
  const prevBearish = prev.close < prev.open;
  const currBullish = curr.close > curr.open;
  const currBearish = curr.close < curr.open;
  if (prevBearish && currBullish &&
      curr.open  <= prev.close &&
      curr.close >= prev.open) {
    return 'bullish';
  }
  if (prevBullish && currBearish &&
      curr.open  >= prev.close &&
      curr.close <= prev.open) {
    return 'bearish';
  }
  return null;
}

function isEngulfing(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 1) return false;
  return engulfingDirection(candles[i - 1], candles[i], opts) !== null;
}

function describeEngulfing(candles, i, opts = {}) {
  const merged = { ...ENGULFING_DEFAULTS, ...opts };
  const prev = candles[i - 1];
  const curr = candles[i];
  const direction = engulfingDirection(prev, curr, merged);
  if (!direction) return null;
  const prevBody   = bodySize(prev);
  const currBody   = bodySize(curr);
  const context = {
    downtrend: isDowntrendBefore(candles, i - 1, merged.lookback),
    uptrend:   isUptrendBefore(candles, i - 1, merged.lookback),
    lookback:  merged.lookback,
  };
  const meaning = direction === 'bullish'
    ? 'Bullish reversal — a bullish candle fully engulfs the prior bearish body; buyers seized control.'
    : 'Bearish reversal — a bearish candle fully engulfs the prior bullish body; sellers seized control.';
  const result = buildBreakout([prev, curr], direction, candles, i + 1);

  return {
    pattern: direction === 'bullish' ? 'bullishEngulfing' : 'bearishEngulfing',
    patternLength: 2,
    direction,
    meaning,
    candles: {
      previous: candleRef(prev, i - 1),
      current:  candleRef(curr, i),
    },
    metrics: {
      prevBody,
      currBody,
      engulfRatio: +(currBody / prevBody).toFixed(4),
    },
    context,
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Tweezer Tops / Bottoms
// ───────────────────────────────────────────────────────────────

const TWEEZER_DEFAULTS = {
  levelTolerance: 0.05,
  lookback:       5,
};

function tweezerDirection(prev, curr, opts = {}) {
  const { levelTolerance } = { ...TWEEZER_DEFAULTS, ...opts };
  const prevRange = totalRange(prev);
  const currRange = totalRange(curr);
  if (prevRange <= 0 || currRange <= 0) return null;
  const avgRange = (prevRange + currRange) / 2;
  const prevBullish = prev.close > prev.open;
  const prevBearish = prev.close < prev.open;
  const currBullish = curr.close > curr.open;
  const currBearish = curr.close < curr.open;
  const highsMatch = Math.abs(prev.high - curr.high) / avgRange <= levelTolerance;
  const lowsMatch  = Math.abs(prev.low  - curr.low)  / avgRange <= levelTolerance;
  if (prevBullish && currBearish && highsMatch) return 'top';
  if (prevBearish && currBullish && lowsMatch)  return 'bottom';
  return null;
}

function isTweezer(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 1) return false;
  return tweezerDirection(candles[i - 1], candles[i], opts) !== null;
}

function describeTweezer(candles, i, opts = {}) {
  const merged = { ...TWEEZER_DEFAULTS, ...opts };
  const prev = candles[i - 1];
  const curr = candles[i];
  const kind = tweezerDirection(prev, curr, merged);
  if (!kind) return null;
  const prevRange  = totalRange(prev);
  const currRange  = totalRange(curr);
  const avgRange   = (prevRange + currRange) / 2;
  const level = kind === 'top'
    ? (prev.high + curr.high) / 2
    : (prev.low  + curr.low)  / 2;
  const diff = kind === 'top'
    ? Math.abs(prev.high - curr.high)
    : Math.abs(prev.low  - curr.low);
  const patternDirection = kind === 'top' ? 'bearish' : 'bullish';
  const result = buildBreakout([prev, curr], patternDirection, candles, i + 1);

  return {
    pattern:   kind === 'top' ? 'tweezerTop' : 'tweezerBottom',
    patternLength: 2,
    direction: patternDirection,
    meaning:   kind === 'top'
      ? 'Bearish reversal — two candles rejected the same high, sellers defending the level.'
      : 'Bullish reversal — two candles rejected the same low, buyers defending the level.',
    candles: {
      previous: candleRef(prev, i - 1),
      current:  candleRef(curr, i),
    },
    metrics: {
      level:          +level.toFixed(6),
      levelDiff:      +diff.toFixed(6),
      levelDiffRatio: +(diff / avgRange).toFixed(4),
    },
    context: {
      downtrend: isDowntrendBefore(candles, i - 1, merged.lookback),
      uptrend:   isUptrendBefore(candles, i - 1, merged.lookback),
      lookback:  merged.lookback,
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Piercing Line / Dark Cloud Cover
// ───────────────────────────────────────────────────────────────

const PIERCING_DEFAULTS = {
  minPrevBodyRatio: 0.3,
  lookback:         5,
};

function bodyMidpoint(c) { return (c.open + c.close) / 2; }

function isPiercingLine(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 1) return false;
  const { minPrevBodyRatio } = { ...PIERCING_DEFAULTS, ...opts };
  const prev = candles[i - 1];
  const curr = candles[i];
  const prevRange = totalRange(prev);
  const prevBody  = bodySize(prev);
  if (prevRange <= 0 || prevBody <= 0) return false;
  if (prevBody / prevRange < minPrevBodyRatio) return false;
  if (prev.close >= prev.open) return false;
  if (curr.close <= curr.open) return false;
  if (!(curr.open  <  prev.low))           return false;
  if (!(curr.close >  bodyMidpoint(prev))) return false;
  if (!(curr.close <  prev.open))          return false;
  return true;
}

function isDarkCloudCover(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 1) return false;
  const { minPrevBodyRatio } = { ...PIERCING_DEFAULTS, ...opts };
  const prev = candles[i - 1];
  const curr = candles[i];
  const prevRange = totalRange(prev);
  const prevBody  = bodySize(prev);
  if (prevRange <= 0 || prevBody <= 0) return false;
  if (prevBody / prevRange < minPrevBodyRatio) return false;
  if (prev.close <= prev.open) return false;
  if (curr.close >= curr.open) return false;
  if (!(curr.open  >  prev.high))          return false;
  if (!(curr.close <  bodyMidpoint(prev))) return false;
  if (!(curr.close >  prev.open))          return false;
  return true;
}

function describePiercing(candles, i, kind, opts = {}) {
  const merged = { ...PIERCING_DEFAULTS, ...opts };
  const prev = candles[i - 1];
  const curr = candles[i];
  const penetration = kind === 'piercing'
    ? (curr.close - prev.close) / (prev.open - prev.close)
    : (prev.close - curr.close) / (prev.close - prev.open);
  const patternDirection = kind === 'piercing' ? 'bullish' : 'bearish';
  const result = buildBreakout([prev, curr], patternDirection, candles, i + 1);

  return {
    pattern:   kind === 'piercing' ? 'piercingLine' : 'darkCloudCover',
    patternLength: 2,
    direction: patternDirection,
    meaning:   kind === 'piercing'
      ? 'Bullish reversal — gap-down bullish candle pushed past the midpoint of the prior bearish body.'
      : 'Bearish reversal — gap-up bearish candle pushed past the midpoint of the prior bullish body.',
    candles: {
      previous: candleRef(prev, i - 1),
      current:  candleRef(curr, i),
    },
    metrics: {
      prevMidpoint: +bodyMidpoint(prev).toFixed(6),
      penetration:  +penetration.toFixed(4),
    },
    context: {
      downtrend: isDowntrendBefore(candles, i - 1, merged.lookback),
      uptrend:   isUptrendBefore(candles, i - 1, merged.lookback),
      lookback:  merged.lookback,
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Morning Star / Evening Star
//
// The scanner runs EXACTLY ONE of these two pairs — never both:
//
//   opts.star.invertedAxis = false (default) → STANDARD pair
//     GREEN candle = close > open  |  RED candle = close < open
//     isMorningStar      : c1 RED,   c3 GREEN  (bullish reversal after downtrend)
//     isEveningStar      : c1 GREEN, c3 RED    (bearish reversal after uptrend)
//
//   opts.star.invertedAxis = true → INVERTED pair
//     On instruments where a LOWER number = a HIGHER displayed price:
//     GREEN candle = close < open  |  RED candle = close > open
//     isMorningStarInverted : c1 RED,   c3 GREEN  (bullish reversal)
//     isEveningStarInverted : c1 GREEN, c3 RED    (bearish reversal)
//
// WHY they must never run together:
//   standard morning  checks c1.close < c1.open  (c1 RED)
//   inverted evening  checks c1.close < c1.open  (c1 GREEN on inv axis)
//   → identical numeric condition, different meaning → cross-contamination
//   A normal Morning Star would be labelled as an Evening Star if both ran.
//
// Rule enforced at the pattern level:
//   c1 GREEN on screen → Evening Star only  (never Morning Star)
//   c1 RED   on screen → Morning Star only  (never Evening Star)
//
// Inverted-axis star body ratio is relaxed to 0.65 (vs 0.30 standard) because
// on negated instruments the middle candle body/range ratio sits around 0.60
// and is still genuinely the smaller candle between two large ones.
// ───────────────────────────────────────────────────────────────

const STAR_DEFAULTS = {
  largeBodyRatio:         0.6,
  starBodyRatio:          0.3,
  starBodyRatioInverted:  0.65,   // relaxed threshold for inverted-axis star candle
  closeIntoBody:          0.5,
  lookback:               5,
};

function isLargeBody(c, ratio) {
  const r = totalRange(c);
  const b = bodySize(c);
  return r > 0 && b > 0 && b / r >= ratio;
}

function isSmallBody(c, ratio) {
  const r = totalRange(c);
  const b = bodySize(c);
  return r > 0 && b / r <= ratio;
}

// ─── Standard (normal axis) ───────────────────────────────────

function isMorningStar(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { largeBodyRatio, starBodyRatio, closeIntoBody } = { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];

  if (!(c1.close < c1.open)) return false;                          // c1 bearish (red)
  if (!isLargeBody(c1, largeBodyRatio)) return false;
  if (!isSmallBody(c2, starBodyRatio)) return false;
  if (!(Math.max(c2.open, c2.close) < c1.close)) return false;     // star gapped down
  if (!(c3.close > c3.open)) return false;                          // c3 bullish (green)
  if (!isLargeBody(c3, largeBodyRatio)) return false;
  const threshold = c1.close + (c1.open - c1.close) * closeIntoBody;
  if (!(c3.close > threshold)) return false;
  return true;
}

function isEveningStar(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { largeBodyRatio, starBodyRatio, closeIntoBody } = { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];

  if (!(c1.close > c1.open)) return false;                          // c1 bullish (green)
  if (!isLargeBody(c1, largeBodyRatio)) return false;
  if (!isSmallBody(c2, starBodyRatio)) return false;
  if (!(Math.min(c2.open, c2.close) > c1.close)) return false;     // star gapped up
  if (!(c3.close < c3.open)) return false;                          // c3 bearish (red)
  if (!isLargeBody(c3, largeBodyRatio)) return false;
  const threshold = c1.close - (c1.close - c1.open) * closeIntoBody;
  if (!(c3.close < threshold)) return false;
  return true;
}

// ─── Inverted-axis variants ───────────────────────────────────
//
// Evening Star on inverted axis:
//   c1  large GREEN  → close < open  (numerically fell = visually rose)
//   c2  small star   → entire body numerically below c1.close
//                      (max(open,close) < c1.close  = visually gapped up)
//   c3  large RED    → close > open  (numerically rose = visually fell)
//                      closes numerically above the 50% threshold of c1's body
//
// Morning Star on inverted axis: exact mirror.

function isEveningStarInverted(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { largeBodyRatio, starBodyRatioInverted, closeIntoBody } =
    { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];

  // c1: large GREEN candle on inverted axis → close < open
  if (!(c1.close < c1.open)) return false;
  if (!isLargeBody(c1, largeBodyRatio)) return false;

  // c2: star — body is small relative to its own range
  if (!isSmallBody(c2, starBodyRatioInverted)) return false;

  // c2 gapped up visually = the entire body is numerically BELOW c1.close
  // (max of open/close is still less than c1.close)
  if (!(Math.max(c2.open, c2.close) < c1.close)) return false;

  // c3: large RED candle on inverted axis → close > open
  if (!(c3.close > c3.open)) return false;
  if (!isLargeBody(c3, largeBodyRatio)) return false;

  // c3 closes well into c1's body.
  // c1 body: from c1.close (numerically low end) to c1.open (numerically high end).
  // Threshold = c1.close + closeIntoBody * (c1.open - c1.close).
  // c3.close must be numerically above this threshold (deeper into the body).
  const threshold = c1.close + (c1.open - c1.close) * closeIntoBody;
  if (!(c3.close > threshold)) return false;

  return true;
}

function isMorningStarInverted(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { largeBodyRatio, starBodyRatioInverted, closeIntoBody } =
    { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];

  // c1: large RED candle on inverted axis → close > open
  if (!(c1.close > c1.open)) return false;
  if (!isLargeBody(c1, largeBodyRatio)) return false;

  // c2: star — body is small relative to its own range
  if (!isSmallBody(c2, starBodyRatioInverted)) return false;

  // c2 gapped down visually = the entire body is numerically ABOVE c1.close
  // (min of open/close is still greater than c1.close)
  if (!(Math.min(c2.open, c2.close) > c1.close)) return false;

  // c3: large GREEN candle on inverted axis → close < open
  if (!(c3.close < c3.open)) return false;
  if (!isLargeBody(c3, largeBodyRatio)) return false;

  // c3 closes well into c1's body.
  // c1 body: from c1.open (numerically low end) to c1.close (numerically high end).
  // Threshold = c1.close - closeIntoBody * (c1.close - c1.open).
  // c3.close must be numerically below this threshold (deeper into the body).
  const threshold = c1.close - (c1.close - c1.open) * closeIntoBody;
  if (!(c3.close < threshold)) return false;

  return true;
}

// ───────────────────────────────────────────────────────────────
// Evening Star Scenario  (bearish reversal — standalone pattern)
//
// Conditions (standard axis: GREEN = close > open, RED = close < open):
//   c1: large GREEN candle  → c1.close > c1.open
//   c2: small star body     → body/range <= 0.65
//                             min(c2.open, c2.close) > c1.close  (star gapped above)
//   c3: large RED candle    → c3.close < c3.open
//                             c3.close < c1.close - (c1.close - c1.open) * 0.5
//                             (c3 closes at least 50% into c1 body)
// ───────────────────────────────────────────────────────────────

const EVENING_STAR_SCENARIO_DEFAULTS = {
  largeBodyRatio:        0.6,
  starBodyRatioScenario: 0.65,
  closeIntoBody:         0.5,
  lookback:              5,
};

function isEveningStarScenario(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { largeBodyRatio, starBodyRatioScenario, closeIntoBody } =
    { ...EVENING_STAR_SCENARIO_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];

  // c1 — large GREEN candle
  if (!(c1.close > c1.open)) return false;
  if (!isLargeBody(c1, largeBodyRatio)) return false;

  // c2 — star: small body, sits above c1.close
  if (!isSmallBody(c2, starBodyRatioScenario)) return false;
  if (!(Math.min(c2.open, c2.close) > c1.close)) return false;

  // c3 — large RED candle, closes at least 50% into c1 body
  if (!(c3.close < c3.open)) return false;
  if (!isLargeBody(c3, largeBodyRatio)) return false;
  const threshold = c1.close - (c1.close - c1.open) * closeIntoBody;
  if (!(c3.close < threshold)) return false;

  return true;
}

function describeEveningStarScenario(candles, i, opts = {}) {
  const merged = { ...EVENING_STAR_SCENARIO_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];
  const result = buildBreakout([c1, c2, c3], 'bearish', candles, i + 1);

  return {
    pattern:       'eveningStarScenario',
    patternLength: 3,
    direction:     'bearish',
    meaning:       'Bearish reversal — large green c1, small star c2 gapped above c1 close, large red c3 closing at least 50% into c1 body.',
    candles: {
      first: candleRef(c1, i - 2),
      star:  candleRef(c2, i - 1),
      third: candleRef(c3, i),
    },
    metrics: {
      firstBody:  bodySize(c1),
      starBody:   bodySize(c2),
      thirdBody:  bodySize(c3),
      threshold:  +(c1.close - (c1.close - c1.open) * merged.closeIntoBody).toFixed(6),
    },
    context: {
      downtrend: isDowntrendBefore(candles, i - 2, merged.lookback),
      uptrend:   isUptrendBefore(candles, i - 2, merged.lookback),
      lookback:  merged.lookback,
    },
    ...result,
  };
}

// ─── describe (shared for both axes) ─────────────────────────

function describeStar(candles, i, kind, opts = {}) {
  const merged = { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];
  const patternDirection = kind === 'morning' ? 'bullish' : 'bearish';
  const invertedAxis = !!merged.invertedAxis;
  const result = buildBreakout([c1, c2, c3], patternDirection, candles, i + 1);

  return {
    pattern:   kind === 'morning' ? 'morningStar' : 'eveningStar',
    patternLength: 3,
    direction: patternDirection,
    invertedAxis,
    meaning:   kind === 'morning'
      ? 'Strong bullish reversal — bearish candle, indecisive gap-down star, then a bullish candle closing well into the first body.'
      : 'Strong bearish reversal — bullish candle, indecisive gap-up star, then a bearish candle closing well into the first body.',
    candles: {
      first:  candleRef(c1, i - 2),
      star:   candleRef(c2, i - 1),
      third:  candleRef(c3, i),
    },
    metrics: {
      firstBody:  bodySize(c1),
      starBody:   bodySize(c2),
      thirdBody:  bodySize(c3),
    },
    context: {
      downtrend: isDowntrendBefore(candles, i - 2, merged.lookback),
      uptrend:   isUptrendBefore(candles, i - 2, merged.lookback),
      lookback:  merged.lookback,
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Three White Soldiers / Three Black Crows
// ───────────────────────────────────────────────────────────────

const TRIPLE_DEFAULTS = {
  minBodyRatio: 0.5,
  lookback:     5,
};

function isThreeWhiteSoldiers(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { minBodyRatio } = { ...TRIPLE_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];
  for (const c of [c1, c2, c3]) {
    if (!(c.close > c.open)) return false;
    if (!isLargeBody(c, minBodyRatio)) return false;
  }
  if (!(c2.open > c1.open && c2.open < c1.close)) return false;
  if (!(c3.open > c2.open && c3.open < c2.close)) return false;
  if (!(c2.close > c1.close)) return false;
  if (!(c3.close > c2.close)) return false;
  return true;
}

function isThreeBlackCrows(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { minBodyRatio } = { ...TRIPLE_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];
  for (const c of [c1, c2, c3]) {
    if (!(c.close < c.open)) return false;
    if (!isLargeBody(c, minBodyRatio)) return false;
  }
  if (!(c2.open < c1.open && c2.open > c1.close)) return false;
  if (!(c3.open < c2.open && c3.open > c2.close)) return false;
  if (!(c2.close < c1.close)) return false;
  if (!(c3.close < c2.close)) return false;
  return true;
}

function describeTriple(candles, i, kind, opts = {}) {
  const merged = { ...TRIPLE_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];
  const patternDirection = kind === 'soldiers' ? 'bullish' : 'bearish';
  const result = buildBreakout([c1, c2, c3], patternDirection, candles, i + 1);

  return {
    pattern:   kind === 'soldiers' ? 'threeWhiteSoldiers' : 'threeBlackCrows',
    patternLength: 3,
    direction: patternDirection,
    meaning:   kind === 'soldiers'
      ? 'Sustained uptrend — three bullish candles with progressively higher closes, each opening inside the prior body.'
      : 'Sustained downtrend — three bearish candles with progressively lower closes, each opening inside the prior body.',
    candles: {
      first:  candleRef(c1, i - 2),
      second: candleRef(c2, i - 1),
      third:  candleRef(c3, i),
    },
    metrics: {
      totalMove: kind === 'soldiers'
        ? +(c3.close - c1.open).toFixed(6)
        : +(c1.open  - c3.close).toFixed(6),
    },
    context: {
      downtrend: isDowntrendBefore(candles, i - 2, merged.lookback),
      uptrend:   isUptrendBefore(candles, i - 2, merged.lookback),
      lookback:  merged.lookback,
    },
    ...result,
  };
}

// ───────────────────────────────────────────────────────────────
// Scanner
// ───────────────────────────────────────────────────────────────

function findPatterns(candles, opts = {}) {
  if (!Array.isArray(candles) || candles.length === 0) return [];
  const results = [];

  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];

    if (isDoji(c, opts.doji)) {
      results.push(describeDoji(c, i, candles, i + 1));
    }
    if (isHammer(candles, i, opts.hammer)) {
      results.push(describeHammer(candles, i, opts.hammer));
    }
    if (isHangingMan(candles, i, opts.hangingMan)) {
      results.push(describeHangingMan(candles, i, opts.hangingMan));
    }
    if (isShootingStar(candles, i, opts.shootingStar)) {
      results.push(describeShootingStar(candles, i, opts.shootingStar));
    }
    if (isMarubozu(c, opts.marubozu)) {
      results.push(describeMarubozu(c, i, candles, i + 1));
    }
    if (isSpinningTop(c, opts.spinningTop)) {
      results.push(describeSpinningTop(c, i, candles, i + 1));
    }
    if (isEngulfing(candles, i, opts.engulfing)) {
      const detection = describeEngulfing(candles, i, opts.engulfing);
      if (detection) results.push(detection);
    }
    if (isTweezer(candles, i, opts.tweezer)) {
      const detection = describeTweezer(candles, i, opts.tweezer);
      if (detection) results.push(detection);
    }
    if (isPiercingLine(candles, i, opts.piercing)) {
      results.push(describePiercing(candles, i, 'piercing', opts.piercing));
    }
    if (isDarkCloudCover(candles, i, opts.piercing)) {
      results.push(describePiercing(candles, i, 'darkCloud', opts.piercing));
    }

    // ── Morning Star / Evening Star ──────────────────────────────
    // Both functions handle two candle shapes each — no flags needed.
    // isEveningStar and isMorningStar each contain a standard branch and
    // an alternate branch (covering instruments like negated-price futures).
    // The scanner simply calls both; at most one fires per window because
    // they are mutually exclusive by their c1+c2+c3 conditions combined.
    if (isMorningStar(candles, i, opts.star)) {
      results.push(describeStar(candles, i, 'morning', opts.star));
    }
    if (isEveningStar(candles, i, opts.star)) {
      results.push(describeStar(candles, i, 'evening', opts.star));
    }

    if (isEveningStarScenario(candles, i, opts.eveningStarScenario)) {
      results.push(describeEveningStarScenario(candles, i, opts.eveningStarScenario));
    }

    if (isThreeWhiteSoldiers(candles, i, opts.triple)) {
      results.push(describeTriple(candles, i, 'soldiers', opts.triple));
    }
    if (isThreeBlackCrows(candles, i, opts.triple)) {
      results.push(describeTriple(candles, i, 'crows', opts.triple));
    }
  }

  return results;
}

function findPatternsFor(symbol, granularity, opts = {}) {
  const candles = signalEngine.getCandles(symbol, granularity, true);
  return findPatterns(candles, opts);
}

module.exports = {
  // detection primitives
  isDoji,
  describeDoji,
  isHammer,
  isHammerShape,
  describeHammer,
  isHangingMan,
  describeHangingMan,
  isShootingStar,
  isShootingStarShape,
  describeShootingStar,
  isMarubozu,
  describeMarubozu,
  isSpinningTop,
  describeSpinningTop,
  isEngulfing,
  describeEngulfing,
  engulfingDirection,
  isTweezer,
  describeTweezer,
  tweezerDirection,
  isPiercingLine,
  isDarkCloudCover,
  describePiercing,
  isMorningStar,
  isMorningStarInverted,
  isEveningStar,
  isEveningStarInverted,
  describeStar,
  isEveningStarScenario,
  describeEveningStarScenario,
  isThreeWhiteSoldiers,
  isThreeBlackCrows,
  describeTriple,
  // scanners
  findPatterns,
  findPatternsFor,
  // helpers
  bodySize,
  totalRange,
  upperWick,
  lowerWick,
  isDowntrendBefore,
  isUptrendBefore,
  buildBreakout,
  buildNeutralBreakout,
};