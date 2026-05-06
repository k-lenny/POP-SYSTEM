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
function totalRange(c) { return c.high - c.low; }
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

// ───────────────────────────────────────────────────────────────
// Breakout helpers
// ───────────────────────────────────────────────────────────────

function buildBreakout(patternCandles, direction, allCandles, firstIdx) {
  const level = direction === 'bullish'
    ? Math.max(...patternCandles.map(c => c.high))
    : Math.min(...patternCandles.map(c => c.low));

  const breakoutDirection = direction === 'bullish' ? 'above' : 'below';

  // No candles to scan
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
    };
  }

  // Find the breakout candle
  let confirmingCandle = null;
  let breakIdx = -1;
  for (let k = firstIdx; k < allCandles.length; k++) {
    const c = allCandles[k];
    const broke = breakoutDirection === 'above'
      ? (c.open > level || c.close > level)
      : (c.open < level || c.close < level);

    if (broke) {
      confirmingCandle = candleRef(c, k);
      breakIdx = k;
      break;
    }
  }

  const candlesChecked = breakIdx >= 0
    ? breakIdx - firstIdx + 1
    : allCandles.length - firstIdx;

  // Search for a second confirmation candle
  let candleConfirmation = null;
  let confCandlesChecked = 0;

  if (confirmingCandle && breakIdx + 1 < allCandles.length) {
    for (let k = breakIdx + 1; k < allCandles.length; k++) {
      confCandlesChecked++;
      const c = allCandles[k];
      const pushed = breakoutDirection === 'above'
        ? (c.open > confirmingCandle.high || c.close > confirmingCandle.high)
        : (c.open < confirmingCandle.low  || c.close < confirmingCandle.low);
      if (pushed) {
        candleConfirmation = candleRef(c, k);
        break;
      }
    }
  }

  // Calculate body percentages if candles exist
  const breakoutResult = {
    breakout: {
      level,
      direction: breakoutDirection,
      confirmed: !!confirmingCandle,
      confirmingCandle,
      candlesChecked,
      confirmationCandlesChecked: confCandlesChecked,
    },
    candleConfirmation,
  };

  // Add body percentage for confirming candle if it exists
  if (confirmingCandle) {
    try {
      breakoutResult.breakout.confirmingCandleBodyPercentage = calculateBodyPercentage({
        open: confirmingCandle.open,
        high: confirmingCandle.high,
        low: confirmingCandle.low,
        close: confirmingCandle.close
      });
    } catch (e) {
      // Silently fail if calculation fails
    }
  }

  // Add body percentage for candle confirmation if it exists
  if (candleConfirmation) {
    try {
      breakoutResult.candleConfirmationBodyPercentage = calculateBodyPercentage({
        open: candleConfirmation.open,
        high: candleConfirmation.high,
        low: candleConfirmation.low,
        close: candleConfirmation.close
      });
    } catch (e) {
      // Silently fail if calculation fails
    }
  }

  return breakoutResult;
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
    };
  }

  let confirmingCandle = null;
  let breakIdx = -1;
  let direction = 'above';
  for (let k = firstIdx; k < allCandles.length; k++) {
    const c = allCandles[k];
    const brokeUp   = c.open > bullLevel || c.close > bullLevel;
    const brokeDown = c.open < bearLevel || c.close < bearLevel;

    if (brokeUp || brokeDown) {
      direction = brokeUp ? 'above' : 'below';
      confirmingCandle = candleRef(c, k);
      breakIdx = k;
      break;
    }
  }

  const candlesChecked = breakIdx >= 0
    ? breakIdx - firstIdx + 1
    : allCandles.length - firstIdx;

  let candleConfirmation = null;
  let confCandlesChecked = 0;
  if (confirmingCandle && breakIdx + 1 < allCandles.length) {
    for (let k = breakIdx + 1; k < allCandles.length; k++) {
      confCandlesChecked++;
      const c = allCandles[k];
      const pushed = direction === 'above'
        ? (c.open > confirmingCandle.high || c.close > confirmingCandle.high)
        : (c.open < confirmingCandle.low  || c.close < confirmingCandle.low);
      if (pushed) {
        candleConfirmation = candleRef(c, k);
        break;
      }
    }
  }

  // Calculate body percentages if candles exist
  const breakoutResult = {
    breakout: {
      level: bullLevel,
      direction,
      confirmed: true,
      confirmingCandle,
      candlesChecked,
      confirmationCandlesChecked: confCandlesChecked,
      altLevel: bearLevel,
    },
    candleConfirmation,
  };

  // Add body percentage for confirming candle if it exists
  if (confirmingCandle) {
    try {
      breakoutResult.breakout.confirmingCandleBodyPercentage = calculateBodyPercentage({
        open: confirmingCandle.open,
        high: confirmingCandle.high,
        low: confirmingCandle.low,
        close: confirmingCandle.close
      });
    } catch (e) {
      // Silently fail if calculation fails
    }
  }

  // Add body percentage for candle confirmation if it exists
  if (candleConfirmation) {
    try {
      breakoutResult.candleConfirmationBodyPercentage = calculateBodyPercentage({
        open: candleConfirmation.open,
        high: candleConfirmation.high,
        low: candleConfirmation.low,
        close: candleConfirmation.close
      });
    } catch (e) {
      // Silently fail if calculation fails
    }
  }

  return breakoutResult;
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildNeutralBreakout([candle], allCandles, firstIdx);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([candle], 'bullish', candles, i + 1);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([candle], 'bearish', candles, i + 1);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const candle = candles[i];  // FIXED: typo removed
  const range  = totalRange(candle);
  const body   = bodySize(candle);
  const up     = upperWick(candle);
  const lo     = lowerWick(candle);
  const uptrend = Array.isArray(candles) ? isUptrendBefore(candles, i, merged.lookback) : false;
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([candle], 'bearish', candles, i + 1);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([candle], direction, allCandles, firstIdx);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildNeutralBreakout([candle], allCandles, firstIdx);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([prev, curr], direction, candles, i + 1);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([prev, curr], patternDirection, candles, i + 1);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([prev, curr], patternDirection, candles, i + 1);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
  };
}

// ───────────────────────────────────────────────────────────────
// Morning Star / Evening Star
// ───────────────────────────────────────────────────────────────

const STAR_DEFAULTS = {
  largeBodyRatio: 0.6,
  starBodyRatio:  0.3,
  closeIntoBody:  0.5,
  lookback:       5,
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

function isMorningStar(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { largeBodyRatio, starBodyRatio, closeIntoBody } = { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];
  if (!(c1.close < c1.open)) return false;
  if (!isLargeBody(c1, largeBodyRatio)) return false;
  if (!isSmallBody(c2, starBodyRatio)) return false;
  if (!(Math.max(c2.open, c2.close) < c1.close)) return false;
  if (!(c3.close > c3.open)) return false;
  if (!isLargeBody(c3, largeBodyRatio)) return false;
  const threshold = c1.close + (c1.open - c1.close) * closeIntoBody;
  if (!(c3.close > threshold)) return false;
  return true;
}

function isEveningStar(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { largeBodyRatio, starBodyRatio, closeIntoBody } = { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];
  if (!(c1.close > c1.open)) return false;
  if (!isLargeBody(c1, largeBodyRatio)) return false;
  if (!isSmallBody(c2, starBodyRatio)) return false;
  if (!(Math.min(c2.open, c2.close) > c1.close)) return false;
  if (!(c3.close < c3.open)) return false;
  if (!isLargeBody(c3, largeBodyRatio)) return false;
  const threshold = c1.close - (c1.close - c1.open) * closeIntoBody;
  if (!(c3.close < threshold)) return false;
  return true;
}

function describeStar(candles, i, kind, opts = {}) {
  const merged = { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];
  const patternDirection = kind === 'morning' ? 'bullish' : 'bearish';
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([c1, c2, c3], patternDirection, candles, i + 1);

  return {
    pattern:   kind === 'morning' ? 'morningStar' : 'eveningStar',
    patternLength: 3,
    direction: patternDirection,
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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
  const { breakout, candleConfirmation, candleConfirmationBodyPercentage } = buildBreakout([c1, c2, c3], patternDirection, candles, i + 1);

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
    breakout,
    candleConfirmation,
    ...(candleConfirmationBodyPercentage !== undefined && { candleConfirmationBodyPercentage }),
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
    if (isMorningStar(candles, i, opts.star)) {
      results.push(describeStar(candles, i, 'morning', opts.star));
    }
    if (isEveningStar(candles, i, opts.star)) {
      results.push(describeStar(candles, i, 'evening', opts.star));
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
  isEveningStar,
  describeStar,
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