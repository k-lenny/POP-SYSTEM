// server/src/signals/dataProcessor/candle.js
//
// Candle pattern detector.
// Pulls OHLC candles from signalEngine and scans them for classical
// single/multi-candle patterns. Currently implemented:
//   • Doji   — open and close are nearly identical; tiny body with wicks
//              on both sides. Signals indecision between buyers and sellers;
//              often warns that a trend is losing momentum.
//   • Hammer — small body at the top with a long lower wick (≥ 2× body).
//              Found at the bottom of a downtrend: sellers pushed price
//              down sharply but buyers fought back to close near the high.
//              A bullish reversal signal.
//   • Hanging Man — identical shape to the hammer but appears at the top
//              of an uptrend. The same long lower wick now warns that
//              selling pressure is emerging — a bearish reversal signal.
//   • Shooting Star — small body at the bottom with a long upper wick
//              (≥ 2× body). Appears at the top of an uptrend: price
//              rallied strongly intraday but collapsed back — bears took
//              control. A bearish reversal signal.
//   • Marubozu — a full body with no wicks. Shows complete dominance by
//              one side (bullish = pure buying pressure, bearish = pure
//              selling). A strong momentum signal in the direction of
//              the candle.
//   • Spinning Top — small body with roughly equal upper and lower wicks.
//              Like the doji, it signals indecision and potential
//              consolidation — but with a visible body.
//   • Engulfing (Bullish / Bearish) — a two-candle reversal where the
//              second candle's body completely swallows the previous
//              candle's body. Bullish engulfing: prior bearish candle
//              swallowed by a bullish one (buyers take control).
//              Bearish engulfing: prior bullish candle swallowed by a
//              bearish one (sellers take control). The most reliable
//              two-candle reversal — the larger the engulfing body
//              relative to the engulfed body, the stronger the signal.
//   • Tweezer Tops / Bottoms — two consecutive candles with matching
//              highs (tops, bearish) or matching lows (bottoms, bullish).
//              Shows price rejection at the shared level.
//   • Piercing Line — after a bearish candle, a bullish candle opens
//              below the prior low but closes above the midpoint of the
//              prior body. Bullish reversal.
//   • Dark Cloud Cover — opposite of piercing: a bullish candle followed
//              by a bearish candle that opens above the prior high and
//              closes below the midpoint. Bearish reversal.
//   • Morning Star — large bearish, small gap-down star, large bullish
//              closing well into the first body. Strong bullish reversal.
//   • Evening Star — mirror of morning star. Strong bearish reversal.
//   • Three White Soldiers — three consecutive bullish candles, each
//              opening within the prior body and closing progressively
//              higher. Sustained uptrend signal.
//   • Three Black Crows — three consecutive bearish candles, each
//              opening within the prior body and closing progressively
//              lower. Sustained downtrend signal.

const signalEngine = require('../signalEngine');

// ─────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────
// Doji
//
// Rules (configurable via opts):
//   1. The candle has a real range (high > low) — guards against flat candles.
//   2. Body is tiny relative to the full range:
//         bodySize / range  <=  bodyRatio       (default 0.1 → 10 %)
//   3. Both wicks exist and neither wick dominates the candle entirely:
//         upperWick  >  0
//         lowerWick  >  0
//      (optional) each wick is at least `minWickRatio` of the range
//         (default 0.2 → 20 %). This filters out dragonfly / gravestone
//         variants where one wick is effectively absent and keeps only
//         the "classic" indecision Doji.
// ─────────────────────────────────────────────────────────────────────────

const DOJI_DEFAULTS = {
  bodyRatio:     0.1,   // body ≤ 10 % of total range
  minWickRatio:  0.2,   // each wick ≥ 20 % of total range (classic doji)
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

function describeDoji(candle, fallbackIndex) {
  const range = totalRange(candle);
  const body  = bodySize(candle);
  const up    = upperWick(candle);
  const lo    = lowerWick(candle);

  return {
    pattern: 'doji',
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Hammer
//
// Shape rules (configurable via opts):
//   1. The candle has a real range (high > low) and a real body
//      (body > 0 — pure Doji-like bodies are rejected here).
//   2. Long lower wick: lowerWick >= wickToBody * body   (default 2×).
//   3. Tiny upper wick: upperWick <= maxUpperWickRatio * range
//      (default 0.15 → 15 %). A large upper wick turns the shape into an
//      "inverted" / indecision candle, not a hammer.
//   4. Body sits in the upper half of the candle:
//         bodyBottom - low   >=  lowerBodyShareMin * range   (default 0.55)
//      i.e. at least ~55 % of the candle's range is below the body.
//
// Context rule (optional, on by default):
//   5. The candle appears at the bottom of a downtrend — over the
//      previous `lookback` candles (default 5) the trend is down
//      (first close > last close) and the hammer's low is the lowest
//      low in that window (current candle included).
//      Toggle off with opts.requireDowntrend = false to get a pure
//      shape-only scan.
// ─────────────────────────────────────────────────────────────────────────

const HAMMER_DEFAULTS = {
  wickToBody:         2,      // lower wick ≥ 2× body
  maxUpperWickRatio:  0.15,   // upper wick ≤ 15 % of range
  lowerBodyShareMin:  0.55,   // bottom of body sits ≥ 55 % up from the low
  requireDowntrend:   true,
  lookback:           5,      // candles used for the downtrend check
};

function isDowntrendBefore(candles, i, lookback) {
  if (i < lookback) return false;

  const start = i - lookback;
  const first = candles[start];
  const prev  = candles[i - 1];
  if (first.close <= prev.close) return false;   // not trending down

  let lowestLow = candles[i].low;
  for (let k = start; k <= i; k++) {
    if (candles[k].low < lowestLow) return false; // hammer must own the low
  }
  return true;
}

function isUptrendBefore(candles, i, lookback) {
  if (i < lookback) return false;

  const start = i - lookback;
  const first = candles[start];
  const prev  = candles[i - 1];
  if (first.close >= prev.close) return false;   // not trending up

  const candleHigh = candles[i].high;
  for (let k = start; k <= i; k++) {
    if (candles[k].high > candleHigh) return false; // hanging man must own the high
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

  if (lo < wickToBody * body)        return false;
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
    if (!Array.isArray(candles)) return false; // cannot verify context
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

  const downtrend = Array.isArray(candles)
    ? isDowntrendBefore(candles, i, merged.lookback)
    : false;

  return {
    pattern: 'hammer',
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Hanging Man
//
// Same shape as a hammer (reuses `isHammerShape`) but appears at the TOP
// of an uptrend instead of the bottom of a downtrend. When the long lower
// wick shows up after a rally it signals that sellers have started to
// probe — a bearish reversal warning.
//
// Context rule (on by default):
//   Over the previous `lookback` candles the trend is up
//   (first close < last close) AND the hanging-man's high is the highest
//   high in that window. Disable with opts.requireUptrend = false for a
//   shape-only scan (redundant with the pure hammer scan).
// ─────────────────────────────────────────────────────────────────────────

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

  const uptrend = Array.isArray(candles)
    ? isUptrendBefore(candles, i, merged.lookback)
    : false;

  return {
    pattern: 'hangingMan',
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Shooting Star
//
// Mirror of the hammer: small body at the BOTTOM of the candle with a
// long UPPER wick. Appears after an uptrend — price rallied strongly
// intraday but closed back near the open, handing control to sellers.
//
// Shape rules (configurable via opts):
//   1. Real range (high > low) and real body (body > 0).
//   2. Long upper wick:  upperWick >= wickToBody * body   (default 2×).
//   3. Tiny lower wick:  lowerWick <= maxLowerWickRatio * range
//                        (default 0.15 → 15 %).
//   4. Body sits in the lower half of the candle:
//         high - bodyTop   >=  upperBodyShareMin * range   (default 0.55)
//      i.e. at least ~55 % of the candle's range is above the body.
//
// Context rule (optional, on by default):
//   5. Previous `lookback` candles trend up (first close < last close)
//      and the shooting star owns the highest high in that window.
//      Toggle off with opts.requireUptrend = false for a shape-only scan.
// ─────────────────────────────────────────────────────────────────────────

const SHOOTING_STAR_DEFAULTS = {
  wickToBody:         2,      // upper wick ≥ 2× body
  maxLowerWickRatio:  0.15,   // lower wick ≤ 15 % of range
  upperBodyShareMin:  0.55,   // top of body sits ≥ 55 % down from the high
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

  if (up < wickToBody * body)        return false;
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

  const uptrend = Array.isArray(candles)
    ? isUptrendBefore(candles, i, merged.lookback)
    : false;

  return {
    pattern: 'shootingStar',
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Marubozu
//
// A full-body candle with (near-)zero wicks. The side that owns the move
// owned it end-to-end — bullish Marubozu opens at the low and closes at
// the high, bearish Marubozu opens at the high and closes at the low.
//
// Rules (configurable via opts):
//   1. Real range (high > low) and a real body (body > 0).
//   2. Body dominates the range:  body / range >= minBodyRatio
//      (default 0.95 → body is ≥ 95 % of the candle).
//   3. Each wick is effectively absent:
//         upperWick / range <= maxWickRatio
//         lowerWick / range <= maxWickRatio
//      (default 0.025 → ≤ 2.5 % of range on each side).
//
// Direction:
//   • close > open → bullish Marubozu (buying pressure)
//   • close < open → bearish Marubozu (selling pressure)
// ─────────────────────────────────────────────────────────────────────────

const MARUBOZU_DEFAULTS = {
  minBodyRatio: 0.95,   // body ≥ 95 % of range
  maxWickRatio: 0.025,  // each wick ≤ 2.5 % of range
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

function describeMarubozu(candle, fallbackIndex) {
  const range = totalRange(candle);
  const body  = bodySize(candle);
  const up    = upperWick(candle);
  const lo    = lowerWick(candle);

  const direction = candle.close > candle.open ? 'bullish' : 'bearish';
  const meaning = direction === 'bullish'
    ? 'Bullish momentum — buyers controlled the candle from open to close with no pushback.'
    : 'Bearish momentum — sellers controlled the candle from open to close with no pushback.';

  return {
    pattern: 'marubozu',
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Spinning Top
//
// Small (but visible) body flanked by roughly equal wicks on both sides.
// Shares the indecision meaning of a Doji, but the real body means some
// directional conviction did take place — the market just didn't commit.
//
// Rules (configurable via opts):
//   1. Real range (high > low) and real body (body > 0).
//   2. Body is small but larger than a doji:
//         bodyMinRatio  <  bodySize / range  <=  bodyMaxRatio
//      (defaults: > 0.10 and ≤ 0.35 → keeps doji on one side and
//       solid-bodied candles on the other).
//   3. Both wicks exist and each is meaningful:
//         upperWick / range  >=  minWickRatio   (default 0.25)
//         lowerWick / range  >=  minWickRatio
//   4. Wicks are roughly balanced:
//         |upperWick - lowerWick| / range  <=  wickBalanceRatio
//      (default 0.20 → the two wicks are within 20 % of range of each
//       other). Rejects hammer / shooting-star shapes.
// ─────────────────────────────────────────────────────────────────────────

const SPINNING_TOP_DEFAULTS = {
  bodyMinRatio:      0.10,  // body > 10 % of range (bigger than doji)
  bodyMaxRatio:      0.35,  // body ≤ 35 % of range (still a small body)
  minWickRatio:      0.25,  // each wick ≥ 25 % of range
  wickBalanceRatio:  0.20,  // |up-lo| ≤ 20 % of range
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

function describeSpinningTop(candle, fallbackIndex) {
  const range = totalRange(candle);
  const body  = bodySize(candle);
  const up    = upperWick(candle);
  const lo    = lowerWick(candle);

  return {
    pattern: 'spinningTop',
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Engulfing  (Bullish / Bearish)
//
// Two-candle reversal pattern detected at the index of the SECOND
// (engulfing) candle. Only the BODIES are compared — wicks are ignored,
// matching the classical definition.
//
// Bullish engulfing:
//   • prev  candle is bearish  (close < open)
//   • curr  candle is bullish  (close > open)
//   • curr.open  <=  prev.close     // opens at/below prev body bottom
//   • curr.close >=  prev.open      // closes at/above prev body top
//
// Bearish engulfing:
//   • prev  candle is bullish  (close > open)
//   • curr  candle is bearish  (close < open)
//   • curr.open  >=  prev.close     // opens at/above prev body top
//   • curr.close <=  prev.open      // closes at/below prev body bottom
//
// Extra filters (configurable via opts):
//   • minEngulfRatio — the engulfing body must be at least this multiple
//     of the engulfed body (default 1.0 → just needs to cover it).
//     Raise to 1.5 / 2.0 to demand a visibly larger candle.
//   • minPrevBodyRatio — the engulfed body must be at least this share
//     of its own range (default 0.1 → ignore pure dojis, which are
//     trivially engulfed and produce noise).
//
// Context (informational, not filtered):
//   • Bullish engulfing is meaningful after a downtrend.
//   • Bearish engulfing is meaningful after an uptrend.
//   Both `downtrend` / `uptrend` flags are returned on the result so the
//   caller can weight the signal, but the pattern still fires without
//   the context — this matches how most books teach it.
// ─────────────────────────────────────────────────────────────────────────

const ENGULFING_DEFAULTS = {
  minEngulfRatio:   1.0,   // engulfing body ≥ 1× engulfed body
  minPrevBodyRatio: 0.10,  // engulfed body ≥ 10 % of its own range
  lookback:         5,     // for informational trend context
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

  const prevBody = bodySize(prev);
  const currBody = bodySize(curr);

  const context = {
    downtrend: isDowntrendBefore(candles, i - 1, merged.lookback),
    uptrend:   isUptrendBefore(candles, i - 1, merged.lookback),
    lookback:  merged.lookback,
  };

  const meaning = direction === 'bullish'
    ? 'Bullish reversal — a bullish candle fully engulfs the prior bearish body; buyers seized control.'
    : 'Bearish reversal — a bearish candle fully engulfs the prior bullish body; sellers seized control.';

  return {
    pattern: direction === 'bullish' ? 'bullishEngulfing' : 'bearishEngulfing',
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Tweezer Tops / Bottoms
//
// Two consecutive candles whose highs (tops) or lows (bottoms) match
// within a small tolerance. The matched level acts as rejection —
// tops are bearish, bottoms are bullish.
//
// Rules (configurable):
//   • levelTolerance — |a - b| / avgRange ≤ 0.05 (default). The two
//     levels are considered "matching" when they differ by ≤ 5 % of the
//     average candle range.
//   • Tops:    prev bullish, curr bearish, matching highs.
//   • Bottoms: prev bearish, curr bullish, matching lows.
//     The opposing-colour requirement keeps this distinct from ordinary
//     pauses in a trend where both candles agree.
// ─────────────────────────────────────────────────────────────────────────

const TWEEZER_DEFAULTS = {
  levelTolerance: 0.05,   // |a-b| ≤ 5 % of avg range
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

  const prevRange = totalRange(prev);
  const currRange = totalRange(curr);
  const avgRange  = (prevRange + currRange) / 2;

  const level = kind === 'top'
    ? (prev.high + curr.high) / 2
    : (prev.low  + curr.low)  / 2;

  const diff = kind === 'top'
    ? Math.abs(prev.high - curr.high)
    : Math.abs(prev.low  - curr.low);

  return {
    pattern: kind === 'top' ? 'tweezerTop' : 'tweezerBottom',
    direction: kind === 'top' ? 'bearish' : 'bullish',
    meaning: kind === 'top'
      ? 'Bearish reversal — two candles rejected the same high, sellers defending the level.'
      : 'Bullish reversal — two candles rejected the same low, buyers defending the level.',
    candles: {
      previous: candleRef(prev, i - 1),
      current:  candleRef(curr, i),
    },
    metrics: {
      level:        +level.toFixed(6),
      levelDiff:    +diff.toFixed(6),
      levelDiffRatio: +(diff / avgRange).toFixed(4),
    },
    context: {
      downtrend: isDowntrendBefore(candles, i - 1, merged.lookback),
      uptrend:   isUptrendBefore(candles, i - 1, merged.lookback),
      lookback:  merged.lookback,
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Piercing Line  /  Dark Cloud Cover
//
// Two-candle reversal where the second candle pushes deep into the first
// candle's body (past its midpoint) but does NOT fully engulf it.
//
// Piercing Line (bullish):
//   • prev bearish (close < open)
//   • curr bullish (close > open)
//   • curr.open  <  prev.low                 (opens below prior low)
//   • curr.close >  midpoint(prev)           (closes above prior mid)
//   • curr.close <  prev.open                (but not a full engulf)
//
// Dark Cloud Cover (bearish) — mirror:
//   • prev bullish, curr bearish
//   • curr.open  >  prev.high
//   • curr.close <  midpoint(prev)
//   • curr.close >  prev.open
// ─────────────────────────────────────────────────────────────────────────

const PIERCING_DEFAULTS = {
  minPrevBodyRatio: 0.3,   // engulfed body ≥ 30 % of its range (require a real candle)
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

  if (prev.close >= prev.open) return false;       // prev must be bearish
  if (curr.close <= curr.open) return false;       // curr must be bullish

  if (!(curr.open  <  prev.low))              return false;
  if (!(curr.close >  bodyMidpoint(prev)))    return false;
  if (!(curr.close <  prev.open))             return false; // not a full engulf

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

  if (prev.close <= prev.open) return false;       // prev must be bullish
  if (curr.close >= curr.open) return false;       // curr must be bearish

  if (!(curr.open  >  prev.high))             return false;
  if (!(curr.close <  bodyMidpoint(prev)))    return false;
  if (!(curr.close >  prev.open))             return false; // not a full engulf

  return true;
}

function describePiercing(candles, i, kind, opts = {}) {
  const merged = { ...PIERCING_DEFAULTS, ...opts };
  const prev = candles[i - 1];
  const curr = candles[i];

  const penetration = kind === 'piercing'
    ? (curr.close - prev.close) / (prev.open - prev.close)
    : (prev.close - curr.close) / (prev.close - prev.open);

  return {
    pattern:   kind === 'piercing' ? 'piercingLine' : 'darkCloudCover',
    direction: kind === 'piercing' ? 'bullish' : 'bearish',
    meaning:   kind === 'piercing'
      ? 'Bullish reversal — gap-down bullish candle pushed past the midpoint of the prior bearish body.'
      : 'Bearish reversal — gap-up bearish candle pushed past the midpoint of the prior bullish body.',
    candles: {
      previous: candleRef(prev, i - 1),
      current:  candleRef(curr, i),
    },
    metrics: {
      prevMidpoint: +bodyMidpoint(prev).toFixed(6),
      penetration:  +penetration.toFixed(4),   // share of prev body covered (0..1)
    },
    context: {
      downtrend: isDowntrendBefore(candles, i - 1, merged.lookback),
      uptrend:   isUptrendBefore(candles, i - 1, merged.lookback),
      lookback:  merged.lookback,
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Morning Star  /  Evening Star  (three-candle reversals)
//
// Morning Star (bullish):
//   C1: large bearish candle
//   C2: small-bodied "star" that gaps down vs C1 (C2 body top < C1 close)
//   C3: large bullish candle that closes well into C1's body
//
// Evening Star (bearish) — mirror:
//   C1: large bullish candle
//   C2: small "star" gapping up (C2 body bottom > C1 close)
//   C3: large bearish candle closing well into C1's body
//
// Configurable:
//   • largeBodyRatio — C1 / C3 body ≥ 60 % of their own range (default).
//   • starBodyRatio  — C2 body ≤ 30 % of its range (default).
//   • closeIntoBody  — C3 must close beyond `closeIntoBody` share of C1's
//     body (default 0.5 → past the midpoint, the classical requirement).
// ─────────────────────────────────────────────────────────────────────────

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

  if (!(c1.close < c1.open)) return false;                 // C1 bearish
  if (!isLargeBody(c1, largeBodyRatio)) return false;

  if (!isSmallBody(c2, starBodyRatio)) return false;
  if (Math.max(c2.open, c2.close) >= c1.close === false) {
    // star body top must be BELOW C1 close (gap down on bodies)
  }
  if (!(Math.max(c2.open, c2.close) < c1.close)) return false;

  if (!(c3.close > c3.open)) return false;                 // C3 bullish
  if (!isLargeBody(c3, largeBodyRatio)) return false;

  const c1Mid = bodyMidpoint(c1);
  const threshold = c1.close + (c1.open - c1.close) * closeIntoBody;
  // closeIntoBody=0.5 → threshold = midpoint; must close above it.
  if (!(c3.close > threshold)) return false;

  return true;
}

function isEveningStar(candles, i, opts = {}) {
  if (!Array.isArray(candles) || i < 2) return false;
  const { largeBodyRatio, starBodyRatio, closeIntoBody } = { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];

  if (!(c1.close > c1.open)) return false;                 // C1 bullish
  if (!isLargeBody(c1, largeBodyRatio)) return false;

  if (!isSmallBody(c2, starBodyRatio)) return false;
  if (!(Math.min(c2.open, c2.close) > c1.close)) return false;  // gap up on bodies

  if (!(c3.close < c3.open)) return false;                 // C3 bearish
  if (!isLargeBody(c3, largeBodyRatio)) return false;

  const threshold = c1.close - (c1.close - c1.open) * closeIntoBody;
  if (!(c3.close < threshold)) return false;

  return true;
}

function describeStar(candles, i, kind, opts = {}) {
  const merged = { ...STAR_DEFAULTS, ...opts };
  const c1 = candles[i - 2], c2 = candles[i - 1], c3 = candles[i];

  return {
    pattern:   kind === 'morning' ? 'morningStar' : 'eveningStar',
    direction: kind === 'morning' ? 'bullish' : 'bearish',
    meaning: kind === 'morning'
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Three White Soldiers  /  Three Black Crows
//
// Three consecutive candles of the same colour where each opens inside
// the prior body and closes progressively in the trend direction.
//
// Soldiers (bullish):
//   • All three bullish (close > open).
//   • Each body is "real" (body / range ≥ minBodyRatio, default 0.5).
//   • open[k]  within body of [k-1]: prev.open < open[k] < prev.close.
//   • close[k] > close[k-1] (higher highs on closes).
//
// Crows (bearish) — mirror rules.
// ─────────────────────────────────────────────────────────────────────────

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

  return {
    pattern:   kind === 'soldiers' ? 'threeWhiteSoldiers' : 'threeBlackCrows',
    direction: kind === 'soldiers' ? 'bullish' : 'bearish',
    meaning: kind === 'soldiers'
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
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Scanner — walk a candle array and collect every pattern match.
// Returns a flat array of detections ordered by candle index.
// ─────────────────────────────────────────────────────────────────────────

function findPatterns(candles, opts = {}) {
  if (!Array.isArray(candles) || candles.length === 0) return [];

  const results = [];

  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];

    if (isDoji(c, opts.doji)) {
      results.push(describeDoji(c, i));
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
      results.push(describeMarubozu(c, i));
    }

    if (isSpinningTop(c, opts.spinningTop)) {
      results.push(describeSpinningTop(c, i));
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

// Convenience: pull candles straight from signalEngine and scan them.
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
  // exposed helpers (useful when adding new patterns elsewhere)
  bodySize,
  totalRange,
  upperWick,
  lowerWick,
  isDowntrendBefore,
  isUptrendBefore,
};
