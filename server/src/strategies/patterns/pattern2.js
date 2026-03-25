// pattern2.js
const EventEmitter = require('events');
const swingEngine = require('../../signals/dataProcessor/swings');
const Logger = require('../../utils/logger');
const { getConfig } = require('../../config');

class Pattern2Engine extends EventEmitter {
  constructor(options = {}) {
    super();
    this.store = {};
    this.logger = options.logger || new Logger('Pattern2Engine');
    this.emitEvents = options.emitEvents ?? getConfig().ENABLE_EVENTS;

    this.config = {
      minCandlesBetweenSwings: 3,
      retestScanRange: 7,
      equalLevelTolerance: 0.002,
      ...options.config
    };
  }

  _initStore(symbol, granularity) {
    if (!this.store[symbol]) this.store[symbol] = {};
    if (!this.store[symbol][granularity]) this.store[symbol][granularity] = [];
  }

  _enrichCandles(candles) {
    return candles.map((c, idx) => ({
      ...c,
      index: idx,
      bodySize: Math.abs(c.open - c.close),
      upperWick: c.high - Math.max(c.open, c.close),
      lowerWick: Math.min(c.open, c.close) - c.low
    }));
  }

  _formatTime(timestamp) {
    if (!timestamp) return null;
    const date = new Date(Number(timestamp) * 1000);
    return date.toISOString().replace('T', ' ').substring(0, 19);
  }

  _findPreviousSameTypeSwing(swings, currentIndex, type) {
    for (let i = currentIndex - 1; i >= 0; i--) {
      if (swings[i].type === type) return swings[i];
    }
    return null;
  }

  _getPatternDirection(currentSwing, previousSwing) {
    if (currentSwing.type === 'low' && previousSwing.type === 'low') return 'bullish';
    if (currentSwing.type === 'high' && previousSwing.type === 'high') return 'bearish';
    return null;
  }

  async detect(symbol, granularity, candles) {
    this._initStore(symbol, granularity);

    // Ensure swings are detected before proceeding (same import/usage as original)
    await swingEngine.detectAll(symbol, granularity, candles.slice(0, -1), 1);

    const swings = swingEngine.get(symbol, granularity) || [];
    this.logger.info(`[Pattern2Engine] Detecting patterns for ${symbol} ${granularity}`);
    this.logger.info(`[Pattern2Engine] Candles: ${candles.length}, Swings: ${swings.length}`);

    if (swings.length < 2) {
      this.logger.warn(`[Pattern2Engine] Not enough swings (${swings.length}) for pattern detection`);
      return [];
    }

    const enrichedCandles = this._enrichCandles(candles);
    const patterns = [];

    for (let i = 1; i < swings.length; i++) {
      const currentSwing = swings[i];
      const previousSwing = this._findPreviousSameTypeSwing(swings, i, currentSwing.type);
      if (!previousSwing) continue;

      const candlesBetween = currentSwing.index - previousSwing.index;
      if (candlesBetween < this.config.minCandlesBetweenSwings) continue;

      const direction = this._getPatternDirection(currentSwing, previousSwing);
      if (!direction) continue;

      // Minimal setup detection: check for wick sweep and a v-shape + breakout
      const sweep = this._isWickSweepSimple(currentSwing, previousSwing, enrichedCandles, direction);
      if (!sweep) continue;

      const vshape = this._findVShapeSimple(previousSwing, currentSwing, enrichedCandles, direction);
      if (!vshape) continue;

      const breakout = this._identifyBreakoutSimple(
        direction === 'bullish' ? vshape.high : vshape.low,
        enrichedCandles,
        currentSwing.index,
        direction,
        currentSwing
      );
      if (!breakout) continue;

      const pattern = {
        type: 'PATTERN',
        direction,
        previousSwing,
        currentSwing,
        sweepData: sweep,
        vShapeCandle: vshape,
        breakout,
        timestamp: enrichedCandles[currentSwing.index]?.timestamp || Date.now()
      };

      patterns.push(this._enrichPatternMetadata(pattern));
      if (this.emitEvents) {
        this.emit('patternDetected', { symbol, granularity, pattern });
      }
    }

    this.store[symbol][granularity] = patterns;
    return patterns;
  }

  // --- simplified helpers (mirrors logic from original file) ---
  _isWickSweepSimple(currentSwing, previousSwing, candles, direction) {
    // Both swings must be same type
    if (currentSwing.type !== previousSwing.type) return null;

    const start = previousSwing.index + 1;
    const end = currentSwing.index;
    if (start > end) return null;

    if (direction === 'bullish') {
      let first = null;
      for (let i = start; i <= end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low < previousSwing.low) { first = c; break; }
      }
      if (!first) return null;
      for (let i = first.index + 1; i <= end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.close < first.low || c.open < first.low) return null;
      }
      return { isSweep: true, firstSweepCandleIndex: first.index, firstSweepCandleClose: first.close };
    } else {
      let first = null;
      for (let i = start; i <= end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.high > previousSwing.high) { first = c; break; }
      }
      if (!first) return null;
      for (let i = first.index + 1; i <= end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.close > first.high || c.open > first.high) return null;
      }
      return { isSweep: true, firstSweepCandleIndex: first.index, firstSweepCandleClose: first.close };
    }
  }

  _findVShapeSimple(s1, s2, candles, direction) {
    const minIdx = Math.min(s1.index, s2.index);
    const maxIdx = Math.max(s1.index, s2.index);
    const start = minIdx + 1;
    const end = maxIdx;
    if (end <= start) return null;

    let extremum = null;
    if (direction === 'bullish') {
      let maxHigh = -Infinity;
      for (let i = start; i < end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.high > maxHigh) { maxHigh = c.high; extremum = c; }
      }
      const prevC = candles[minIdx];
      if (prevC && prevC.high > (extremum?.high ?? -Infinity)) extremum = prevC;
    } else {
      let minLow = Infinity;
      for (let i = start; i < end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low < minLow) { minLow = c.low; extremum = c; }
      }
      const prevC = candles[minIdx];
      if (prevC && prevC.low < (extremum?.low ?? Infinity)) extremum = prevC;
    }
    return extremum;
  }

  _identifyBreakoutSimple(level, candles, startIndex, direction, currentSwing) {
    for (let i = startIndex; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (c.index === currentSwing.index) continue;
      if (direction === 'bullish') {
        if (c.low < currentSwing.low) return null;
        if (c.close > level) return c;
      } else {
        if (c.high > currentSwing.high) return null;
        if (c.close < level) return c;
      }
    }
    return null;
  }

  _enrichPatternMetadata(pattern) {
    return {
      type: 'PATTERN',
      direction: pattern.direction,
      previousSwingIndex: pattern.previousSwing?.index ?? null,
      currentSwingIndex: pattern.currentSwing?.index ?? null,
      vShapeCandleIndex: pattern.vShapeCandle?.index ?? null,
      breakoutIndex: pattern.breakout?.index ?? null,
      formattedTime: this._formatTime(pattern.timestamp),
      timestamp: pattern.timestamp
    };
  }

  get(symbol, granularity) {
    return this.store[symbol]?.[granularity] || [];
  }

  getStats(symbol, granularity) {
    const patterns = this.get(symbol, granularity);
    return {
      total: patterns.length,
      bullish: patterns.filter(p => p.direction === 'bullish').length,
      bearish: patterns.filter(p => p.direction === 'bearish').length
    };
  }

  clearOld(symbol, granularity, maxAge) {
    const patterns = this.get(symbol, granularity);
    const cutoff = Date.now() - maxAge;
    this.store[symbol][granularity] = patterns.filter(p => p.timestamp > cutoff);
  }
}

module.exports = new Pattern2Engine();
