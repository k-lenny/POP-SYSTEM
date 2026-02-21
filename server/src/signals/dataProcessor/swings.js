// server/src/signals/dataProcessor/swings.js (memory-only version)
const EventEmitter = require('events');
const Logger = require('../../utils/logger');
const metrics = require('../../utils/metrics');
const { getConfig } = require('../../config');

// ── Key price calculation ──
const calculateKeyPrice = (type, candle) => {
  if (type === 'high') return Math.max(candle.open, candle.close);
  if (type === 'low') return Math.min(candle.open, candle.close);
  return candle.close;
};

// ── Candle sanity check ──
const isSaneCandle = (c) => {
  return (
    typeof c.high === 'number' && isFinite(c.high) &&
    typeof c.low === 'number' && isFinite(c.low) &&
    typeof c.open === 'number' && isFinite(c.open) &&
    typeof c.close === 'number' && isFinite(c.close) &&
    c.high >= c.low
  );
};

class SwingEngine extends EventEmitter {
  /**
   * Create a SwingEngine instance.
   * @param {Object} options - Configuration options.
   * @param {Logger} options.logger - Logger instance.
   * @param {boolean} options.emitEvents - Whether to emit events.
   */
  constructor(options = {}) {
    super();
    this.store = {};
    this.indexSets = {};
    this.locks = new Map(); // concurrency control

    this.logger = options.logger || new Logger('SwingEngine');
    this.emitEvents = options.emitEvents ?? getConfig().ENABLE_EVENTS;
  }

  // ── Concurrency lock ──
  async _withLock(symbol, granularity, fn) {
    const key = `${symbol}_${granularity}`;
    while (this.locks.get(key)) {
      await this.locks.get(key);
    }
    let resolve;
    const promise = new Promise(r => (resolve = r));
    this.locks.set(key, promise);
    try {
      return await fn();
    } finally {
      this.locks.delete(key);
      resolve();
    }
  }

  // ── Get config with overrides ──
  _getConfig(symbol, granularity) {
    return getConfig(symbol, granularity);
  }

  // ── O(1) duplicate check via Set ──
  _isDuplicate(symbol, granularity, index, type) {
    return this.indexSets[symbol]?.[granularity]?.has(`${index}_${type}`) || false;
  }

  // ── Register swing in index Set ──
  _registerSwing(symbol, granularity, swing) {
    if (!this.indexSets[symbol]) this.indexSets[symbol] = {};
    if (!this.indexSets[symbol][granularity]) this.indexSets[symbol][granularity] = new Set();
    this.indexSets[symbol][granularity].add(`${swing.index}_${swing.type}`);
  }

  // ── Build a swing object ──
  _buildSwing(type, candle, index, strength) {
    return {
      type,
      price: type === 'high' ? candle.high : candle.low,
      keyPrice: calculateKeyPrice(type, candle),
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      time: candle.time,
      formattedTime: candle.formattedTime,
      date: candle.date,
      index,
      strength,
      candleIndex: index,
      direction: null,
    };
  }

  // ── Initialize store structures ──
  _initStore(symbol, granularity) {
    if (!this.store[symbol]) this.store[symbol] = {};
    if (!this.store[symbol][granularity]) this.store[symbol][granularity] = [];
    if (!this.indexSets[symbol]) this.indexSets[symbol] = {};
    if (!this.indexSets[symbol][granularity]) this.indexSets[symbol][granularity] = new Set();
  }

  // ── Full detection run — called on startup or after reconnect ──
  async detectAll(symbol, granularity, candles, strength = 1) {
    return this._withLock(symbol, granularity, async () => {
      metrics.increment('swing_detectAll_calls');
      const timer = `detectAll_${symbol}_${granularity}`;
      metrics.startTimer(timer);

      // Validate input
      if (!candles || candles.length < strength * 2 + 1) {
        this.logger.warn(`[SwingEngine] Not enough candles for ${symbol} @ ${granularity}s — need ${strength * 2 + 1}, have ${candles?.length || 0}`);
        metrics.endTimer(timer);
        return [];
      }

      // Validate strength
      if (!Number.isInteger(strength) || strength < 1) {
        this.logger.warn(`[SwingEngine] Invalid strength ${strength} for ${symbol} @ ${granularity}s — using 1`);
        strength = 1;
      }

      const swings = [];

      // Reset store for clean rebuild
      this._initStore(symbol, granularity);
      this.store[symbol][granularity] = [];
      this.indexSets[symbol][granularity] = new Set();

      for (let i = strength; i < candles.length - strength; i++) {
        const current = candles[i];
        if (!isSaneCandle(current)) continue;

        let isSwingHigh = true;
        let isSwingLow = true;

        for (let j = 1; j <= strength; j++) {
          const prev = candles[i - j];
          const next = candles[i + j];

          if (!isSaneCandle(prev) || !isSaneCandle(next)) {
            isSwingHigh = false;
            isSwingLow = false;
            break;
          }

          if (current.high <= prev.high || current.high <= next.high) isSwingHigh = false;
          if (current.low >= prev.low || current.low >= next.low) isSwingLow = false;
        }

        if (isSwingHigh) {
          const swing = this._buildSwing('high', current, i, strength);
          swings.push(swing);
          this._registerSwing(symbol, granularity, swing);
        }

        if (isSwingLow) {
          const swing = this._buildSwing('low', current, i, strength);
          swings.push(swing);
          this._registerSwing(symbol, granularity, swing);
        }
      }

      // Sort by time ascending
      swings.sort((a, b) => a.time - b.time);

      // Save to store
      this.store[symbol][granularity] = swings;

      // Full direction rebuild
      this.updateDirections(symbol, granularity, true);

      // Emit events
      if (this.emitEvents) {
        for (const swing of swings) {
          this.emit('newSwing', { swing });
        }
      }

      metrics.set(`swings_${symbol}_${granularity}`, swings.length);
      const elapsed = metrics.endTimer(timer);
      this.logger.info(`[SwingEngine] ${symbol} @ ${granularity}s — ${swings.length} swings detected (strength ${strength}) [${elapsed}ms]`);

      return swings;
    });
  }

  // ── Incremental detection — called on every candle close ──
  async detectLatest(symbol, granularity, candles, strength = 1) {
    return this._withLock(symbol, granularity, async () => {
      metrics.increment('swing_detectLatest_calls');
      const timer = `detectLatest_${symbol}_${granularity}`;
      metrics.startTimer(timer);

      if (!candles || candles.length < strength * 2 + 1) {
        metrics.endTimer(timer);
        return [];
      }

      if (!Number.isInteger(strength) || strength < 1) strength = 1;

      this._initStore(symbol, granularity);

      const newSwings = [];

      const i = candles.length - strength - 1;
      if (i < strength || i < 0) {
        metrics.endTimer(timer);
        return [];
      }

      const current = candles[i];
      if (!isSaneCandle(current)) {
        metrics.endTimer(timer);
        return [];
      }

      const alreadyHigh = this._isDuplicate(symbol, granularity, i, 'high');
      const alreadyLow = this._isDuplicate(symbol, granularity, i, 'low');
      if (alreadyHigh && alreadyLow) {
        metrics.endTimer(timer);
        return [];
      }

      let isSwingHigh = !alreadyHigh;
      let isSwingLow = !alreadyLow;

      for (let j = 1; j <= strength; j++) {
        const prev = candles[i - j];
        const next = candles[i + j];

        if (!isSaneCandle(prev) || !isSaneCandle(next)) {
          isSwingHigh = false;
          isSwingLow = false;
          break;
        }

        if (current.high <= prev.high || current.high <= next.high) isSwingHigh = false;
        if (current.low >= prev.low || current.low >= next.low) isSwingLow = false;
      }

      if (isSwingHigh) {
        const swing = this._buildSwing('high', current, i, strength);
        this.store[symbol][granularity].push(swing);
        this._registerSwing(symbol, granularity, swing);
        newSwings.push(swing);
        this.logger.info(`[SwingEngine] 🔺 New Swing High → ${symbol} @ ${granularity}s | Price: ${swing.price} | ${swing.formattedTime}`);
        if (this.emitEvents) this.emit('newSwing', { swing });
        metrics.increment('new_swings');
      }

      if (isSwingLow) {
        const swing = this._buildSwing('low', current, i, strength);
        this.store[symbol][granularity].push(swing);
        this._registerSwing(symbol, granularity, swing);
        newSwings.push(swing);
        this.logger.info(`[SwingEngine] 🔻 New Swing Low  → ${symbol} @ ${granularity}s | Price: ${swing.price} | ${swing.formattedTime}`);
        if (this.emitEvents) this.emit('newSwing', { swing });
        metrics.increment('new_swings');
      }

      if (newSwings.length > 0) {
        this.updateDirections(symbol, granularity, false);
      }

      const elapsed = metrics.endTimer(timer);
      this.logger.debug(`[SwingEngine] detectLatest for ${symbol} @ ${granularity}s completed in ${elapsed}ms, ${newSwings.length} new swings`);

      return newSwings;
    });
  }

  // ── Update swing directions ──
  // fullRebuild = true  → assigns direction to every swing (used after detectAll)
  // fullRebuild = false → only updates last swing in sequence (used after detectLatest)
  updateDirections(symbol, granularity, fullRebuild = false) {
    const swings = this.store[symbol]?.[granularity] || [];
    if (swings.length === 0) return;

    const highs = swings.filter((s) => s.type === 'high');
    const lows = swings.filter((s) => s.type === 'low');

    if (fullRebuild) {
      // ── Full pass — no nulls guaranteed ──
      if (highs.length > 0) highs[0].direction = 'FIRST';
      if (lows.length > 0) lows[0].direction = 'FIRST';

      for (let i = 1; i < highs.length; i++) {
        highs[i].direction = highs[i].price > highs[i - 1].price ? 'HH' : 'LH';
      }

      for (let i = 1; i < lows.length; i++) {
        lows[i].direction = lows[i].price > lows[i - 1].price ? 'HL' : 'LL';
      }
    } else {
      // ── Incremental pass — only update last swing ──
      if (highs.length > 0 && highs[0].direction === null) highs[0].direction = 'FIRST';
      if (lows.length > 0 && lows[0].direction === null) lows[0].direction = 'FIRST';

      const lastHighIdx = highs.length - 1;
      const lastLowIdx = lows.length - 1;

      if (lastHighIdx > 0) {
        highs[lastHighIdx].direction = highs[lastHighIdx].price > highs[lastHighIdx - 1].price ? 'HH' : 'LH';
      }

      if (lastLowIdx > 0) {
        lows[lastLowIdx].direction = lows[lastLowIdx].price > lows[lastLowIdx - 1].price ? 'HL' : 'LL';
      }
    }
  }

  // ── Clear store for one symbol/timeframe ──
  clearStore(symbol, granularity) {
    if (this.store[symbol]) {
      this.store[symbol][granularity] = [];
      this.logger.info(`[SwingEngine] Store cleared → ${symbol} @ ${granularity}s`);
    }
    if (this.indexSets[symbol]) {
      this.indexSets[symbol][granularity] = new Set();
    }
  }

  // ── Clear entire store ──
  clearAll() {
    Object.keys(this.store).forEach((symbol) => {
      Object.keys(this.store[symbol]).forEach((granularity) => {
        this.store[symbol][granularity] = [];
      });
    });
    Object.keys(this.indexSets).forEach((symbol) => {
      Object.keys(this.indexSets[symbol]).forEach((granularity) => {
        this.indexSets[symbol][granularity] = new Set();
      });
    });
    this.logger.info(`[SwingEngine] Full store cleared`);
  }

  // ── Getters (immutable) ──

  get(symbol, granularity) {
    const arr = this.store[symbol]?.[granularity];
    return arr ? [...arr] : [];
  }

  getHighs(symbol, granularity) {
    return this.get(symbol, granularity).filter((s) => s.type === 'high');
  }

  getLows(symbol, granularity) {
    return this.get(symbol, granularity).filter((s) => s.type === 'low');
  }

  getLatestHigh(symbol, granularity) {
    const highs = this.getHighs(symbol, granularity);
    return highs[highs.length - 1] || null;
  }

  getLatestLow(symbol, granularity) {
    const lows = this.getLows(symbol, granularity);
    return lows[lows.length - 1] || null;
  }

  getLastN(symbol, granularity, n) {
    const arr = this.store[symbol]?.[granularity];
    return arr ? arr.slice(-n) : [];
  }

  getAll() {
    const copy = {};
    for (const [symbol, symData] of Object.entries(this.store)) {
      copy[symbol] = {};
      for (const [gran, swings] of Object.entries(symData)) {
        copy[symbol][gran] = [...swings];
      }
    }
    return copy;
  }

  // ── Metrics ──
  getMetrics() {
    return metrics.getAll();
  }

  // ── Summary for logging and routes ──
  getSummary(symbol, granularity) {
    const swings = this.get(symbol, granularity);
    const highs = swings.filter((s) => s.type === 'high');
    const lows = swings.filter((s) => s.type === 'low');

    return {
      symbol,
      granularity,
      total: swings.length,
      highs: highs.length,
      lows: lows.length,
      latestHigh: this.getLatestHigh(symbol, granularity),
      latestLow: this.getLatestLow(symbol, granularity),
    };
  }
}

const swingEngine = new SwingEngine();
module.exports = swingEngine;
module.exports.calculateKeyPrice = calculateKeyPrice;