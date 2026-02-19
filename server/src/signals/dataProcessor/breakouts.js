// server/src/signals/dataProcessor/breakouts.js
const EventEmitter = require('events');
const fs = require('fs').promises;
const path = require('path');
const swingEngine = require('./swings');
const {
  buildCandleIndexMap,
  nextArrayIdx,
  Counter,
} = require('../../utils/dataProcessorUtils');
const Logger = require('../../utils/logger');
const metrics = require('../../utils/metrics');
const { getConfig } = require('../../config');

class BreakoutEngine extends EventEmitter {
  /**
   * Create a BreakoutEngine instance.
   * @param {Object} options - Configuration options.
   * @param {Logger} options.logger - Logger instance.
   * @param {boolean} options.emitEvents - Whether to emit events.
   * @param {string} options.dataDir - Directory for persistence.
   */
  constructor(options = {}) {
    super();
    this.store          = {};
    this.indexSets      = {};
    this.lastBullishBOS = {};
    this.lastBearishBOS = {};
    this.counts         = {};
    this.locks          = new Map(); // concurrency control

    this.logger     = options.logger     || new Logger('BreakoutEngine');
    this.emitEvents = options.emitEvents ?? getConfig().ENABLE_EVENTS;
    this.dataDir    = options.dataDir    || path.join(__dirname, '../../data/breakouts');
  }

  // ── Concurrency lock ──
  async _withLock(symbol, granularity, fn) {
    const key = `${symbol}_${granularity}`;
    while (this.locks.get(key)) {
      await this.locks.get(key);
    }
    let resolve;
    const promise = new Promise(r => resolve = r);
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

  // ── Confidence scoring ──
  _calculateConfidence(breakout) {
    // Simple confidence based on strength and whether it's CHoCH
    // Stronger breakouts get higher confidence
    const base = breakout.strength / 3; // 0.33, 0.66, 1.0
    // If it's a CHoCH (change of character), increase confidence
    const chochBonus = breakout.isCHoCH ? 0.2 : 0;
    return Math.min(base + chochBonus, 1.0);
  }

  // ── Persistence ──
  async _saveToDisk(symbol, granularity) {
    const filePath = path.join(this.dataDir, `${symbol}_${granularity}.json`);
    const data = {
      store: this.store[symbol]?.[granularity] || [],
      lastBullishBOS: this.lastBullishBOS[symbol]?.[granularity] || null,
      lastBearishBOS: this.lastBearishBOS[symbol]?.[granularity] || null,
    };
    await fs.mkdir(this.dataDir, { recursive: true });
    await fs.writeFile(filePath, JSON.stringify(data, null, 2));
    this.logger.debug(`Saved breakouts for ${symbol} @ ${granularity} to disk`);
  }

  async _loadFromDisk(symbol, granularity) {
    const filePath = path.join(this.dataDir, `${symbol}_${granularity}.json`);
    try {
      const data = JSON.parse(await fs.readFile(filePath, 'utf8'));
      if (data.store) {
        this._initStore(symbol, granularity);
        this.store[symbol][granularity] = data.store;
        this.lastBullishBOS[symbol][granularity] = data.lastBullishBOS;
        this.lastBearishBOS[symbol][granularity] = data.lastBearishBOS;

        // Rebuild indexSets and counts from loaded store
        for (const breakout of data.store) {
          this._registerBreakout(symbol, granularity, breakout);
          this._updateCounts(symbol, granularity, breakout);
        }
        this.logger.info(`Loaded breakouts for ${symbol} @ ${granularity} from disk (${data.store.length} breakouts)`);
      }
    } catch (err) {
      if (err.code !== 'ENOENT') this.logger.error(`Failed to load breakouts for ${symbol} @ ${granularity}: ${err.message}`);
    }
  }

  // ── Initialize store ──
  _initStore(symbol, granularity) {
    if (!this.store[symbol])          this.store[symbol]          = {};
    if (!this.indexSets[symbol])      this.indexSets[symbol]      = {};
    if (!this.lastBullishBOS[symbol]) this.lastBullishBOS[symbol] = {};
    if (!this.lastBearishBOS[symbol]) this.lastBearishBOS[symbol] = {};
    if (!this.counts[symbol])         this.counts[symbol]         = {};

    if (!this.store[symbol][granularity])     this.store[symbol][granularity]     = [];
    if (!this.indexSets[symbol][granularity]) this.indexSets[symbol][granularity] = new Set();
    if (!this.counts[symbol][granularity])    this._resetCounts(symbol, granularity);
  }

  // ── Reset counts using Counter ──
  _resetCounts(symbol, granularity) {
    if (!this.counts[symbol]) this.counts[symbol] = {};
    this.counts[symbol][granularity] = new Counter({
      sustained: 0,
      close:     0,
      wick:      0,
      bullish:   0,
      bearish:   0,
      choch:     0,
      bos:       0,
    });
  }

  // ── O(1) duplicate check ──
  _isDuplicate(symbol, granularity, swingIndex, swingType) {
    return this.indexSets[symbol]?.[granularity]?.has(`${swingIndex}_${swingType}`) || false;
  }

  // ── Register breakout in Set ──
  _registerBreakout(symbol, granularity, breakout) {
    if (!this.indexSets[symbol])              this.indexSets[symbol]              = {};
    if (!this.indexSets[symbol][granularity]) this.indexSets[symbol][granularity] = new Set();
    this.indexSets[symbol][granularity].add(`${breakout.swingIndex}_${breakout.swingType}`);
  }

  // ── Update counts cache ──
  _updateCounts(symbol, granularity, breakout) {
    if (!this.counts[symbol])              this.counts[symbol]              = {};
    if (!this.counts[symbol][granularity]) this._resetCounts(symbol, granularity);

    const c = this.counts[symbol][granularity];
    if (breakout.bosType === 'BOS_SUSTAINED')  c.inc('sustained');
    if (breakout.bosType === 'BOS_CLOSE')      c.inc('close');
    if (breakout.bosType === 'BOS_WICK')       c.inc('wick');
    if (breakout.breakDirection === 'bullish') c.inc('bullish');
    if (breakout.breakDirection === 'bearish') c.inc('bearish');
    if (breakout.isCHoCH)                       c.inc('choch');
    if (!breakout.isCHoCH)                      c.inc('bos');
  }

  // ── Update last BOS cache ──
  _updateLastBOS(symbol, granularity, breakout) {
    if (!this.lastBullishBOS[symbol]) this.lastBullishBOS[symbol] = {};
    if (!this.lastBearishBOS[symbol]) this.lastBearishBOS[symbol] = {};

    if (breakout.breakDirection === 'bullish') {
      this.lastBullishBOS[symbol][granularity] = breakout;
    }
    if (breakout.breakDirection === 'bearish') {
      this.lastBearishBOS[symbol][granularity] = breakout;
    }
  }

  // ── CHoCH check ──
  _isCHoCH(swing) {
    return (
      (swing.type === 'high' && swing.direction === 'LH') ||
      (swing.type === 'low'  && swing.direction === 'HL')
    );
  }

  // ── Build a breakout object ──
  _buildBreakout(type, swing, breakingCandle, confirmingCandles = []) {
    const isHigh = swing.type === 'high';
    const breakout = {
      bosType:   type,
      isCHoCH:   this._isCHoCH(swing),
      strength:  type === 'BOS_SUSTAINED' ? 3
               : type === 'BOS_CLOSE'     ? 2
               : 1,

      swingType:          swing.type,
      swingIndex:         swing.index,
      swingPrice:         swing.price,
      swingKeyPrice:      swing.keyPrice,
      swingDirection:     swing.direction,
      swingTime:          swing.time,
      swingFormattedTime: swing.formattedTime,

      breakingCandleIndex:         breakingCandle.index,
      breakingCandleTime:          breakingCandle.time,
      breakingCandleHigh:          breakingCandle.high,
      breakingCandleLow:           breakingCandle.low,
      breakingCandleClose:         breakingCandle.close,
      breakingCandleFormattedTime: breakingCandle.formattedTime,

      confirmingCandles: confirmingCandles.map((c) => ({
        index:         c.index,
        time:          c.time,
        close:         c.close,
        formattedTime: c.formattedTime,
      })),

      breakDirection: isHigh ? 'bullish' : 'bearish',

      formattedTime: breakingCandle.formattedTime,
      time:          breakingCandle.time,
      date:          breakingCandle.date,
    };
    breakout.confidence = this._calculateConfidence(breakout);
    return breakout;
  }

  // ── Input validation ──
  _isValidCandle(candle) {
    return candle && typeof candle.index === 'number' && typeof candle.time === 'number' &&
           typeof candle.high === 'number' && typeof candle.low === 'number' &&
           typeof candle.close === 'number';
  }

  _isValidSwing(swing) {
    return swing && typeof swing.index === 'number' && typeof swing.price === 'number' &&
           swing.type && swing.direction;
  }

  // ── Core break detection ──
  _checkBreak(swing, candles, candleIndexMap) {
    if (!this._isValidSwing(swing) || !candles.every(c => this._isValidCandle(c))) {
      this.logger.warn('Invalid input in _checkBreak');
      return null;
    }

    const isHigh = swing.type === 'high';
    const level  = swing.price;

    const startIdx = nextArrayIdx(candleIndexMap, candles, swing.index);
    if (startIdx === undefined) return null;

    let firstWickBOS  = null;
    let firstCloseBOS = null;
    let sustainedBOS  = null;

    for (let i = startIdx; i < candles.length; i++) {
      const candle      = candles[i];
      const wickBeyond  = isHigh ? candle.high  > level : candle.low   < level;
      const closeBeyond = isHigh ? candle.close > level : candle.close < level;

      if (wickBeyond && !firstWickBOS) firstWickBOS = candle;

      if (closeBeyond && !firstCloseBOS) {
        firstCloseBOS = candle;
        const firstClosePos = candleIndexMap.get(firstCloseBOS.index);
        if (firstClosePos === undefined) return null;
        const config = this._getConfig(); // we don't have symbol/gran here, but config is same for all? We'll pass symbol/gran from caller.
        // Actually, we need symbol/gran to get config. We'll modify callers to pass config.
        // For now, use default.
        const maxScan = this._getConfig().MAX_BOS_SCAN_CANDLES;
        const endIdx = Math.min(candles.length - 1, firstClosePos + maxScan);

        for (let k = i + 1; k <= endIdx; k++) {
          const c = candles[k];
          const closedBeyondConfirming = isHigh
            ? c.close > firstCloseBOS.close
            : c.close < firstCloseBOS.close;

          if (closedBeyondConfirming) {
            sustainedBOS = c;
            break;
          }
        }
        break;
      }
    }

    if (sustainedBOS) {
      return this._buildBreakout(
        'BOS_SUSTAINED',
        swing,
        firstCloseBOS,
        [firstCloseBOS, sustainedBOS]
      );
    }
    if (firstCloseBOS) return this._buildBreakout('BOS_CLOSE', swing, firstCloseBOS);
    if (firstWickBOS)  return this._buildBreakout('BOS_WICK',  swing, firstWickBOS);
    return null;
  }

  // ── Upgrade existing breakout if stronger confirmation found ──
  _upgradeBreakout(symbol, granularity, swing, candles, candleIndexMap) {
    const existing = this.store[symbol][granularity]
      .find((b) => b.swingIndex === swing.index && b.swingType === swing.type);

    if (!existing) return;
    if (existing.bosType === 'BOS_SUSTAINED') return;

    const result = this._checkBreak(swing, candles, candleIndexMap);
    if (!result) return;

    const currentStrength = existing.strength;
    const newStrength     = result.strength;
    if (newStrength <= currentStrength) return;

    const c = this.counts[symbol][granularity];
    if (currentStrength === 1) c.dec('wick');
    if (currentStrength === 2) c.dec('close');
    if (newStrength     === 2) c.inc('close');
    if (newStrength     === 3) c.inc('sustained');

    const oldBosType = existing.bosType;

    // Preserve confidence? We'll recalc after upgrade.
    const oldConfidence = existing.confidence;

    Object.assign(existing, {
      bosType:                     result.bosType,
      strength:                    result.strength,
      breakingCandleIndex:         result.breakingCandleIndex,
      breakingCandleTime:          result.breakingCandleTime,
      breakingCandleHigh:          result.breakingCandleHigh,
      breakingCandleLow:           result.breakingCandleLow,
      breakingCandleClose:         result.breakingCandleClose,
      breakingCandleFormattedTime: result.breakingCandleFormattedTime,
      confirmingCandles:           result.confirmingCandles,
      formattedTime:               result.formattedTime,
      time:                        result.time,
      date:                        result.date,
    });

    existing.confidence = this._calculateConfidence(existing); // recalc confidence

    this._updateLastBOS(symbol, granularity, existing);

    this.logger.info(`⬆️ Upgraded → ${symbol} @ ${granularity}s | ${existing.swingType} at ${existing.swingPrice} | ${oldBosType} → ${existing.bosType} | Confidence: ${existing.confidence.toFixed(2)} | ${existing.formattedTime}`);
    if (this.emitEvents) this.emit('breakoutUpgraded', { breakout: existing });
    metrics.increment('breakout_upgrades');
  }

  // ── Full detection run ──
  async detectAll(symbol, granularity, candles) {
    return this._withLock(symbol, granularity, async () => {
      metrics.increment('detectAll_calls');
      const timer = 'detectAll_' + symbol + '_' + granularity;
      metrics.startTimer(timer);

      this._initStore(symbol, granularity);
      this.store[symbol][granularity]          = [];
      this.indexSets[symbol][granularity]      = new Set();
      this.lastBullishBOS[symbol][granularity] = null;
      this.lastBearishBOS[symbol][granularity] = null;
      this._resetCounts(symbol, granularity);

      let swings;
      try {
        swings = swingEngine.get(symbol, granularity);
      } catch (err) {
        this.logger.error(`Failed to get swings for ${symbol} @ ${granularity}s: ${err.message}`);
        metrics.increment('swingEngine_errors');
        metrics.endTimer(timer);
        return []; // graceful degradation
      }

      if (!swings.length) {
        this.logger.warn(`No swings for ${symbol} @ ${granularity}s — run swingEngine.detectAll first`);
        metrics.endTimer(timer);
        return [];
      }

      const candleIndexMap = buildCandleIndexMap(candles);
      const breakouts      = [];

      for (const swing of swings) {
        const result = this._checkBreak(swing, candles, candleIndexMap);
        if (result) {
          breakouts.push(result);
          this.store[symbol][granularity].push(result);
          this._registerBreakout(symbol, granularity, result);
          this._updateCounts(symbol, granularity, result);
          this._updateLastBOS(symbol, granularity, result);
          if (this.emitEvents) this.emit('newBreakout', { breakout: result });
        }
      }

      metrics.set(`breakouts_${symbol}_${granularity}`, breakouts.length);
      const elapsed = metrics.endTimer(timer);
      this.logger.info(`${symbol} @ ${granularity}s — ${breakouts.length} breakouts detected [${elapsed}ms]`);

      if (this.dataDir) await this._saveToDisk(symbol, granularity);

      return breakouts;
    });
  }

  // ── Incremental detection ──
  async detectLatest(symbol, granularity, candles) {
    return this._withLock(symbol, granularity, async () => {
      metrics.increment('detectLatest_calls');
      const timer = 'detectLatest_' + symbol + '_' + granularity;
      metrics.startTimer(timer);

      this._initStore(symbol, granularity);

      let swings;
      try {
        swings = swingEngine.get(symbol, granularity);
      } catch (err) {
        this.logger.error(`Failed to get swings for ${symbol} @ ${granularity}s: ${err.message}`);
        metrics.increment('swingEngine_errors');
        metrics.endTimer(timer);
        return [];
      }

      if (!swings.length) {
        metrics.endTimer(timer);
        return [];
      }

      const candleIndexMap = buildCandleIndexMap(candles);
      const newBreakouts   = [];

      for (const swing of swings) {
        if (swing.index >= candles.length - 1) continue;

        if (this._isDuplicate(symbol, granularity, swing.index, swing.type)) {
          this._upgradeBreakout(symbol, granularity, swing, candles, candleIndexMap);
          continue;
        }

        const result = this._checkBreak(swing, candles, candleIndexMap);
        if (result) {
          this.store[symbol][granularity].push(result);
          this._registerBreakout(symbol, granularity, result);
          this._updateCounts(symbol, granularity, result);
          this._updateLastBOS(symbol, granularity, result);
          newBreakouts.push(result);
          this.logger.info(`🚨 ${result.bosType}${result.isCHoCH ? ' (CHoCH)' : ''} → ${symbol} @ ${granularity}s | ${result.swingType} broken at ${result.swingPrice} | Strength: ${result.strength} | Confidence: ${result.confidence.toFixed(2)} | ${result.formattedTime}`);
          if (this.emitEvents) this.emit('newBreakout', { breakout: result });
          metrics.increment('new_breakouts');
        }
      }

      const elapsed = metrics.endTimer(timer);
      this.logger.debug(`detectLatest for ${symbol} @ ${granularity}s completed in ${elapsed}ms, ${newBreakouts.length} new breakouts`);

      if (this.dataDir && newBreakouts.length > 0) await this._saveToDisk(symbol, granularity);

      return newBreakouts;
    });
  }

  // ── Check if a swing is broken ──
  isBroken(symbol, granularity, swingIndex, swingType) {
    return this._isDuplicate(symbol, granularity, swingIndex, swingType);
  }

  // ── Current market bias ──
  getCurrentBias(symbol, granularity) {
    const latest = this.getLatest(symbol, granularity);
    if (!latest) return null;
    return {
      bias:          latest.breakDirection,
      isCHoCH:       latest.isCHoCH,
      bosType:       latest.bosType,
      strength:      latest.strength,
      confidence:    latest.confidence,
      formattedTime: latest.formattedTime,
    };
  }

  // ── Immutable getters ──
  get(symbol, granularity) {
    const arr = this.store[symbol]?.[granularity];
    return arr ? [...arr] : [];
  }

  getSustained(symbol, granularity) {
    return this.get(symbol, granularity).filter(b => b.bosType === 'BOS_SUSTAINED');
  }

  getClose(symbol, granularity) {
    return this.get(symbol, granularity).filter(b => b.bosType === 'BOS_CLOSE');
  }

  getWick(symbol, granularity) {
    return this.get(symbol, granularity).filter(b => b.bosType === 'BOS_WICK');
  }

  getBullish(symbol, granularity) {
    return this.get(symbol, granularity).filter(b => b.breakDirection === 'bullish');
  }

  getBearish(symbol, granularity) {
    return this.get(symbol, granularity).filter(b => b.breakDirection === 'bearish');
  }

  getCHoCH(symbol, granularity) {
    return this.get(symbol, granularity).filter(b => b.isCHoCH);
  }

  getBOS(symbol, granularity) {
    return this.get(symbol, granularity).filter(b => !b.isCHoCH);
  }

  getByStrength(symbol, granularity, minStrength = 2) {
    return this.get(symbol, granularity).filter(b => b.strength >= minStrength);
  }

  getLastBullishBOS(symbol, granularity) {
    return this.lastBullishBOS[symbol]?.[granularity] || null;
  }

  getLastBearishBOS(symbol, granularity) {
    return this.lastBearishBOS[symbol]?.[granularity] || null;
  }

  getLatest(symbol, granularity) {
    const breakouts = this.get(symbol, granularity);
    return breakouts[breakouts.length - 1] || null;
  }

  getLastN(symbol, granularity, n) {
    const arr = this.store[symbol]?.[granularity];
    return arr ? arr.slice(-n) : [];
  }

  getAll() {
    const copy = {};
    for (const [symbol, symData] of Object.entries(this.store)) {
      copy[symbol] = {};
      for (const [gran, breakouts] of Object.entries(symData)) {
        copy[symbol][gran] = [...breakouts];
      }
    }
    return copy;
  }

  // ── Metrics ──
  getMetrics() {
    return metrics.getAll();
  }

  // ── Clear store ──
  clearStore(symbol, granularity) {
    if (this.store[symbol])          this.store[symbol][granularity]          = [];
    if (this.indexSets[symbol])      this.indexSets[symbol][granularity]      = new Set();
    if (this.lastBullishBOS[symbol]) this.lastBullishBOS[symbol][granularity] = null;
    if (this.lastBearishBOS[symbol]) this.lastBearishBOS[symbol][granularity] = null;
    if (this.counts[symbol])         this._resetCounts(symbol, granularity);
    this.logger.info(`Store cleared → ${symbol} @ ${granularity}s`);
  }

  clearAll() {
    Object.keys(this.store).forEach((symbol) => {
      Object.keys(this.store[symbol]).forEach((g) => {
        this.store[symbol][g] = [];
        this.indexSets[symbol][g] = new Set();
        this._resetCounts(symbol, g);
        if (this.lastBullishBOS[symbol]) this.lastBullishBOS[symbol][g] = null;
        if (this.lastBearishBOS[symbol]) this.lastBearishBOS[symbol][g] = null;
      });
    });
    this.logger.info(`Full store cleared`);
  }

  // ── Summary ──
  getSummary(symbol, granularity) {
    const c = this.counts[symbol]?.[granularity] || new Counter({
      sustained: 0, close: 0, wick: 0,
      bullish: 0, bearish: 0, choch: 0, bos: 0,
    });
    return {
      symbol,
      granularity,
      total:     this.get(symbol, granularity).length,
      sustained: c.get('sustained'),
      close:     c.get('close'),
      wick:      c.get('wick'),
      bullish:   c.get('bullish'),
      bearish:   c.get('bearish'),
      choch:     c.get('choch'),
      bos:       c.get('bos'),
      latest:    this.getLatest(symbol, granularity),
      bias:      this.getCurrentBias(symbol, granularity),
    };
  }
}

module.exports = new BreakoutEngine();