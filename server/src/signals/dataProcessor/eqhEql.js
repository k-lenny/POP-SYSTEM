// server/src/signals/dataProcessor/eqhEql.js
const EventEmitter = require('events');
const fs = require('fs').promises;
const path = require('path');
const swingEngine    = require('./swings');
const breakoutEngine = require('./breakouts');
const {
  buildCandleIndexMap,
  nextArrayIdx,
  sortedInsert,
  Counter,
} = require('../../utils/dataProcessorUtils');
const Logger = require('../../utils/logger');
const metrics = require('../../utils/metrics');
const { getConfig } = require('../../config');

class EqhEqlEngine extends EventEmitter {
  /**
   * Create an EqhEqlEngine instance.
   * @param {Object} options - Configuration options.
   * @param {Logger} options.logger - Logger instance.
   * @param {boolean} options.emitEvents - Whether to emit events.
   * @param {string} options.dataDir - Directory for persistence.
   */
  constructor(options = {}) {
    super();
    this.store           = {};
    this.indexSets       = {};
    this.counts          = {};
    this.lastLevel       = {};
    this.lastActiveLevel = {};
    this.lastSwingCount  = {};
    this.locks           = new Map(); // concurrency control

    this.logger      = options.logger      || new Logger('EqhEqlEngine');
    this.emitEvents  = options.emitEvents  ?? getConfig().ENABLE_EVENTS;
    this.dataDir     = options.dataDir     || path.join(__dirname, '../../data/eqhEql');
  }

  // ─────────────────────────────────────────────
  // PRIVATE HELPERS
  // ─────────────────────────────────────────────

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

  // ── Confidence scoring based on status and BOS type ──
  _calculateConfidence(level) {
    const statusConf = { active: 1.0, swept: 0.5, broken: 0.2 }[level.status] || 0.0;
    const bosBoost = (level.status === 'broken' && level.brokenBosType === 'BOS_SUSTAINED') ? 0.3 : 0.0;
    return Math.min(statusConf + bosBoost, 1.0);
  }

  // ── Persistence ──
  async _saveToDisk(symbol, granularity) {
    const filePath = path.join(this.dataDir, `${symbol}_${granularity}.json`);
    const data = {
      store: this.store[symbol]?.[granularity] || [],
      lastSwingCount: this.lastSwingCount[symbol]?.[granularity] || { highs: 0, lows: 0 },
    };
    await fs.mkdir(this.dataDir, { recursive: true });
    await fs.writeFile(filePath, JSON.stringify(data, null, 2));
    this.logger.debug(`Saved levels for ${symbol} @ ${granularity} to disk`);
  }

  async _loadFromDisk(symbol, granularity) {
    const filePath = path.join(this.dataDir, `${symbol}_${granularity}.json`);
    try {
      const data = JSON.parse(await fs.readFile(filePath, 'utf8'));
      if (data.store) {
        this._initStore(symbol, granularity);
        this.store[symbol][granularity] = data.store;
        this.lastSwingCount[symbol][granularity] = data.lastSwingCount || { highs: 0, lows: 0 };

        // Migrate old levels to include new fields
        let migrationCount = 0;
        for (const level of data.store) {
          if (this._migrateLevel(level)) {
            migrationCount++;
          }
          this._registerLevel(symbol, granularity, level);
          this._registerLevelCounts(symbol, granularity, level);
        }
        
        if (migrationCount > 0) {
          this.logger.info(`Migrated ${migrationCount} levels to new schema for ${symbol} @ ${granularity}`);
        }
        
        this._repairCachesIfNeeded(symbol, granularity);
        this.logger.info(`Loaded levels for ${symbol} @ ${granularity} from disk (${data.store.length} levels)`);
      }
    } catch (err) {
      if (err.code !== 'ENOENT') this.logger.error(`Failed to load levels for ${symbol} @ ${granularity}: ${err.message}`);
    }
  }

  _migrateLevel(level) {
    let migrated = false;
    
    // Add missing pre-breakout fields
    if (!level.hasOwnProperty('preBreakoutVIndex')) {
      level.preBreakoutVIndex = null;
      migrated = true;
    }
    if (!level.hasOwnProperty('preBreakoutVTime')) {
      level.preBreakoutVTime = null;
      migrated = true;
    }
    
    // Ensure all pre-breakout fields exist
    if (!level.hasOwnProperty('preBreakoutVDepth')) {
      level.preBreakoutVDepth = null;
      migrated = true;
    }
    if (!level.hasOwnProperty('preBreakoutVFormattedTime')) {
      level.preBreakoutVFormattedTime = null;
      migrated = true;
    }
    
    return migrated;
  }

  _initStore(symbol, granularity) {
    if (!this.store[symbol])           this.store[symbol]           = {};
    if (!this.indexSets[symbol])       this.indexSets[symbol]       = {};
    if (!this.counts[symbol])          this.counts[symbol]          = {};
    if (!this.lastLevel[symbol])       this.lastLevel[symbol]       = {};
    if (!this.lastActiveLevel[symbol]) this.lastActiveLevel[symbol] = {};
    if (!this.lastSwingCount[symbol])  this.lastSwingCount[symbol]  = {};

    if (!this.store[symbol][granularity])               this.store[symbol][granularity]               = [];
    if (!this.indexSets[symbol][granularity])           this.indexSets[symbol][granularity]           = new Set();
    if (!this.counts[symbol][granularity])              this._resetCounts(symbol, granularity);
    if (this.lastLevel[symbol][granularity]      == null) this.lastLevel[symbol][granularity]         = null;
    if (this.lastActiveLevel[symbol][granularity] == null) this.lastActiveLevel[symbol][granularity]  = null;
    if (!this.lastSwingCount[symbol][granularity])      this.lastSwingCount[symbol][granularity]      = { highs: 0, lows: 0 };
  }

  _resetCounts(symbol, granularity) {
    if (!this.counts[symbol]) this.counts[symbol] = {};
    this.counts[symbol][granularity] = new Counter({ eqh: 0, eql: 0, active: 0, broken: 0, swept: 0 });
  }

  _isDuplicate(symbol, granularity, key) {
    return this.indexSets[symbol]?.[granularity]?.has(key) || false;
  }

  _registerLevel(symbol, granularity, level) {
    if (!this.indexSets[symbol])              this.indexSets[symbol]              = {};
    if (!this.indexSets[symbol][granularity]) this.indexSets[symbol][granularity] = new Set();
    this.indexSets[symbol][granularity].add(level.key);
  }

  _registerLevelCounts(symbol, granularity, level) {
    const c = this.counts[symbol][granularity];
    if (level.type === 'EQH') c.inc('eqh');
    else                      c.inc('eql');
    if      (level.status === 'active') c.inc('active');
    else if (level.status === 'broken') c.inc('broken');
    else if (level.status === 'swept')  c.inc('swept');
  }

  _updateLastLevel(symbol, granularity, level) {
    if (!this.lastLevel[symbol]) this.lastLevel[symbol] = {};
    this.lastLevel[symbol][granularity] = level;
    if (level.status === 'active') {
      if (!this.lastActiveLevel[symbol]) this.lastActiveLevel[symbol] = {};
      this.lastActiveLevel[symbol][granularity] = level;
    }
  }

  _repairCachesIfNeeded(symbol, granularity) {
    const sorted = this.store[symbol]?.[granularity];
    if (!sorted?.length) return;
    if (!this.lastLevel[symbol][granularity]) {
      this.lastLevel[symbol][granularity] = sorted[sorted.length - 1];
    }
    if (!this.lastActiveLevel[symbol][granularity]) {
      this.lastActiveLevel[symbol][granularity] = this._findLastActive(symbol, granularity);
    }
  }

  _findLastActive(symbol, granularity) {
    const levels = this.store[symbol]?.[granularity] || [];
    for (let i = levels.length - 1; i >= 0; i--) {
      if (levels[i].status === 'active') return levels[i];
    }
    return null;
  }

  // ── Input validation ──
  _isValidCandle(candle) {
    return candle && typeof candle.index === 'number' && typeof candle.time === 'number' &&
           typeof candle.high === 'number' && typeof candle.low === 'number' &&
           typeof candle.close === 'number';
  }

  _isValidSwing(swing) {
    return swing && typeof swing.index === 'number' && typeof swing.price === 'number' &&
           typeof swing.keyPrice === 'number' && swing.type && swing.direction;
  }

  _validateAndBuild(type, firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap) {
    if (!this._isValidSwing(firstSwing) || !this._isValidSwing(secondSwing)) {
      this.logger.warn('Invalid swing object');
      return null;
    }
    if (!candles.every(c => this._isValidCandle(c))) {
      this.logger.warn('Invalid candle data');
      return null;
    }

    const zoneTop    = firstSwing.price;
    const zoneBottom = firstSwing.keyPrice;

    const startIdx = nextArrayIdx(candleIndexMap, candles, firstSwing.index);
    if (startIdx === undefined) return null;

    const { map: swingMap, indices: swingIndices } = swingIndexMap;
    const vTargetType = type === 'EQH' ? 'low' : 'high';

    let hasCandles     = false;
    let hasVShape      = false;
    let vExtreme       = type === 'EQH' ? Infinity : -Infinity;
    let vExtremeCandle = null;
    let candleCount    = 0;

    for (let i = startIdx; i < candles.length; i++) {
      const c = candles[i];
      if (c.index >= secondSwing.index) break;

      hasCandles = true;
      candleCount++;

      if (type === 'EQH' && c.high >= zoneBottom) return null;
      if (type === 'EQL' && c.low  <= zoneTop)    return null;

      if (type === 'EQH' && c.low  < vExtreme) { vExtreme = c.low;  vExtremeCandle = c; }
      if (type === 'EQL' && c.high > vExtreme) { vExtreme = c.high; vExtremeCandle = c; }
    }

    if (!hasCandles) return null;

    for (const idx of swingIndices) {
      if (idx <= firstSwing.index)  continue;
      if (idx >= secondSwing.index) break;
      if (swingMap.get(idx).type === vTargetType) { hasVShape = true; break; }
    }

    if (!hasVShape) return null;

    const level = {
      type,
      key: `${firstSwing.index}_${secondSwing.index}_${type}`,

      zoneTop,
      zoneBottom,
      zoneMid: (zoneTop + zoneBottom) / 2,

      firstSwingIndex:         firstSwing.index,
      firstSwingPrice:         firstSwing.price,
      firstSwingKeyPrice:      firstSwing.keyPrice,
      firstSwingDirection:     firstSwing.direction,
      firstSwingTime:          firstSwing.time,
      firstSwingFormattedTime: firstSwing.formattedTime,

      secondSwingIndex:         secondSwing.index,
      secondSwingPrice:         secondSwing.price,
      secondSwingKeyPrice:      secondSwing.keyPrice,
      secondSwingDirection:     secondSwing.direction,
      secondSwingTime:          secondSwing.time,
      secondSwingFormattedTime: secondSwing.formattedTime,

      // V-shape that occurs BETWEEN first and second swing (original detection)
      vShapeDepth:         vExtreme === Infinity || vExtreme === -Infinity ? null : vExtreme,
      vShapeIndex:         vExtremeCandle?.index         ?? null,
      vShapeTime:          vExtremeCandle?.time          ?? null,
      vShapeFormattedTime: vExtremeCandle?.formattedTime ?? null,
      candlesBetween:      candleCount,

      // V-shape that occurs AFTER second swing, BEFORE breakout (pre-breakout extreme)
      preBreakoutVDepth:         null,
      preBreakoutVIndex:         null,
      preBreakoutVTime:          null,
      preBreakoutVFormattedTime: null,

      status:              'active',
      brokenTime:          null,
      brokenFormattedTime: null,
      brokenIndex:         null,
      brokenBy:            null,
      brokenBosType:       null,
      sweptTime:           null,
      sweptFormattedTime:  null,
      sweptIndex:          null,
      sweptBy:             null,

      lastCheckedIndex: null,

      bias:          null,
      formattedTime: secondSwing.formattedTime,
      time:          secondSwing.time,
      date:          secondSwing.date,
    };
    level.confidence = this._calculateConfidence(level);
    return level;
  }

  _classifyBosType(level, candles, startK, candleIndexMap) {
    const breakArrayPos = candleIndexMap.get(level.brokenIndex);
    if (breakArrayPos === undefined) return 'BOS_CLOSE';

    const config = this._getConfig(); 
    const maxScan = config.MAX_BOS_SCAN_CANDLES;
    const endIdx = Math.min(candles.length - 1, breakArrayPos + maxScan);

    if (level.type === 'EQH') {
      for (let k = startK; k <= endIdx; k++) {
        const c = candles[k];
        if (c.close > level.brokenBy) return 'BOS_SUSTAINED';
      }
    } else { // EQL
      for (let k = startK; k <= endIdx; k++) {
        const c = candles[k];
        if (c.close < level.brokenBy) return 'BOS_SUSTAINED';
      }
    }
    return 'BOS_CLOSE';
  }

  _checkLevelStatus(level, candles, candleIndexMap) {
    const resumeFrom = level.lastCheckedIndex ?? level.secondSwingIndex;
    const startIdx   = nextArrayIdx(candleIndexMap, candles, resumeFrom);
    if (startIdx === undefined) return;

    // If level is not yet broken, initialise extreme tracking if needed
    if (level.status !== 'broken') {
      if (level.type === 'EQH') {
        if (level.preBreakoutVDepth === null) {
          level.preBreakoutVDepth = Infinity; // start with highest possible (will be lowered)
        }
      } else {
        if (level.preBreakoutVDepth === null) {
          level.preBreakoutVDepth = -Infinity; // start with lowest possible (will be raised)
        }
      }
    }

    for (let i = startIdx; i < candles.length; i++) {
      const candle = candles[i];
      level.lastCheckedIndex = candle.index;

      // ── Track pre‑breakout extreme if level is still active or swept ──
      if (level.status !== 'broken') {
        if (level.type === 'EQH') {
          // Track the lowest low (deepest pullback)
          if (candle.low < level.preBreakoutVDepth) {
            level.preBreakoutVDepth = candle.low;
            level.preBreakoutVIndex = candle.index;
            level.preBreakoutVTime = candle.time;
            level.preBreakoutVFormattedTime = candle.formattedTime;
          }
        } else { // EQL
          // Track the highest high (highest retracement)
          if (candle.high > level.preBreakoutVDepth) {
            level.preBreakoutVDepth = candle.high;
            level.preBreakoutVIndex = candle.index;
            level.preBreakoutVTime = candle.time;
            level.preBreakoutVFormattedTime = candle.formattedTime;
          }
        }
      }

      if (level.type === 'EQH') {
        const wickBreaches  = candle.high  > level.zoneTop;
        const closeBreaches = candle.close > level.zoneTop;

        if (closeBreaches) {
          level.status              = 'broken';
          level.brokenTime          = candle.time;
          level.brokenFormattedTime = candle.formattedTime;
          level.brokenIndex         = candle.index;
          level.brokenBy            = candle.close;
          level.brokenBosType = this._classifyBosType(level, candles, i + 1, candleIndexMap);
          level.confidence = this._calculateConfidence(level);

          if (this.emitEvents) {
            this.emit('levelBroken', { level, candle });
          }
          break;
        }

        if (level.status === 'active' && wickBreaches) {
          level.status             = 'swept';
          level.sweptTime          = candle.time;
          level.sweptFormattedTime = candle.formattedTime;
          level.sweptIndex         = candle.index;
          level.sweptBy            = candle.high;
          level.confidence = this._calculateConfidence(level);

          if (this.emitEvents) {
            this.emit('levelSwept', { level, candle });
          }
        }

      } else {
        const wickBreaches  = candle.low   < level.zoneBottom;
        const closeBreaches = candle.close < level.zoneBottom;

        if (closeBreaches) {
          level.status              = 'broken';
          level.brokenTime          = candle.time;
          level.brokenFormattedTime = candle.formattedTime;
          level.brokenIndex         = candle.index;
          level.brokenBy            = candle.close;
          level.brokenBosType = this._classifyBosType(level, candles, i + 1, candleIndexMap);
          level.confidence = this._calculateConfidence(level);

          if (this.emitEvents) {
            this.emit('levelBroken', { level, candle });
          }
          break;
        }

        if (level.status === 'active' && wickBreaches) {
          level.status             = 'swept';
          level.sweptTime          = candle.time;
          level.sweptFormattedTime = candle.formattedTime;
          level.sweptIndex         = candle.index;
          level.sweptBy            = candle.low;
          level.confidence = this._calculateConfidence(level);

          if (this.emitEvents) {
            this.emit('levelSwept', { level, candle });
          }
        }
      }
    }
  }

  _upgradePendingBosTypes(symbol, granularity, candles, candleIndexMap, newLevelKeys) {
    const levels = this.store[symbol]?.[granularity] || [];

    for (const level of levels) {
      if (level.status !== 'broken')           continue;
      if (level.brokenBosType !== 'BOS_CLOSE') continue;
      if (newLevelKeys.has(level.key))         continue;

      const startK = nextArrayIdx(candleIndexMap, candles, level.brokenIndex);
      if (startK === undefined) continue;

      const result = this._classifyBosType(level, candles, startK, candleIndexMap);
      if (result === 'BOS_SUSTAINED') {
        level.brokenBosType = 'BOS_SUSTAINED';
        level.confidence = this._calculateConfidence(level);
        this.logger.info(`⬆️ ${level.type} BOS_CLOSE → BOS_SUSTAINED → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop}`);

        if (this.emitEvents) {
          this.emit('levelUpgraded', { level });
        }
        metrics.increment('level_upgrades');
      }
    }
  }

  _insertSorted(symbol, granularity, level) {
    const arr = this.store[symbol][granularity];
    sortedInsert(arr, level, (a, b) => a.time - b.time);

    const config = this._getConfig(symbol, granularity);
    const maxLevels = config.MAX_LEVELS_PER_SYMBOL;
    const maxAgeMs = config.MAX_LEVEL_AGE_MS;

    if (maxLevels > 0 && arr.length > maxLevels) {
      const removed = arr.splice(0, arr.length - maxLevels);
      for (const l of removed) {
        this.indexSets[symbol][granularity].delete(l.key);
      }
      this.logger.debug(`Pruned ${removed.length} old levels for ${symbol} @ ${granularity}s`);
    }

    if (maxAgeMs > 0) {
      const now = Date.now();
      const keepFromIdx = arr.findIndex(l => (now - l.time) <= maxAgeMs);
      if (keepFromIdx > 0) {
        const removed = arr.splice(0, keepFromIdx);
        for (const l of removed) {
          this.indexSets[symbol][granularity].delete(l.key);
        }
        this.logger.debug(`Pruned ${removed.length} aged levels for ${symbol} @ ${granularity}s`);
      }
    }
  }

  _buildSwingIndexMap(allSwings) {
    const map     = new Map();
    const indices = [];
    for (const swing of allSwings) {
      map.set(swing.index, swing);
      indices.push(swing.index);
    }
    return { map, indices };
  }

  // ─────────────────────────────────────────────
  // PUBLIC API
  // ─────────────────────────────────────────────

  async detectAll(symbol, granularity, candles) {
    return this._withLock(symbol, granularity, async () => {
      metrics.increment('eqhEql_detectAll_calls');
      const timer = `detectAll_${symbol}_${granularity}`;
      metrics.startTimer(timer);

      this._initStore(symbol, granularity);

      if (!candles?.length) {
        this.logger.warn(`No candles for ${symbol} @ ${granularity}s — detectAll skipped`);
        metrics.endTimer(timer);
        return [];
      }

      this.store[symbol][granularity]           = [];
      this.indexSets[symbol][granularity]       = new Set();
      this.lastLevel[symbol][granularity]       = null;
      this.lastActiveLevel[symbol][granularity] = null;
      this.lastSwingCount[symbol][granularity]  = { highs: 0, lows: 0 };
      this._resetCounts(symbol, granularity);

      let allSwings, highs, lows;
      try {
        allSwings = swingEngine.get(symbol, granularity);
        highs     = swingEngine.getHighs(symbol, granularity);
        lows      = swingEngine.getLows(symbol, granularity);
      } catch (err) {
        this.logger.error(`Failed to get swings for ${symbol} @ ${granularity}s: ${err.message}`);
        metrics.increment('swingEngine_errors');
        metrics.endTimer(timer);
        return [];
      }

      if (!allSwings.length) {
        this.logger.warn(`No swings for ${symbol} @ ${granularity}s`);
        metrics.endTimer(timer);
        return [];
      }

      const candleIndexMap = buildCandleIndexMap(candles);
      const swingIndexMap  = this._buildSwingIndexMap(allSwings);

      // EQH
      for (let i = 0; i < highs.length - 1; i++) {
        const firstSwing = highs[i];
        for (let j = i + 1; j < highs.length; j++) {
          const secondSwing = highs[j];
          if (secondSwing.price > firstSwing.price)    continue;
          if (secondSwing.price < firstSwing.keyPrice) continue;

          const key = `${firstSwing.index}_${secondSwing.index}_EQH`;
          if (this._isDuplicate(symbol, granularity, key)) continue;

          const level = this._validateAndBuild('EQH', firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap);
          if (!level) continue;

          level.bias = breakoutEngine.getCurrentBias(symbol, granularity);
          this._checkLevelStatus(level, candles, candleIndexMap);
          this._registerLevelCounts(symbol, granularity, level);
          this.store[symbol][granularity].push(level);
          this._registerLevel(symbol, granularity, level);
          if (this.emitEvents) this.emit('newLevel', { level });
        }
      }

      // EQL
      for (let i = 0; i < lows.length - 1; i++) {
        const firstSwing = lows[i];
        for (let j = i + 1; j < lows.length; j++) {
          const secondSwing = lows[j];
          if (secondSwing.price < firstSwing.price)    continue;
          if (secondSwing.price > firstSwing.keyPrice) continue;

          const key = `${firstSwing.index}_${secondSwing.index}_EQL`;
          if (this._isDuplicate(symbol, granularity, key)) continue;

          const level = this._validateAndBuild('EQL', firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap);
          if (!level) continue;

          level.bias = breakoutEngine.getCurrentBias(symbol, granularity);
          this._checkLevelStatus(level, candles, candleIndexMap);
          this._registerLevelCounts(symbol, granularity, level);
          this.store[symbol][granularity].push(level);
          this._registerLevel(symbol, granularity, level);
          if (this.emitEvents) this.emit('newLevel', { level });
        }
      }

      this.store[symbol][granularity].sort((a, b) => a.time - b.time);

      const sorted = this.store[symbol][granularity];
      if (sorted.length) {
        this.lastLevel[symbol][granularity]       = sorted[sorted.length - 1];
        this.lastActiveLevel[symbol][granularity] = this._findLastActive(symbol, granularity);
      }

      this.lastSwingCount[symbol][granularity] = { highs: highs.length, lows: lows.length };

      const c = this.counts[symbol][granularity];
      metrics.set(`levels_${symbol}_${granularity}`, sorted.length);
      metrics.set(`active_${symbol}_${granularity}`, c.get('active'));

      const elapsed = metrics.endTimer(timer);
      this.logger.info(`${symbol} @ ${granularity}s — ${c.get('eqh') + c.get('eql')} levels (EQH: ${c.get('eqh')}, EQL: ${c.get('eql')} | Active: ${c.get('active')}, Broken: ${c.get('broken')}, Swept: ${c.get('swept')}) [${elapsed}ms]`);

      if (this.dataDir) await this._saveToDisk(symbol, granularity);

      return sorted;
    });
  }

  async detectLatest(symbol, granularity, candles) {
    return this._withLock(symbol, granularity, async () => {
      metrics.increment('eqhEql_detectLatest_calls');
      const timer = `detectLatest_${symbol}_${granularity}`;
      metrics.startTimer(timer);

      this._initStore(symbol, granularity);

      if (!candles?.length) {
        this.logger.warn(`No candles for ${symbol} @ ${granularity}s — detectLatest skipped`);
        metrics.endTimer(timer);
        return [];
      }

      let allSwings, highs, lows;
      try {
        allSwings = swingEngine.get(symbol, granularity);
        highs     = swingEngine.getHighs(symbol, granularity);
        lows      = swingEngine.getLows(symbol, granularity);
      } catch (err) {
        this.logger.error(`Failed to get swings for ${symbol} @ ${granularity}s: ${err.message}`);
        metrics.increment('swingEngine_errors');
        metrics.endTimer(timer);
        return [];
      }

      if (!allSwings.length) {
        metrics.endTimer(timer);
        return [];
      }

      const lastCounts    = this.lastSwingCount[symbol][granularity];
      const newHighsStart = lastCounts.highs;
      const newLowsStart  = lastCounts.lows;

      const candleIndexMap = buildCandleIndexMap(candles);
      const swingIndexMap  = this._buildSwingIndexMap(allSwings);
      const newLevels      = [];

      // New EQH
      for (let j = newHighsStart; j < highs.length; j++) {
        const secondSwing = highs[j];
        for (let i = 0; i < j; i++) {
          const firstSwing = highs[i];
          if (secondSwing.price > firstSwing.price)    continue;
          if (secondSwing.price < firstSwing.keyPrice) continue;

          const key = `${firstSwing.index}_${secondSwing.index}_EQH`;
          if (this._isDuplicate(symbol, granularity, key)) continue;

          const level = this._validateAndBuild('EQH', firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap);
          if (!level) continue;

          level.bias = breakoutEngine.getCurrentBias(symbol, granularity);
          this._checkLevelStatus(level, candles, candleIndexMap);
          this._registerLevelCounts(symbol, granularity, level);
          this._updateLastLevel(symbol, granularity, level);
          this._insertSorted(symbol, granularity, level);
          this._registerLevel(symbol, granularity, level);
          newLevels.push(level);
          this.logger.info(`🔴 New EQH → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom.toFixed(2)} - ${level.zoneTop.toFixed(2)} | V-Shape (between swings): ${level.vShapeDepth?.toFixed(2) ?? 'N/A'} @ ${level.vShapeFormattedTime ?? 'N/A'} | Pre-Breakout Extreme: ${level.preBreakoutVDepth?.toFixed(2) ?? 'N/A'} @ ${level.preBreakoutVFormattedTime ?? 'N/A'} | Status: ${level.status} | Confidence: ${level.confidence.toFixed(2)} | ${level.formattedTime}`);
          if (this.emitEvents) this.emit('newLevel', { level });
          metrics.increment('new_levels');
        }
      }

      // New EQL
      for (let j = newLowsStart; j < lows.length; j++) {
        const secondSwing = lows[j];
        for (let i = 0; i < j; i++) {
          const firstSwing = lows[i];
          if (secondSwing.price < firstSwing.price)    continue;
          if (secondSwing.price > firstSwing.keyPrice) continue;

          const key = `${firstSwing.index}_${secondSwing.index}_EQL`;
          if (this._isDuplicate(symbol, granularity, key)) continue;

          const level = this._validateAndBuild('EQL', firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap);
          if (!level) continue;

          level.bias = breakoutEngine.getCurrentBias(symbol, granularity);
          this._checkLevelStatus(level, candles, candleIndexMap);
          this._registerLevelCounts(symbol, granularity, level);
          this._updateLastLevel(symbol, granularity, level);
          this._insertSorted(symbol, granularity, level);
          this._registerLevel(symbol, granularity, level);
          newLevels.push(level);
          this.logger.info(`🟢 New EQL → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom.toFixed(2)} - ${level.zoneTop.toFixed(2)} | V-Shape (between swings): ${level.vShapeDepth?.toFixed(2) ?? 'N/A'} @ ${level.vShapeFormattedTime ?? 'N/A'} | Pre-Breakout Extreme: ${level.preBreakoutVDepth?.toFixed(2) ?? 'N/A'} @ ${level.preBreakoutVFormattedTime ?? 'N/A'} | Status: ${level.status} | Confidence: ${level.confidence.toFixed(2)} | ${level.formattedTime}`);
          if (this.emitEvents) this.emit('newLevel', { level });
          metrics.increment('new_levels');
        }
      }

      lastCounts.highs = highs.length;
      lastCounts.lows  = lows.length;

      const newLevelKeys = new Set(newLevels.map((l) => l.key));
      this._updateActiveStatuses(symbol, granularity, candles, candleIndexMap, newLevelKeys);
      this._repairCachesIfNeeded(symbol, granularity);

      const elapsed = metrics.endTimer(timer);
      this.logger.debug(`detectLatest for ${symbol} @ ${granularity}s completed in ${elapsed}ms, ${newLevels.length} new levels`);

      if (this.dataDir && newLevels.length > 0) await this._saveToDisk(symbol, granularity);

      return newLevels;
    });
  }

  _updateActiveStatuses(symbol, granularity, candles, candleIndexMap, newLevelKeys = new Set()) {
    const levels = this.store[symbol]?.[granularity] || [];
    const c      = this.counts[symbol][granularity];

    let activeCacheInvalidated = false;

    if (c.get('active') > 0 || c.get('swept') > 0) {
      for (const level of levels) {
        if (level.status === 'broken')   continue;
        if (newLevelKeys.has(level.key)) continue;

        const prevStatus = level.status;
        this._checkLevelStatus(level, candles, candleIndexMap);
        if (level.status === prevStatus) continue;

        if (prevStatus === 'active' && level.status === 'swept') {
          c.dec('active');
          c.inc('swept');
          this.logger.info(`🧹 ${level.type} Swept → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop} | ${level.sweptFormattedTime}`);
          if (this.lastActiveLevel[symbol]?.[granularity]?.key === level.key) activeCacheInvalidated = true;
        }
        else if (prevStatus === 'active' && level.status === 'broken') {
          c.dec('active');
          c.inc('broken');
          this.logger.info(`❌ ${level.type} Broken → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop} | ${level.brokenFormattedTime}`);
          if (this.lastActiveLevel[symbol]?.[granularity]?.key === level.key) activeCacheInvalidated = true;
        }
        else if (prevStatus === 'swept' && level.status === 'broken') {
          c.dec('swept');
          c.inc('broken');
          this.logger.info(`❌ ${level.type} Broken (was Swept) → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop} | ${level.brokenFormattedTime}`);
        }
      }

      if (activeCacheInvalidated) {
        this.lastActiveLevel[symbol][granularity] = this._findLastActive(symbol, granularity);
      }
    }

    this._upgradePendingBosTypes(symbol, granularity, candles, candleIndexMap, newLevelKeys);
  }

  // ─────────────────────────────────────────────
  // GETTERS (immutable)
  // ─────────────────────────────────────────────

  get(symbol, granularity) {
    const arr = this.store[symbol]?.[granularity];
    return arr ? [...arr] : [];
  }

  /**
   * Get a level with full field visibility (useful for debugging)
   * Returns all fields including pre-breakout V-shape data
   */
  getLevelDetail(symbol, granularity, levelKey) {
    const levels = this.store[symbol]?.[granularity] || [];
    const level = levels.find(l => l.key === levelKey);
    
    if (!level) return null;
    
    // Explicitly return all fields to ensure nothing is missing
    return {
      // Basic info
      type: level.type,
      key: level.key,
      
      // Zone info
      zoneTop: level.zoneTop,
      zoneBottom: level.zoneBottom,
      zoneMid: level.zoneMid,
      
      // First swing
      firstSwingIndex: level.firstSwingIndex,
      firstSwingPrice: level.firstSwingPrice,
      firstSwingKeyPrice: level.firstSwingKeyPrice,
      firstSwingDirection: level.firstSwingDirection,
      firstSwingTime: level.firstSwingTime,
      firstSwingFormattedTime: level.firstSwingFormattedTime,
      
      // Second swing
      secondSwingIndex: level.secondSwingIndex,
      secondSwingPrice: level.secondSwingPrice,
      secondSwingKeyPrice: level.secondSwingKeyPrice,
      secondSwingDirection: level.secondSwingDirection,
      secondSwingTime: level.secondSwingTime,
      secondSwingFormattedTime: level.secondSwingFormattedTime,
      
      // V-Shape BETWEEN swings
      vShapeDepth: level.vShapeDepth,
      vShapeIndex: level.vShapeIndex,
      vShapeTime: level.vShapeTime,
      vShapeFormattedTime: level.vShapeFormattedTime,
      candlesBetween: level.candlesBetween,
      
      // V-Shape AFTER second swing (PRE-BREAKOUT)
      preBreakoutVDepth: level.preBreakoutVDepth,
      preBreakoutVIndex: level.preBreakoutVIndex,
      preBreakoutVTime: level.preBreakoutVTime,
      preBreakoutVFormattedTime: level.preBreakoutVFormattedTime,
      
      // Status
      status: level.status,
      brokenTime: level.brokenTime,
      brokenFormattedTime: level.brokenFormattedTime,
      brokenIndex: level.brokenIndex,
      brokenBy: level.brokenBy,
      brokenBosType: level.brokenBosType,
      sweptTime: level.sweptTime,
      sweptFormattedTime: level.sweptFormattedTime,
      sweptIndex: level.sweptIndex,
      sweptBy: level.sweptBy,
      
      // Other
      lastCheckedIndex: level.lastCheckedIndex,
      bias: level.bias,
      confidence: level.confidence,
      formattedTime: level.formattedTime,
      time: level.time,
      date: level.date,
    };
  }

  getAll() {
    const copy = {};
    for (const [symbol, symData] of Object.entries(this.store)) {
      copy[symbol] = {};
      for (const [gran, levels] of Object.entries(symData)) {
        copy[symbol][gran] = [...levels];
      }
    }
    return copy;
  }

  getLastN(symbol, granularity, n) {
    const arr = this.store[symbol]?.[granularity];
    return arr ? arr.slice(-n) : [];
  }

  getEQH(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.type === 'EQH');
  }

  getEQL(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.type === 'EQL');
  }

  getActive(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.status === 'active');
  }

  getActiveEQH(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.type === 'EQH' && l.status === 'active');
  }

  getActiveEQL(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.type === 'EQL' && l.status === 'active');
  }

  getBroken(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.status === 'broken');
  }

  getBrokenEQH(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.type === 'EQH' && l.status === 'broken');
  }

  getBrokenEQL(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.type === 'EQL' && l.status === 'broken');
  }

  getSwept(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.status === 'swept');
  }

  getSweptEQH(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.type === 'EQH' && l.status === 'swept');
  }

  getSweptEQL(symbol, granularity) {
    return this.get(symbol, granularity).filter(l => l.type === 'EQL' && l.status === 'swept');
  }

  getLatest(symbol, granularity) {
    return this.lastLevel[symbol]?.[granularity] || null;
  }

  getLatestActive(symbol, granularity) {
    return this.lastActiveLevel[symbol]?.[granularity] || null;
  }

  getSummary(symbol, granularity) {
    const c = this.counts[symbol]?.[granularity] || new Counter({ eqh: 0, eql: 0, active: 0, broken: 0, swept: 0 });
    return {
      symbol,
      granularity,
      total:   this.get(symbol, granularity).length,
      eqh:     c.get('eqh'),
      eql:     c.get('eql'),
      active:  c.get('active'),
      broken:  c.get('broken'),
      swept:   c.get('swept'),
      latest:  this.getLatest(symbol, granularity),
      bias:    breakoutEngine.getCurrentBias(symbol, granularity),
    };
  }

  getMetrics() {
    return metrics.getAll();
  }

  // ─────────────────────────────────────────────
  // CLEAR
  // ─────────────────────────────────────────────

  clearStore(symbol, granularity) {
    if (this.store[symbol])           this.store[symbol][granularity]           = [];
    if (this.indexSets[symbol])       this.indexSets[symbol][granularity]       = new Set();
    if (this.counts[symbol])          this._resetCounts(symbol, granularity);
    if (this.lastLevel[symbol])       this.lastLevel[symbol][granularity]       = null;
    if (this.lastActiveLevel[symbol]) this.lastActiveLevel[symbol][granularity] = null;
    if (this.lastSwingCount[symbol])  this.lastSwingCount[symbol][granularity]  = { highs: 0, lows: 0 };
    this.logger.info(`Store cleared → ${symbol} @ ${granularity}s`);
  }

  clearAll() {
    Object.keys(this.store).forEach((symbol) => {
      Object.keys(this.store[symbol]).forEach((g) => {
        this.store[symbol][g]           = [];
        this.indexSets[symbol][g]       = new Set();
        this._resetCounts(symbol, g);
        this.lastLevel[symbol][g]       = null;
        this.lastActiveLevel[symbol][g] = null;
        this.lastSwingCount[symbol][g]  = { highs: 0, lows: 0 };
      });
    });
    this.logger.info(`Full store cleared`);
  }
}

module.exports = new EqhEqlEngine();