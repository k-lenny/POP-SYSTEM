const EventEmitter = require('events');
const swingEngine = require('../../signals/dataProcessor/swings');
const { processOBLV } = require('../../signals/dataProcessor/OBLV');
const Logger = require('../../utils/logger');
const { getConfig } = require('../../config');

const DIRECTION = {
  BULLISH: 'bullish',
  BEARISH: 'bearish'
};

const STATUS = {
  S_SETUP: 'S SETUP',
  DOUBLE_EQ: 'DOUBLE EQ',
  ACTIVE: 'ACTIVE',
  EXPIRED: 'EXPIRED'
};

class PatternEngine extends EventEmitter {
  constructor(options = {}) {
    super();
    this.store = {};
    this.logger = options.logger || new Logger('PatternEngine');
    this.emitEvents = options.emitEvents ?? getConfig().ENABLE_EVENTS;
    this.debugMode = options.debugMode ?? false;

    // Configurable parameters
    this.config = {
      vShapeWickRatio: 2.0,
      equalLevelTolerance: 0.002,
      retestScanRange: 30,
      obScanRange: 10,
      ...options.config
    };

    // Caches
    this._swingCache = new Map();
    this._candleCache = new Map();
    this._oblvCache = new Map();
    this.maxConcurrency = options.maxConcurrency || 4;
  }

  // ---------- Debug logging ----------
  _debugLog(...args) {
    if (this.debugMode) this.logger.debug(...args);
  }

  // ---------- Caching helpers ----------
  _getCacheKey(symbol, granularity) {
    return `${symbol}|${granularity}`;
  }

  async _getSwings(symbol, granularity, candles) {
    const key = this._getCacheKey(symbol, granularity);
    if (!this._swingCache.has(key)) {
      await swingEngine.detectAll(symbol, granularity, candles.slice(0, -1), 1);
      const swings = swingEngine.get(symbol, granularity) || [];
      this._swingCache.set(key, swings);
    }
    return this._swingCache.get(key);
  }

  _getCachedCandles(symbol, granularity, candles) {
    const key = this._getCacheKey(symbol, granularity);
    const cached = this._candleCache.get(key);
    const lastTimestamp = candles[candles.length - 1]?.timestamp;
    if (cached && cached.length === candles.length && cached.lastTimestamp === lastTimestamp) {
      return cached;
    }

    const enriched = this._enrichCandles(candles);
    const candleMap = new Map();
    for (const c of enriched) {
      candleMap.set(c.formattedTime, c);
    }

    const oblvData = this._getOBLV(symbol, granularity, enriched);
    const obMap = new Map();
    oblvData.forEach(entry => {
      if (entry.OB && entry.OBFormattedTime) {
        const candle = candleMap.get(entry.OBFormattedTime);
        if (candle && !obMap.has(candle.index)) {
          obMap.set(candle.index, {
            index: candle.index,
            formattedTime: entry.OBFormattedTime,
            high: entry.OB.high,
            low: entry.OB.low
          });
        }
      }
    });

    const cacheEntry = {
      enriched,
      obMap,
      candleMap,
      length: candles.length,
      lastTimestamp
    };
    this._candleCache.set(key, cacheEntry);
    return cacheEntry;
  }

  _getOBLV(symbol, granularity, candles) {
    const key = this._getCacheKey(symbol, granularity);
    if (!this._oblvCache.has(key)) {
      const oblvData = processOBLV(symbol, granularity, candles);
      this._oblvCache.set(key, oblvData);
    }
    return this._oblvCache.get(key);
  }

  // ---------- Store init ----------
  _initStore(symbol, granularity) {
    if (!this.store[symbol]) this.store[symbol] = {};
    if (!this.store[symbol][granularity]) this.store[symbol][granularity] = [];
  }

  // ---------- Swing indexing ----------
  _buildSwingIndex(swings) {
    const indexMap = new Map();
    const prevSameType = new Map();
    const nextSameType = new Map();

    let lastHigh = null, lastLow = null;
    for (let i = 0; i < swings.length; i++) {
      const s = swings[i];
      indexMap.set(s.index, s);
      if (s.type === 'high') {
        prevSameType.set(i, lastHigh);
        lastHigh = s;
      } else {
        prevSameType.set(i, lastLow);
        lastLow = s;
      }
    }

    lastHigh = lastLow = null;
    for (let i = swings.length - 1; i >= 0; i--) {
      const s = swings[i];
      if (s.type === 'high') {
        nextSameType.set(i, lastHigh);
        lastHigh = s;
      } else {
        nextSameType.set(i, lastLow);
        lastLow = s;
      }
    }

    return { indexMap, prevSameType, nextSameType };
  }

  _enrichCandles(candles) {
    return candles.map((candle, index) => ({
      ...candle,
      index,
      bodySize: Math.abs(candle.open - candle.close),
      upperWick: candle.high - Math.max(candle.open, candle.close),
      lowerWick: Math.min(candle.open, candle.close) - candle.low
    }));
  }

  _formatTime(timestamp) {
    if (!timestamp) return null;
    const date = new Date(Number(timestamp) * 1000);
    return date.toISOString().replace('T', ' ').substring(0, 19);
  }

  _getPatternDirection(currentSwing, previousSwing) {
    if (currentSwing.type === 'low' && previousSwing.type === 'low') return DIRECTION.BULLISH;
    if (currentSwing.type === 'high' && previousSwing.type === 'high') return DIRECTION.BEARISH;
    return null;
  }

  _findPreviousSameTypeSwing(swings, currentIndex, type) {
    for (let i = currentIndex - 1; i >= 0; i--) {
      if (swings[i].type === type) return swings[i];
    }
    return null;
  }

  _findNextSameTypeSwing(swings, currentIndex, type) {
    for (let i = currentIndex + 1; i < swings.length; i++) {
      if (swings[i].type === type) return swings[i];
    }
    return null;
  }

  _findPreviousExtremeSwing(swings, currentIndex, direction, currentSwingLevel, minIndex = 0) {
    const type = direction === DIRECTION.BULLISH ? 'low' : 'high';
    let extremeSwing = null;
    let extremeValue = direction === DIRECTION.BULLISH ? Infinity : -Infinity;

    for (let i = currentIndex - 1; i >= 0; i--) {
      if (swings[i].type === type && swings[i].index >= minIndex) {
        if (direction === DIRECTION.BULLISH) {
          if (swings[i].low > currentSwingLevel && swings[i].low < extremeValue) {
            extremeValue = swings[i].low;
            extremeSwing = swings[i];
          }
        } else {
          if (swings[i].high < currentSwingLevel && swings[i].high > extremeValue) {
            extremeValue = swings[i].high;
            extremeSwing = swings[i];
          }
        }
      }
    }
    return extremeSwing;
  }

  _findConfirmedSetupRetest(vshape, currentSwing, breakout, candles, direction) {
    if (!breakout || !vshape || !currentSwing) return null;
    const startIdx = breakout.index + 1;
    if (startIdx >= candles.length) return null;

    const vshapePrice = direction === DIRECTION.BULLISH ? vshape.high : vshape.low;
    const currentSwingPrice = direction === DIRECTION.BULLISH ? currentSwing.low : currentSwing.high;
    let retestCandle = null;

    if (direction === DIRECTION.BULLISH) {
      let extremeLow = Infinity;
      for (let i = startIdx; i < candles.length; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low >= currentSwingPrice && c.low <= vshapePrice && c.low < extremeLow) {
          extremeLow = c.low;
          retestCandle = c;
        }
      }
    } else {
      let extremeHigh = -Infinity;
      for (let i = startIdx; i < candles.length; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.high <= currentSwingPrice && c.high >= vshapePrice && c.high > extremeHigh) {
          extremeHigh = c.high;
          retestCandle = c;
        }
      }
    }
    return retestCandle;
  }

  _findOBForSetup(startIndex, obMap, direction, vShapeCandle, maxScanCandles = this.config.obScanRange) {
    if (!vShapeCandle || !obMap) return null;
    const vShapeLow = vShapeCandle.low;
    const vShapeHigh = vShapeCandle.high;
    const endIndex = startIndex + maxScanCandles;

    const relevantOBs = Array.from(obMap.entries())
      .filter(([idx]) => idx >= startIndex && idx <= endIndex)
      .sort((a, b) => a[0] - b[0]);

    for (const [idx, ob] of relevantOBs) {
      if (direction === DIRECTION.BULLISH) {
        if (ob.low < vShapeLow) return ob;
      } else {
        if (ob.high > vShapeHigh) return ob;
      }
    }
    return null;
  }

  _findCandle2PreviousMitigation(previousSwingIndex, candle1Index, candles, direction) {
    if (previousSwingIndex == null || !candle1Index) return null;
    let mitigationCandle = null;
    if (direction === DIRECTION.BULLISH) {
      let maxHigh = -Infinity;
      for (let i = previousSwingIndex; i < candle1Index; i++) {
        const candle = candles[i];
        if (candle && candle.close >= candle.open && candle.high > maxHigh) {
          maxHigh = candle.high;
          mitigationCandle = candle;
        }
      }
    } else {
      let minLow = Infinity;
      for (let i = previousSwingIndex; i < candle1Index; i++) {
        const candle = candles[i];
        if (candle && candle.close < candle.open && candle.low < minLow) {
          minLow = candle.low;
          mitigationCandle = candle;
        }
      }
    }
    return mitigationCandle;
  }

  _findCandle2NextMitigation(candle1Index, candle2NextIndex, candles, direction) {
    if (!candle1Index || !candle2NextIndex) return null;
    let mitigationCandle = null;
    if (direction === DIRECTION.BULLISH) {
      let maxHigh = -Infinity;
      for (let i = candle1Index; i < candle2NextIndex; i++) {
        const candle = candles[i];
        if (candle && candle.close >= candle.open && candle.high > maxHigh) {
          maxHigh = candle.high;
          mitigationCandle = candle;
        }
      }
    } else {
      let minLow = Infinity;
      for (let i = candle1Index; i < candle2NextIndex; i++) {
        const candle = candles[i];
        if (candle && candle.close < candle.open && candle.low < minLow) {
          minLow = candle.low;
          mitigationCandle = candle;
        }
      }
    }
    return mitigationCandle;
  }

  _checkMitigationRetest(mtCandle, startIndex, candles, direction) {
    if (!mtCandle) return null;
    const mtLow = mtCandle.low;
    const mtHigh = mtCandle.high;
    let firstCrossCandle = null;

    for (let i = startIndex; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === DIRECTION.BULLISH) {
        if (c.low < mtLow) { firstCrossCandle = c; break; }
      } else {
        if (c.high > mtHigh) { firstCrossCandle = c; break; }
      }
    }
    if (!firstCrossCandle) return null;

    const refValue = direction === DIRECTION.BULLISH ? firstCrossCandle.low : firstCrossCandle.high;
    for (let i = firstCrossCandle.index + 1; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === DIRECTION.BULLISH) {
        if (c.close < refValue || c.open < refValue) return STATUS.EXPIRED;
      } else {
        if (c.close > refValue || c.open > refValue) return STATUS.EXPIRED;
      }
    }
    return STATUS.ACTIVE;
  }

  // ---------- Main detection ----------
  async detect(symbol, granularity, candles) {
    this._initStore(symbol, granularity);

    const swings = await this._getSwings(symbol, granularity, candles);
    const swingIndex = this._buildSwingIndex(swings);

    this.logger.info(`[PatternEngine] Detecting patterns for ${symbol} ${granularity}`);
    this.logger.info(`[PatternEngine] Candles: ${candles.length}, Swings: ${swings.length}`);

    if (swings.length < 2) {
      this.logger.warn(`[PatternEngine] Not enough swings (${swings.length}) for pattern detection`);
      return [];
    }

    const { enriched: enrichedCandles, obMap } = this._getCachedCandles(symbol, granularity, candles);

    const patterns = [];
    const rejectionStats = {
      noPreviousSameType: 0,
      noSweep: 0,
      noVShape: 0,
      noBreakout: 0,
      total: 0
    };

    for (let i = 1; i < swings.length; i++) {
      const currentSwing = swings[i];
      rejectionStats.total++;

      const previousSwing = this._findPreviousSameTypeSwing(swings, i, currentSwing.type);
      if (!previousSwing) {
        rejectionStats.noPreviousSameType++;
        continue;
      }

      const direction = this._getPatternDirection(currentSwing, previousSwing);
      const setup = this.identifySetup(currentSwing, previousSwing, enrichedCandles, direction, swings, swingIndex);

      if (setup) {
        this._buildPatternStages(setup, enrichedCandles, direction, swings, swingIndex, obMap, patterns);
        const enrichedPattern = this._enrichPatternMetadata(setup, enrichedCandles);
        patterns.push(enrichedPattern);
        this.logger.info(`[PatternEngine] Found ${direction} pattern from swing ${previousSwing.index} to ${currentSwing.index}`);
        if (this.emitEvents) {
          this.emit('patternDetected', { symbol, granularity, pattern: enrichedPattern });
        }
      }
    }

    this.logger.info(`[PatternEngine] Total patterns detected: ${patterns.length}`);
    this.logger.info(`[PatternEngine] Rejection stats:`, rejectionStats);
    this.store[symbol][granularity] = patterns;
    return patterns;
  }

  async detectBatch(tasks, concurrency = this.maxConcurrency) {
    const results = [];
    const queue = [...tasks];
    const inProgress = new Set();

    const runTask = async (task) => {
      const { symbol, granularity, candles } = task;
      const patterns = await this.detect(symbol, granularity, candles);
      results.push({ symbol, granularity, patterns });
    };

    while (queue.length || inProgress.size) {
      while (queue.length && inProgress.size < concurrency) {
        const task = queue.shift();
        const promise = runTask(task).finally(() => inProgress.delete(promise));
        inProgress.add(promise);
      }
      if (inProgress.size) await Promise.race(inProgress);
    }
    return results;
  }

  // ---------- Pattern enrichment ----------
  _enrichPatternMetadata(pattern, candles) {
    const result = {
      type: 'PATTERN',
      direction: pattern.direction,

      previousSwingIndex: pattern.previousSwing?.index || null,
      previousSwingPrice: pattern.previousSwing?.type === 'high'
        ? pattern.previousSwing?.high
        : pattern.previousSwing?.low,
      previousSwingType: pattern.previousSwing?.type || null,
      previousSwingTime: pattern.previousSwing?.time || null,
      previousSwingFormattedTime: this._formatTime(pattern.previousSwing?.time),

      currentSwingIndex: pattern.currentSwing?.index || null,
      currentSwingPrice: pattern.currentSwing?.type === 'high'
        ? pattern.currentSwing?.high
        : pattern.currentSwing?.low,
      currentSwingType: pattern.currentSwing?.type || null,
      currentSwingTime: pattern.currentSwing?.time || null,
      currentSwingFormattedTime: this._formatTime(pattern.currentSwing?.time),

      firstSweepCandleIndex: pattern.sweepData?.firstSweepCandleIndex || null,
      firstSweepCandleClose: pattern.sweepData?.firstSweepCandleClose || null,

      vShapeCandleIndex: pattern.vShapeCandle?.index || null,
      vShapeCandlePrice: pattern.direction === DIRECTION.BULLISH
        ? pattern.vShapeCandle?.high
        : pattern.vShapeCandle?.low,
      vShapeCandleTime: pattern.vShapeCandle?.time || null,
      vShapeCandleFormattedTime: this._formatTime(pattern.vShapeCandle?.time),
      vShapeWickSize: pattern.direction === DIRECTION.BULLISH
        ? pattern.vShapeCandle?.lowerWick
        : pattern.vShapeCandle?.upperWick,

      breakoutIndex: pattern.breakout?.index || null,
      breakoutPrice: pattern.breakout?.close || null,
      breakoutTime: pattern.breakout?.time || null,
      breakoutFormattedTime: this._formatTime(pattern.breakout?.time),

      retestIndex: pattern.retest?.index || null,
      retestPrice: pattern.retest?.close || null,
      retestTime: pattern.retest?.time || null,
      retestFormattedTime: this._formatTime(pattern.retest?.time),

      formattedTime: this._formatTime(pattern.currentSwing?.time),
      time: pattern.currentSwing?.time || null,
      timestamp: pattern.timestamp || Date.now(),
    };
    return result;
  }

  _buildPatternStages(setup, candles, direction, swings, swingIndex, obMap, patterns) {
    const retest = this.identifyRetest(setup, candles, direction);
    if (!retest) return;
    setup.retest = retest;

    const confirmedSetup = this.identifyConfirmedSetup(retest, candles, direction, swings, swingIndex, obMap);
    if (!confirmedSetup) return;
    setup.confirmedSetup = confirmedSetup;

    // Every swing involved in this pattern is eligible to form its own S setup pattern
    const candidateIndices = [
      confirmedSetup.candle1?.index,
      confirmedSetup.candle2Previous?.index,
      confirmedSetup.candle2Next?.index,
      retest.index,
      retest.vShapeCandle?.index,
      retest.breakout?.index,
    ].filter(idx => idx != null);

    for (const idx of candidateIndices) {
      const swing = swingIndex.indexMap.get(idx);
      if (!swing) continue;

      const swingPos = swings.findIndex(s => s.index === swing.index);
      if (swingPos < 1) continue;

      const prevSwing = this._findPreviousSameTypeSwing(swings, swingPos, swing.type);
      if (!prevSwing) continue;

      const dir = this._getPatternDirection(swing, prevSwing);
      if (!dir) continue;

      const alreadyExists = patterns.some(p =>
        p.previousSwingIndex === prevSwing.index && p.currentSwingIndex === swing.index
      );
      if (alreadyExists) continue;

      const newSetup = this.identifySetup(swing, prevSwing, candles, dir, swings, swingIndex);
      if (newSetup) {
        this._buildPatternStages(newSetup, candles, dir, swings, swingIndex, obMap, patterns);
        const enrichedNewPattern = this._enrichPatternMetadata(newSetup, candles);
        patterns.push(enrichedNewPattern);
        this.logger.info(`[PatternEngine] Added S setup pattern from swing ${prevSwing.index} to ${swing.index}`);
      }
    }
  }

  // ---------- Setup identification ----------
  identifySetup(currentSwing, previousSwing, candles, direction, swings, swingIndex) {
    const sweepData = this.isWickSweep(currentSwing, previousSwing, candles, direction);
    if (!sweepData.isSweep) {
      this._debugLog(`[identifySetup] No sweep detected between swing ${previousSwing.index} and ${currentSwing.index} (${direction})`);
      return null;
    }

    const vShapeCandle = this.findVShapeCandle(previousSwing, currentSwing, candles, direction);
    if (!vShapeCandle) {
      this._debugLog(`[identifySetup] No V-shape candle found between swing ${previousSwing.index} and ${currentSwing.index} (${direction})`);
      return null;
    }

    const breakoutLevel = direction === DIRECTION.BULLISH ? vShapeCandle.high : vShapeCandle.low;
    const breakout = this.identifyBreakoutOfLevel(breakoutLevel, candles, currentSwing.index, direction, currentSwing);
    if (!breakout) {
      this._debugLog(`[identifySetup] No breakout found for swing ${currentSwing.index} (${direction}, level: ${breakoutLevel})`);
      return null;
    }

    this._debugLog(`[identifySetup] Valid setup found: sweep at ${sweepData.firstSweepCandleIndex}, v-shape at ${vShapeCandle.index}, breakout at ${breakout.index}`);

    return {
      type: 'setup',
      direction,
      currentSwing,
      previousSwing,
      vShapeCandle,
      breakout,
      sweepData,
      timestamp: candles[currentSwing.index]?.timestamp || Date.now()
    };
  }

  isWickSweep(currentSwing, previousSwing, candles, direction) {
    const result = { isSweep: false, firstSweepCandleIndex: null, firstSweepCandleClose: null };
    if (currentSwing.type !== previousSwing.type) return result;

    const startIndex = previousSwing.index + 1;
    const endIndex = currentSwing.index;

    if (direction === DIRECTION.BULLISH) {
      let firstSweepCandle = null;
      for (let i = startIndex; i <= endIndex; i++) {
        const candle = candles[i];
        if (candle.low < previousSwing.low) {
          firstSweepCandle = candle;
          break;
        }
      }
      if (!firstSweepCandle) return result;

      for (let i = firstSweepCandle.index + 1; i <= endIndex; i++) {
        const candle = candles[i];
        if (candle.close < firstSweepCandle.low || candle.open < firstSweepCandle.low) {
          this._debugLog(`[isWickSweep] Sweep invalidated: candle ${i} close/open went below first sweep candle low (${firstSweepCandle.low})`);
          return result;
        }
      }
      result.isSweep = true;
      result.firstSweepCandleIndex = firstSweepCandle.index;
      result.firstSweepCandleClose = firstSweepCandle.close;
    } else {
      let firstSweepCandle = null;
      for (let i = startIndex; i <= endIndex; i++) {
        const candle = candles[i];
        if (candle.high > previousSwing.high) {
          firstSweepCandle = candle;
          break;
        }
      }
      if (!firstSweepCandle) return result;

      for (let i = firstSweepCandle.index + 1; i <= endIndex; i++) {
        const candle = candles[i];
        if (candle.close > firstSweepCandle.high || candle.open > firstSweepCandle.high) {
          this._debugLog(`[isWickSweep] Sweep invalidated: candle ${i} close/open went above first sweep candle high (${firstSweepCandle.high})`);
          return result;
        }
      }
      result.isSweep = true;
      result.firstSweepCandleIndex = firstSweepCandle.index;
      result.firstSweepCandleClose = firstSweepCandle.close;
    }
    return result;
  }

  findVShapeCandle(swing1, swing2, candles, direction) {
    const minSwingIndex = Math.min(swing1.index, swing2.index);
    const maxSwingIndex = Math.max(swing1.index, swing2.index);
    const startIndex = minSwingIndex + 1;
    const endIndex = maxSwingIndex;
    if (endIndex <= startIndex) return null;

    let extremumCandle = null;
    if (direction === DIRECTION.BULLISH) {
      let maxHigh = -Infinity;
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.high > maxHigh) {
          maxHigh = candle.high;
          extremumCandle = candle;
        }
      }
      const previousSwingCandle = candles[minSwingIndex];
      if (previousSwingCandle && previousSwingCandle.high > maxHigh) {
        extremumCandle = previousSwingCandle;
      }
    } else {
      let minLow = Infinity;
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.low < minLow) {
          minLow = candle.low;
          extremumCandle = candle;
        }
      }
      const previousSwingCandle = candles[minSwingIndex];
      if (previousSwingCandle && previousSwingCandle.low < minLow) {
        extremumCandle = previousSwingCandle;
      }
    }
    return extremumCandle;
  }

  identifyBreakoutOfLevel(level, candles, startIndex, direction, currentSwing) {
    let firstCrossCandle = null;

    for (let i = startIndex; i < candles.length; i++) {
      const candle = candles[i];
      if (candle.index === currentSwing.index) continue;

      if (direction === DIRECTION.BULLISH) {
        if (candle.low < currentSwing.low) return null;

        if (!firstCrossCandle) {
          if (candle.close > level || candle.open > level) return candle;
          if (candle.high > level) { firstCrossCandle = candle; continue; }
        } else {
          if (candle.close > firstCrossCandle.high || candle.open > firstCrossCandle.high) return candle;
          if (candle.high > firstCrossCandle.high) firstCrossCandle = candle;
        }
      } else {
        if (candle.high > currentSwing.high) return null;

        if (!firstCrossCandle) {
          if (candle.close < level || candle.open < level) return candle;
          if (candle.low < level) { firstCrossCandle = candle; continue; }
        } else {
          if (candle.close < firstCrossCandle.low || candle.open < firstCrossCandle.low) return candle;
          if (candle.low < firstCrossCandle.low) firstCrossCandle = candle;
        }
      }
    }
    return null;
  }

  // ---------- Retest ----------
  identifyRetest(setup, candles, direction) {
    if (!setup.breakout || !setup.vShapeCandle || !setup.currentSwing) return null;

    const breakoutStartIndex = setup.breakout.index + 1;
    if (breakoutStartIndex >= candles.length) return null;

    const vShapePrice = direction === DIRECTION.BULLISH ? setup.vShapeCandle.high : setup.vShapeCandle.low;
    const currentSwingPrice = direction === DIRECTION.BULLISH ? setup.currentSwing.low : setup.currentSwing.high;
    const breakoutExtreme = direction === DIRECTION.BULLISH ? setup.breakout.high : setup.breakout.low;

    let crossCandleIndex = null;
    for (let i = breakoutStartIndex; i < candles.length; i++) {
      const candle = candles[i];
      if (!candle) continue;
      if (direction === DIRECTION.BULLISH) {
        if (candle.high > breakoutExtreme) { crossCandleIndex = i; break; }
      } else {
        if (candle.low < breakoutExtreme) { crossCandleIndex = i; break; }
      }
    }
    if (crossCandleIndex === null) {
      this._debugLog(`[identifyRetest] No candle crossed beyond breakout extreme`);
      return null;
    }

    const startIndex = crossCandleIndex + 1;
    const maxScanRange = this.config.retestScanRange;
    const endIndex = Math.min(startIndex + maxScanRange, candles.length);
    let extremumCandle = null;

    if (direction === DIRECTION.BULLISH) {
      let minLow = Infinity;
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.low < currentSwingPrice) {
          this._debugLog(`[identifyRetest] Retest invalidated: price crossed below currentSwing low at candle ${i}`);
          return null;
        }
        if (candle.low >= currentSwingPrice && candle.low <= vShapePrice && candle.low < minLow) {
          minLow = candle.low;
          extremumCandle = candle;
        }
      }
    } else {
      let maxHigh = -Infinity;
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.high > currentSwingPrice) {
          this._debugLog(`[identifyRetest] Retest invalidated: price crossed above currentSwing high at candle ${i}`);
          return null;
        }
        if (candle.high <= currentSwingPrice && candle.high >= vShapePrice && candle.high > maxHigh) {
          maxHigh = candle.high;
          extremumCandle = candle;
        }
      }
    }

    if (extremumCandle) {
      const vShapeCandle = this.findRetestVShapeCandle(setup.breakout, extremumCandle, candles, direction);
      let retestBreakout = null;
      if (vShapeCandle) {
        const vShapeLevel = direction === DIRECTION.BULLISH ? vShapeCandle.high : vShapeCandle.low;
        retestBreakout = this.findRetestBreakout(vShapeLevel, candles, extremumCandle.index, direction);
      }
      return {
        ...extremumCandle,
        retestLevel: direction === DIRECTION.BULLISH ? extremumCandle.low : extremumCandle.high,
        retestType: direction === DIRECTION.BULLISH ? 'support' : 'resistance',
        vShapeCandle,
        breakout: retestBreakout,
        context: setup.context || 'unknown'
      };
    }
    return null;
  }

  findRetestBreakout(level, candles, startIndex, direction) {
    for (let i = startIndex + 1; i < candles.length; i++) {
      const candle = candles[i];
      if (!candle) continue;
      if (direction === DIRECTION.BULLISH) {
        if (candle.close > level) return candle;
      } else {
        if (candle.close < level) return candle;
      }
    }
    return null;
  }

  // MODIFIED: Include retest candle itself when searching for extreme
  findRetestVShapeCandle(breakoutCandle, retestCandle, candles, direction) {
    const startIndex = breakoutCandle.index + 1;
    const endIndex = retestCandle.index; // inclusive
    if (startIndex > endIndex) return null;

    let extremumCandle = null;
    if (direction === DIRECTION.BULLISH) {
      let maxHigh = -Infinity;
      for (let i = startIndex; i <= endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.high > maxHigh) {
          maxHigh = candle.high;
          extremumCandle = candle;
        }
      }
      if (breakoutCandle.high > maxHigh) extremumCandle = breakoutCandle;
    } else {
      let minLow = Infinity;
      for (let i = startIndex; i <= endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.low < minLow) {
          minLow = candle.low;
          extremumCandle = candle;
        }
      }
      if (breakoutCandle.low < minLow) extremumCandle = breakoutCandle;
    }
    return extremumCandle;
  }

  findExtremeCandleAfterRetestBreakout(retest, candles, direction) {
    if (!retest.breakout) return null;

    const startIndex = retest.breakout.index + 1;
    const retestLevel = direction === DIRECTION.BULLISH ? retest.low : retest.high;
    if (startIndex >= candles.length) return null;

    if (direction === DIRECTION.BULLISH) {
      let extremeCandle = null;
      let minLow = Infinity;
      let firstBodyCrossCandle = null;
      for (let i = startIndex; i < candles.length; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (!firstBodyCrossCandle && (candle.close < retestLevel || candle.open < retestLevel)) {
          firstBodyCrossCandle = candle;
        }
        if (candle.low < minLow) {
          minLow = candle.low;
          extremeCandle = candle;
        }
      }
      if (!extremeCandle) return null;
      if (firstBodyCrossCandle) {
        for (let i = firstBodyCrossCandle.index + 1; i < candles.length; i++) {
          const candle = candles[i];
          if (!candle) continue;
          if (candle.close < firstBodyCrossCandle.low || candle.open < firstBodyCrossCandle.low) {
            this._debugLog(`[findExtremeCandleAfterRetestBreakout] Extreme candle invalidated: candle ${i} close/open went below first body cross candle low (${firstBodyCrossCandle.low})`);
            return null;
          }
        }
      }
      return extremeCandle;
    } else {
      let extremeCandle = null;
      let maxHigh = -Infinity;
      let firstBodyCrossCandle = null;
      for (let i = startIndex; i < candles.length; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (!firstBodyCrossCandle && (candle.close > retestLevel || candle.open > retestLevel)) {
          firstBodyCrossCandle = candle;
        }
        if (candle.high > maxHigh) {
          maxHigh = candle.high;
          extremeCandle = candle;
        }
      }
      if (!extremeCandle) return null;
      if (firstBodyCrossCandle) {
        for (let i = firstBodyCrossCandle.index + 1; i < candles.length; i++) {
          const candle = candles[i];
          if (candle.close > firstBodyCrossCandle.high || candle.open > firstBodyCrossCandle.high) {
            this._debugLog(`[findExtremeCandleAfterRetestBreakout] Extreme candle invalidated: candle ${i} close/open went above first body cross candle high (${firstBodyCrossCandle.high})`);
            return null;
          }
        }
      }
      return extremeCandle;
    }
  }

  _checkSSetup(swing1, swing2, candles, direction) {
    const sweepData = this.isWickSweep(swing2, swing1, candles, direction);
    return sweepData.isSweep ? STATUS.S_SETUP : null;
  }

  _checkDoubleEq(candle1, nextSwing, direction) {
    if (!candle1 || !nextSwing) return null;

    const isRedCandle = candle1.close < candle1.open;
    const isGreenCandle = candle1.close >= candle1.open;

    if (direction === DIRECTION.BULLISH) {
      const nextSwingLow = nextSwing.low;
      if (isRedCandle) {
        if (nextSwingLow >= candle1.low && nextSwingLow <= candle1.close) return STATUS.DOUBLE_EQ;
      } else if (isGreenCandle) {
        if (nextSwingLow >= candle1.low && nextSwingLow <= candle1.open) return STATUS.DOUBLE_EQ;
      }
    } else {
      const nextSwingHigh = nextSwing.high;
      if (isRedCandle) {
        if (nextSwingHigh <= candle1.high && nextSwingHigh >= candle1.open) return STATUS.DOUBLE_EQ;
      } else if (isGreenCandle) {
        if (nextSwingHigh <= candle1.high && nextSwingHigh >= candle1.close) return STATUS.DOUBLE_EQ;
      }
    }
    return null;
  }

  // ---------- Confirmed setup (refactored with helper) ----------
  identifyConfirmedSetup(retest, candles, direction, swings, swingIndex, obMap) {
    if (!retest.vShapeCandle || !retest.breakout) {
      this._debugLog(`[identifyConfirmedSetup] Retest must have vShapeCandle and breakout`);
      return null;
    }

    const extremeCandle = this.findExtremeCandleAfterRetestBreakout(retest, candles, direction);
    if (!extremeCandle) {
      this._debugLog(`[identifyConfirmedSetup] No valid extreme candle found after retest breakout`);
      return null;
    }

    const extremeCandleSwing = swingIndex.indexMap.get(extremeCandle.index);
    if (!extremeCandleSwing) {
      this._debugLog(`[identifyConfirmedSetup] Extreme candle at index ${extremeCandle.index} is not a swing`);
      return null;
    }

    const swingPosition = swings.findIndex(s => s.index === extremeCandleSwing.index);
    const currentSwingLevel = direction === DIRECTION.BULLISH ? extremeCandle.low : extremeCandle.high;
    const previousSwing = this._findPreviousExtremeSwing(swings, swingPosition, direction, currentSwingLevel, retest.breakout.index);

    // Build data for previous side
    const prevData = this._buildSwingSideData(previousSwing, extremeCandleSwing, candles, direction, swingIndex, obMap, retest);

    // Find next swing (if any)
    const nextSwing = this._findNextExtremeSwing(swings, swingPosition, direction);
    const nextData = nextSwing ? this._buildSwingSideData(extremeCandleSwing, nextSwing, candles, direction, swingIndex, obMap, retest) : null;

    // Construct result
    const result = {
      type: 'confirmedSetup',
      direction,
      candle1: extremeCandle,
      candle2Previous: prevData.candle,
      candle2PreviousStatus: prevData.status,
      candle2PreviousVshape: prevData.vshape,
      candle2PreviousBreakout: prevData.breakout,
      candle2PreviousRetest: prevData.retest,
      candle2PreviousRetestVshape: prevData.retestVshape,
      candle2PreviousMitigationIndex: prevData.mitigationIndex,
      candle2PreviousMitigationFormattedTime: prevData.mitigationFormattedTime,
      candle2PreviousMitigationStatus: prevData.mitigationStatus,
      candle2PreviousOBIndex: prevData.obIndex,
      candle2PreviousOBFormattedTime: prevData.obFormattedTime,
      candle2PreviousOBStatus: prevData.obStatus,
      candle2Next: nextData?.candle || null,
      candle2NextStatus: nextData?.status || null,
      candle2NextVshape: nextData?.vshape || null,
      candle2NextBreakout: nextData?.breakout || null,
      candle2NextRetest: nextData?.retest || null,
      candle2NextRetestVshape: nextData?.retestVshape || null,
      candle2NextMitigationIndex: nextData?.mitigationIndex || null,
      candle2NextMitigationFormattedTime: nextData?.mitigationFormattedTime || null,
      candle2NextMitigationStatus: nextData?.mitigationStatus || null,
      candle2NextOBIndex: nextData?.obIndex || null,
      candle2NextOBFormattedTime: nextData?.obFormattedTime || null,
      candle2NextOBStatus: nextData?.obStatus || false,
      level: direction === DIRECTION.BULLISH ? extremeCandle.low : extremeCandle.high,
      levelType: direction === DIRECTION.BULLISH ? 'support' : 'resistance'
    };

    if (nextData && nextData.candle) {
      result.level = direction === DIRECTION.BULLISH
        ? Math.min(extremeCandle.low, nextData.candle.low)
        : Math.max(extremeCandle.high, nextData.candle.high);
    }
    return result;
  }

  _buildSwingSideData(swingA, swingB, candles, direction, swingIndex, obMap, retestRef = null) {
    if (!swingA || !swingB) {
      return {
        candle: null, status: null, vshape: null, breakout: null, retest: null, retestVshape: null,
        mitigationIndex: null, mitigationFormattedTime: null, mitigationStatus: null,
        obIndex: null, obFormattedTime: null, obStatus: false
      };
    }

    const status = this._checkSSetup(swingA, swingB, candles, direction);
    const candleA = candles[swingA.index];
    let vshape = null, breakout = null, retest = null, retestVshape = null;

    if (status === STATUS.S_SETUP) {
      vshape = this.findVShapeCandle(swingA, swingB, candles, direction);
      if (vshape) {
        const vLevel = direction === DIRECTION.BULLISH ? vshape.high : vshape.low;
        breakout = this.identifyBreakoutOfLevel(vLevel, candles, swingB.index, direction, swingB);
        if (breakout) {
          retest = this._findConfirmedSetupRetest(vshape, swingB, breakout, candles, direction);
        }
        if (retest) {
          retestVshape = this.findRetestVShapeCandle(breakout, retest, candles, direction);
        }
      }
    }

    const mitigationCandle = this._findCandle2PreviousMitigation(swingA.index, swingB.index, candles, direction);
    let mitigationStatus = null;
    if (mitigationCandle && breakout) {
      mitigationStatus = this._checkMitigationRetest(mitigationCandle, breakout.index + 1, candles, direction);
    }

    let ob = null, obStatus = false;
    if (vshape) {
      ob = this._findOBForSetup(swingB.index, obMap, direction, vshape, this.config.obScanRange);
      if (ob && retest) {
        const retestPrice = direction === DIRECTION.BULLISH ? retest.low : retest.high;
        obStatus = retestPrice >= ob.low && retestPrice <= ob.high;
      }
    }

    return {
      candle: candleA,
      status,
      vshape,
      breakout,
      retest,
      retestVshape,
      mitigationIndex: mitigationCandle?.index || null,
      mitigationFormattedTime: mitigationCandle ? this._formatTime(mitigationCandle.time) : null,
      mitigationStatus,
      obIndex: ob?.index || null,
      obFormattedTime: ob?.formattedTime || null,
      obStatus
    };
  }

  _findNextExtremeSwing(swings, startPos, direction) {
    const type = direction === DIRECTION.BULLISH ? 'low' : 'high';
    let extreme = null;
    let extremeValue = direction === DIRECTION.BULLISH ? Infinity : -Infinity;
    for (let i = startPos + 1; i < swings.length; i++) {
      if (swings[i].type === type) {
        const val = direction === DIRECTION.BULLISH ? swings[i].low : swings[i].high;
        if ((direction === DIRECTION.BULLISH && val < extremeValue) ||
            (direction === DIRECTION.BEARISH && val > extremeValue)) {
          extremeValue = val;
          extreme = swings[i];
        }
      }
    }
    return extreme;
  }

  // ---------- Public API ----------
  get(symbol, granularity) {
    return this.store[symbol]?.[granularity] || [];
  }

  getStats(symbol, granularity) {
    const patterns = this.get(symbol, granularity);
    return {
      total: patterns.length,
      bullish: patterns.filter(p => p.direction === DIRECTION.BULLISH).length,
      bearish: patterns.filter(p => p.direction === DIRECTION.BEARISH).length,
    };
  }

  clearOld(symbol, granularity, maxAge) {
    const patterns = this.get(symbol, granularity);
    const cutoff = Date.now() - maxAge;
    this.store[symbol][granularity] = patterns.filter(p => p.timestamp > cutoff);
  }
}

module.exports = new PatternEngine();