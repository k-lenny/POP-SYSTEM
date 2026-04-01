const EventEmitter = require('events');
const swingEngine = require('../../signals/dataProcessor/swings');
const { processOBLV } = require('../../signals/dataProcessor/OBLV');
const Logger = require('../../utils/logger');
const { getConfig } = require('../../config');

class PatternEngine extends EventEmitter {
  constructor(options = {}) {
    super();
    this.store = {};
    this.logger = options.logger || new Logger('PatternEngine');
    this.emitEvents = options.emitEvents ?? getConfig().ENABLE_EVENTS;

    // Configurable parameters
    this.config = {
      vShapeWickRatio: 2.0,
      equalLevelTolerance: 0.002,
      minCandlesBetweenSwings: 3,
      retestScanRange: 30,
      obScanRange: 10,
      ...options.config
    };

    // Caches
    this._swingCache = new Map();      // key = `${symbol}|${granularity}`
    this._candleCache = new Map();     // key = `${symbol}|${granularity}` -> { enriched, obMap, candleMap, length, lastTimestamp }
    this._oblvCache = new Map();       // key = `${symbol}|${granularity}`

    // Concurrency limit for batch processing
    this.maxConcurrency = options.maxConcurrency || 4;
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
      // Same candles – reuse everything
      return cached;
    }

    // Compute fresh
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

  // ---------- Original methods ----------
  _initStore(symbol, granularity) {
    if (!this.store[symbol]) this.store[symbol] = {};
    if (!this.store[symbol][granularity]) this.store[symbol][granularity] = [];
  }

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
    if (currentSwing.type === 'low' && previousSwing.type === 'low') return 'bullish';
    if (currentSwing.type === 'high' && previousSwing.type === 'high') return 'bearish';
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
    const type = direction === 'bullish' ? 'low' : 'high';
    let extremeSwing = null;
    let extremeValue = direction === 'bullish' ? Infinity : -Infinity;

    for (let i = currentIndex - 1; i >= 0; i--) {
      if (swings[i].type === type && swings[i].index >= minIndex) {
        if (direction === 'bullish') {
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

    const vshapePrice = direction === 'bullish' ? vshape.high : vshape.low;
    const currentSwingPrice = direction === 'bullish' ? currentSwing.low : currentSwing.high;
    let retestCandle = null;

    if (direction === 'bullish') {
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
      if (direction === 'bullish') {
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
    if (direction === 'bullish') {
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
    if (direction === 'bullish') {
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
      if (direction === 'bullish') {
        if (c.low < mtLow) { firstCrossCandle = c; break; }
      } else {
        if (c.high > mtHigh) { firstCrossCandle = c; break; }
      }
    }
    if (!firstCrossCandle) return null;

    const refValue = direction === 'bullish' ? firstCrossCandle.low : firstCrossCandle.high;
    for (let i = firstCrossCandle.index + 1; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === 'bullish') {
        if (c.close < refValue || c.open < refValue) return 'EXPIRED';
      } else {
        if (c.close > refValue || c.open > refValue) return 'EXPIRED';
      }
    }
    return 'ACTIVE';
  }

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

      const candlesBetween = currentSwing.index - previousSwing.index;
      if (candlesBetween < this.config.minCandlesBetweenSwings) continue;

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
      vShapeCandlePrice: pattern.direction === 'bullish' 
        ? pattern.vShapeCandle?.high 
        : pattern.vShapeCandle?.low,
      vShapeCandleTime: pattern.vShapeCandle?.time || null,
      vShapeCandleFormattedTime: this._formatTime(pattern.vShapeCandle?.time),
      vShapeWickSize: pattern.direction === 'bullish'
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
      retestLevel: pattern.retest?.retestLevel || null,
      retestType: pattern.retest?.retestType || null,

      retestVshapeIndex: pattern.retest?.vShapeCandle?.index || null,
      retestVshapePrice: pattern.direction === 'bullish'
        ? pattern.retest?.vShapeCandle?.high
        : pattern.retest?.vShapeCandle?.low,
      retestVshapeTime: pattern.retest?.vShapeCandle?.time || null,
      retestVshapeFormattedTime: this._formatTime(pattern.retest?.vShapeCandle?.time),
      retestVshapeLevel: pattern.direction === 'bullish'
        ? pattern.retest?.vShapeCandle?.high
        : pattern.retest?.vShapeCandle?.low,

      retestBreakoutIndex: pattern.retest?.breakout?.index || null,
      retestBreakoutPrice: pattern.retest?.breakout?.close || null,
      retestBreakoutTime: pattern.retest?.breakout?.time || null,
      retestBreakoutFormattedTime: this._formatTime(pattern.retest?.breakout?.time),

      confirmedSetupCandle1Index: pattern.confirmedSetup?.candle1?.index || null,
      confirmedSetupCandle1Price: pattern.confirmedSetup?.candle1?.close || null,
      confirmedSetupCandle1Time: pattern.confirmedSetup?.candle1?.time|| null,
      confirmedSetupCandle1FormattedTime: this._formatTime(pattern.confirmedSetup?.candle1?.time),

      confirmedSetupCandle2PreviousIndex: pattern.confirmedSetup?.candle2Previous?.index || null,
      confirmedSetupCandle2PreviousPrice: pattern.confirmedSetup?.candle2Previous?.close || null,
      confirmedSetupCandle2PreviousTime: pattern.confirmedSetup?.candle2Previous?.time|| null,
      confirmedSetupCandle2PreviousFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2Previous?.time),
      confirmedSetupCandle2PreviousStatus: pattern.confirmedSetup?.candle2PreviousStatus || null,

      confirmedSetupCandle2PreviousVshapeIndex: pattern.confirmedSetup?.candle2PreviousVshape?.index || null,
      confirmedSetupCandle2PreviousVshapePrice: pattern.direction === 'bullish'
        ? pattern.confirmedSetup?.candle2PreviousVshape?.high
        : pattern.confirmedSetup?.candle2PreviousVshape?.low,
      confirmedSetupCandle2PreviousVshapeTime: pattern.confirmedSetup?.candle2PreviousVshape?.time || null,
      confirmedSetupCandle2PreviousVshapeFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2PreviousVshape?.time),

      confirmedSetupCandle2PreviousBreakoutIndex: pattern.confirmedSetup?.candle2PreviousBreakout?.index || null,
      confirmedSetupCandle2PreviousBreakoutPrice: pattern.confirmedSetup?.candle2PreviousBreakout?.close || null,
      confirmedSetupCandle2PreviousBreakoutTime: pattern.confirmedSetup?.candle2PreviousBreakout?.time || null,
      confirmedSetupCandle2PreviousBreakoutFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2PreviousBreakout?.time),

      confirmedSetupCandle2PreviousRetestIndex: pattern.confirmedSetup?.candle2PreviousRetest?.index || null,
      confirmedSetupCandle2PreviousRetestPrice: pattern.direction === 'bullish'
        ? pattern.confirmedSetup?.candle2PreviousRetest?.low
        : pattern.confirmedSetup?.candle2PreviousRetest?.high,
      confirmedSetupCandle2PreviousRetestTime: pattern.confirmedSetup?.candle2PreviousRetest?.time || null,
      confirmedSetupCandle2PreviousRetestFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2PreviousRetest?.time),

      confirmedSetupCandle2PreviousRetestVshapeIndex: pattern.confirmedSetup?.candle2PreviousRetestVshape?.index || null,
      confirmedSetupCandle2PreviousRetestVshapePrice: pattern.direction === 'bullish'
        ? pattern.confirmedSetup?.candle2PreviousRetestVshape?.high
        : pattern.confirmedSetup?.candle2PreviousRetestVshape?.low,
      confirmedSetupCandle2PreviousRetestVshapeTime: pattern.confirmedSetup?.candle2PreviousRetestVshape?.time || null,
      confirmedSetupCandle2PreviousRetestVshapeFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2PreviousRetestVshape?.time),

      confirmedSetupCandle2PreviousMitigationIndex: pattern.confirmedSetup?.candle2PreviousMitigationIndex || null,
      confirmedSetupCandle2PreviousMitigationFormattedTime: pattern.confirmedSetup?.candle2PreviousMitigationFormattedTime || null,
      confirmedSetupCandle2PreviousMitigationStatus: pattern.confirmedSetup?.candle2PreviousMitigationStatus ?? null,

      confirmedSetupCandle2PreviousOBIndex: pattern.confirmedSetup?.candle2PreviousOBIndex || null,
      confirmedSetupCandle2PreviousOBFormattedTime: pattern.confirmedSetup?.candle2PreviousOBFormattedTime || null,
      confirmedSetupCandle2PreviousOBStatus: pattern.confirmedSetup?.candle2PreviousOBStatus ?? false,

      confirmedSetupCandle2NextIndex: pattern.confirmedSetup?.candle2Next?.index || null,
      confirmedSetupCandle2NextPrice: pattern.confirmedSetup?.candle2Next?.close || null,
      confirmedSetupCandle2NextTime: pattern.confirmedSetup?.candle2Next?.time|| null,
      confirmedSetupCandle2NextFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2Next?.time),
      confirmedSetupCandle2NextStatus: pattern.confirmedSetup?.candle2NextStatus || null,

      confirmedSetupCandle2NextVshapeIndex: pattern.confirmedSetup?.candle2NextVshape?.index || null,
      confirmedSetupCandle2NextVshapePrice: pattern.direction === 'bullish'
        ? pattern.confirmedSetup?.candle2NextVshape?.high
        : pattern.confirmedSetup?.candle2NextVshape?.low,
      confirmedSetupCandle2NextVshapeTime: pattern.confirmedSetup?.candle2NextVshape?.time || null,
      confirmedSetupCandle2NextVshapeFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2NextVshape?.time),

      confirmedSetupCandle2NextBreakoutIndex: pattern.confirmedSetup?.candle2NextBreakout?.index || null,
      confirmedSetupCandle2NextBreakoutPrice: pattern.confirmedSetup?.candle2NextBreakout?.close || null,
      confirmedSetupCandle2NextBreakoutTime: pattern.confirmedSetup?.candle2NextBreakout?.time || null,
      confirmedSetupCandle2NextBreakoutFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2NextBreakout?.time),

      confirmedSetupCandle2NextRetestIndex: pattern.confirmedSetup?.candle2NextRetest?.index || null,
      confirmedSetupCandle2NextRetestPrice: pattern.direction === 'bullish'
        ? pattern.confirmedSetup?.candle2NextRetest?.low
        : pattern.confirmedSetup?.candle2NextRetest?.high,
      confirmedSetupCandle2NextRetestTime: pattern.confirmedSetup?.candle2NextRetest?.time || null,
      confirmedSetupCandle2NextRetestFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2NextRetest?.time),

      confirmedSetupCandle2NextRetestVshapeIndex: pattern.confirmedSetup?.candle2NextRetestVshape?.index || null,
      confirmedSetupCandle2NextRetestVshapePrice: pattern.direction === 'bullish'
        ? pattern.confirmedSetup?.candle2NextRetestVshape?.high
        : pattern.confirmedSetup?.candle2NextRetestVshape?.low,
      confirmedSetupCandle2NextRetestVshapeTime: pattern.confirmedSetup?.candle2NextRetestVshape?.time || null,
      confirmedSetupCandle2NextRetestVshapeFormattedTime: this._formatTime(pattern.confirmedSetup?.candle2NextRetestVshape?.time),

      confirmedSetupCandle2NextMitigationIndex: pattern.confirmedSetup?.candle2NextMitigationIndex || null,
      confirmedSetupCandle2NextMitigationFormattedTime: pattern.confirmedSetup?.candle2NextMitigationFormattedTime || null,
      confirmedSetupCandle2NextMitigationStatus: pattern.confirmedSetup?.candle2NextMitigationStatus ?? null,

      confirmedSetupCandle2NextOBIndex: pattern.confirmedSetup?.candle2NextOBIndex || null,
      confirmedSetupCandle2NextOBFormattedTime: pattern.confirmedSetup?.candle2NextOBFormattedTime || null,
      confirmedSetupCandle2NextOBStatus: pattern.confirmedSetup?.candle2NextOBStatus ?? false,

      stage: this._getPatternStage(pattern),
      isComplete: !!(pattern.confirmedSetup),

      formattedTime: this._formatTime(pattern.currentSwing?.time),
      time: pattern.currentSwing?.time || null,
      timestamp: pattern.timestamp || Date.now(),
    };
    return result;
  }

  _getPatternStage(pattern) {
    if (pattern.confirmedSetup) return 3;
    if (pattern.retest) return 2;
    return 1;
  }

  _buildPatternStages(setup, candles, direction, swings, swingIndex, obMap, patterns) {
    const retest = this.identifyRetest(setup, candles, direction);
    if (!retest) return;
    setup.retest = retest;

    const confirmedSetup = this.identifyConfirmedSetup(retest, candles, direction, swings, swingIndex, obMap);
    if (!confirmedSetup) return;
    setup.confirmedSetup = confirmedSetup;

    // Check for additional S setup pattern using swingIndex map
    if (confirmedSetup.candle2PreviousStatus === 'S SETUP' && confirmedSetup.candle2PreviousVshape && confirmedSetup.candle2PreviousBreakout) {
      const prevSwing = swingIndex.indexMap.get(confirmedSetup.candle2Previous.index);
      const currSwing = swingIndex.indexMap.get(confirmedSetup.candle1.index);
      if (prevSwing && currSwing && prevSwing.index < currSwing.index) {
        const alreadyExists = patterns.some(p => 
          p.previousSwingIndex === prevSwing.index && p.currentSwingIndex === currSwing.index
        );
        if (!alreadyExists) {
          const newSetup = this.identifySetup(currSwing, prevSwing, candles, direction, swings, swingIndex);
          if (newSetup) {
            this._buildPatternStages(newSetup, candles, direction, swings, swingIndex, obMap, patterns);
            const enrichedNewPattern = this._enrichPatternMetadata(newSetup, candles);
            patterns.push(enrichedNewPattern);
            this.logger.info(`[PatternEngine] Added additional S setup pattern from swing ${prevSwing.index} to ${currSwing.index}`);
          }
        }
      }
    }
  }

  identifySetup(currentSwing, previousSwing, candles, direction, swings, swingIndex) {
    const sweepData = this.isWickSweep(currentSwing, previousSwing, candles, direction);
    if (!sweepData.isSweep) {
      this.logger.debug(`[PatternEngine] No sweep detected between swing ${previousSwing.index} and ${currentSwing.index} (${direction})`);
      return null;
    }

    const vShapeCandle = this.findVShapeCandle(previousSwing, currentSwing, candles, direction);
    if (!vShapeCandle) {
      this.logger.debug(`[PatternEngine] No V-shape candle found between swing ${previousSwing.index} and ${currentSwing.index} (${direction})`);
      return null;
    }

    const breakoutLevel = direction === 'bullish' ? vShapeCandle.high : vShapeCandle.low;
    const breakout = this.identifyBreakoutOfLevel(breakoutLevel, candles, currentSwing.index, direction, currentSwing);
    if (!breakout) {
      this.logger.debug(`[PatternEngine] No breakout found for swing ${currentSwing.index} (${direction}, level: ${breakoutLevel})`);
      return null;
    }

    this.logger.debug(`[PatternEngine] Valid setup found: sweep at ${sweepData.firstSweepCandleIndex}, v-shape at ${vShapeCandle.index}, breakout at ${breakout.index}`);

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

    if (direction === 'bullish') {
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
          this.logger.debug(`[PatternEngine] Sweep invalidated: candle ${i} close/open went below first sweep candle low (${firstSweepCandle.low})`);
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
          this.logger.debug(`[PatternEngine] Sweep invalidated: candle ${i} close/open went above first sweep candle high (${firstSweepCandle.high})`);
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
    if (direction === 'bullish') {
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
    for (let i = startIndex; i < candles.length; i++) {
      const candle = candles[i];
      if (candle.index === currentSwing.index) continue;

      if (direction === 'bullish') {
        if (candle.low < currentSwing.low) return null;
        if (candle.close > level || candle.open > level) return candle;
      } else {
        if (candle.high > currentSwing.high) return null;
        if (candle.close < level || candle.open < level) return candle;
      }
    }
    return null;
  }

  identifyRetest(setup, candles, direction) {
    if (!setup.breakout) return null;

    const startIndex = setup.breakout.index + 1;
    if (startIndex >= candles.length) return null;

    const maxScanRange = this.config.retestScanRange;
    const endIndex = Math.min(startIndex + maxScanRange, candles.length);

    const currentSwing = setup.currentSwing;
    let currentSwingLevel = currentSwing ? (direction === 'bullish' ? currentSwing.low : currentSwing.high) : null;
    let extremumCandle = null;

    if (direction === 'bullish') {
      let minLow = Infinity;
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (currentSwingLevel !== null && candle.low < currentSwingLevel) {
          this.logger.debug(`[PatternEngine] Retest invalidated: price crossed below currentSwing low at candle ${i}`);
          return null;
        }
        if (candle.low < minLow) {
          minLow = candle.low;
          extremumCandle = candle;
        }
      }
    } else {
      let maxHigh = -Infinity;
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (currentSwingLevel !== null && candle.high > currentSwingLevel) {
          this.logger.debug(`[PatternEngine] Retest invalidated: price crossed above currentSwing high at candle ${i}`);
          return null;
        }
        if (candle.high > maxHigh) {
          maxHigh = candle.high;
          extremumCandle = candle;
        }
      }
    }

    if (extremumCandle) {
      const vShapeCandle = this.findRetestVShapeCandle(setup.breakout, extremumCandle, candles, direction);
      let retestBreakout = null;
      if (vShapeCandle) {
        const vShapeLevel = direction === 'bullish' ? vShapeCandle.high : vShapeCandle.low;
        retestBreakout = this.findRetestBreakout(vShapeLevel, candles, extremumCandle.index, direction);
      }
      return {
        ...extremumCandle,
        retestLevel: direction === 'bullish' ? extremumCandle.low : extremumCandle.high,
        retestType: direction === 'bullish' ? 'support' : 'resistance',
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
      if (direction === 'bullish') {
        if (candle.close > level) return candle;
      } else {
        if (candle.close < level) return candle;
      }
    }
    return null;
  }

  findRetestVShapeCandle(breakoutCandle, retestCandle, candles, direction) {
    const startIndex = breakoutCandle.index + 1;
    const endIndex = retestCandle.index;
    if (endIndex <= startIndex) return null;

    let extremumCandle = null;
    if (direction === 'bullish') {
      let maxHigh = -Infinity;
      for (let i = startIndex; i < endIndex; i++) {
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
      for (let i = startIndex; i < endIndex; i++) {
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
    const retestLevel = direction === 'bullish' ? retest.low : retest.high;
    if (startIndex >= candles.length) return null;

    if (direction === 'bullish') {
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
            this.logger.debug(`[PatternEngine] Extreme candle invalidated: candle ${i} close/open went below first body cross candle low (${firstBodyCrossCandle.low})`);
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
            this.logger.debug(`[PatternEngine] Extreme candle invalidated: candle ${i} close/open went above first body cross candle high (${firstBodyCrossCandle.high})`);
            return null;
          }
        }
      }
      return extremeCandle;
    }
  }

  _checkSSetup(swing1, swing2, candles, direction) {
    const sweepData = this.isWickSweep(swing2, swing1, candles, direction);
    return sweepData.isSweep ? 'S SETUP' : null;
  }

  _checkDoubleEq(candle1, nextSwing, direction) {
    if (!candle1 || !nextSwing) return null;

    const isRedCandle = candle1.close < candle1.open;
    const isGreenCandle = candle1.close >= candle1.open;

    if (direction === 'bullish') {
      const nextSwingLow = nextSwing.low;
      if (isRedCandle) {
        if (nextSwingLow >= candle1.low && nextSwingLow <= candle1.close) return 'DOUBLE EQ';
      } else if (isGreenCandle) {
        if (nextSwingLow >= candle1.low && nextSwingLow <= candle1.open) return 'DOUBLE EQ';
      }
    } else {
      const nextSwingHigh = nextSwing.high;
      if (isRedCandle) {
        if (nextSwingHigh <= candle1.high && nextSwingHigh >= candle1.open) return 'DOUBLE EQ';
      } else if (isGreenCandle) {
        if (nextSwingHigh <= candle1.high && nextSwingHigh >= candle1.close) return 'DOUBLE EQ';
      }
    }
    return null;
  }

  identifyConfirmedSetup(retest, candles, direction, swings, swingIndex, obMap) {
    if (!retest.vShapeCandle || !retest.breakout) {
      this.logger.debug(`[PatternEngine] Retest must have vShapeCandle and breakout before confirmed setup can be identified`);
      return null;
    }

    const extremeCandle = this.findExtremeCandleAfterRetestBreakout(retest, candles, direction);
    if (!extremeCandle) {
      this.logger.debug(`[PatternEngine] No valid extreme candle found after retest breakout`);
      return null;
    }

    const extremeCandleSwing = swingIndex.indexMap.get(extremeCandle.index);
    if (!extremeCandleSwing) {
      this.logger.debug(`[PatternEngine] Extreme candle at index ${extremeCandle.index} is not a swing`);
      return null;
    }

    const swingPosition = swings.findIndex(s => s.index === extremeCandleSwing.index);
    const currentSwingLevel = direction === 'bullish' ? extremeCandle.low : extremeCandle.high;
    const previousSwing = this._findPreviousExtremeSwing(swings, swingPosition, direction, currentSwingLevel, retest.breakout.index);

    const candle2PreviousStatus = previousSwing ? this._checkSSetup(previousSwing, extremeCandleSwing, candles, direction) : null;

    let candle2PreviousVshape = null, candle2PreviousBreakout = null, candle2PreviousRetest = null;
    if (candle2PreviousStatus === 'S SETUP' && previousSwing) {
      candle2PreviousVshape = this.findVShapeCandle(previousSwing, extremeCandleSwing, candles, direction);
      if (candle2PreviousVshape) {
        const vShapeLevel = direction === 'bullish' ? candle2PreviousVshape.high : candle2PreviousVshape.low;
        candle2PreviousBreakout = this.identifyBreakoutOfLevel(vShapeLevel, candles, extremeCandleSwing.index, direction, extremeCandleSwing);
        if (candle2PreviousBreakout) {
          candle2PreviousRetest = this._findConfirmedSetupRetest(candle2PreviousVshape, extremeCandleSwing, candle2PreviousBreakout, candles, direction);
        }
      }
    }

    let candle2PreviousRetestVshape = null;
    if (candle2PreviousBreakout && candle2PreviousRetest) {
      candle2PreviousRetestVshape = this.findRetestVShapeCandle(candle2PreviousBreakout, candle2PreviousRetest, candles, direction);
    }

    let candle2PreviousMitigation = null;
    if (previousSwing) {
      candle2PreviousMitigation = this._findCandle2PreviousMitigation(previousSwing.index, extremeCandle.index, candles, direction);
    }

    let candle2PreviousMitigationStatus = null;
    if (candle2PreviousMitigation && candle2PreviousBreakout) {
      const startCheckIndex = candle2PreviousBreakout.index + 1;
      candle2PreviousMitigationStatus = this._checkMitigationRetest(candle2PreviousMitigation, startCheckIndex, candles, direction);
    }

    let candle2PreviousOB = null, candle2PreviousOBStatus = false;
    if (candle2PreviousVshape) {
      candle2PreviousOB = this._findOBForSetup(extremeCandle.index, obMap, direction, candle2PreviousVshape, this.config.obScanRange);
      if (candle2PreviousOB && candle2PreviousRetest) {
        const retestPrice = direction === 'bullish' ? candle2PreviousRetest.low : candle2PreviousRetest.high;
        candle2PreviousOBStatus = retestPrice >= candle2PreviousOB.low && retestPrice <= candle2PreviousOB.high;
      }
    }

    const swingType = direction === 'bullish' ? 'low' : 'high';
    let nextSwing = null;
    let extremeValue = direction === 'bullish' ? Infinity : -Infinity;
    for (let i = swingPosition + 1; i < swings.length; i++) {
      if (swings[i].type === swingType) {
        if (direction === 'bullish') {
          if (swings[i].low < extremeValue) {
            extremeValue = swings[i].low;
            nextSwing = swings[i];
          }
        } else {
          if (swings[i].high > extremeValue) {
            extremeValue = swings[i].high;
            nextSwing = swings[i];
          }
        }
      }
    }

    const candle2NextStatus = nextSwing ? this._checkDoubleEq(extremeCandle, nextSwing, direction) : null;

    let candle2NextVshape = null, candle2NextBreakout = null, candle2NextRetest = null;
    if (nextSwing) {
      const extremeCandleAsSwing = extremeCandleSwing;
      const nextSwingAsSwing = swingIndex.indexMap.get(nextSwing.index);
      if (nextSwingAsSwing) {
        candle2NextVshape = this.findVShapeCandle(extremeCandleAsSwing, nextSwingAsSwing, candles, direction);
        if (candle2NextVshape) {
          const vShapeLevel = direction === 'bullish' ? candle2NextVshape.high : candle2NextVshape.low;
          candle2NextBreakout = this.identifyBreakoutOfLevel(vShapeLevel, candles, nextSwingAsSwing.index, direction, nextSwingAsSwing);
          if (candle2NextBreakout) {
            candle2NextRetest = this._findConfirmedSetupRetest(candle2NextVshape, nextSwingAsSwing, candle2NextBreakout, candles, direction);
          }
        }
      }
    }

    let candle2NextRetestVshape = null;
    if (candle2NextBreakout && candle2NextRetest) {
      candle2NextRetestVshape = this.findRetestVShapeCandle(candle2NextBreakout, candle2NextRetest, candles, direction);
    }

    let candle2NextMitigation = null;
    if (nextSwing) {
      candle2NextMitigation = this._findCandle2NextMitigation(extremeCandle.index, nextSwing.index, candles, direction);
    }

    let candle2NextMitigationStatus = null;
    if (candle2NextMitigation && candle2NextBreakout) {
      const startCheckIndex = candle2NextBreakout.index + 1;
      candle2NextMitigationStatus = this._checkMitigationRetest(candle2NextMitigation, startCheckIndex, candles, direction);
    }

    let candle2NextOB = null, candle2NextOBStatus = false;
    if (nextSwing && candle2NextVshape) {
      candle2NextOB = this._findOBForSetup(nextSwing.index, obMap, direction, candle2NextVshape, this.config.obScanRange);
      if (candle2NextOB && candle2NextRetest) {
        const retestPrice = direction === 'bullish' ? candle2NextRetest.low : candle2NextRetest.high;
        candle2NextOBStatus = retestPrice >= candle2NextOB.low && retestPrice <= candle2NextOB.high;
      }
    }

    if (!nextSwing) {
      return {
        type: 'confirmedSetup',
        direction,
        candle1: extremeCandle,
        candle2Previous: previousSwing ? candles[previousSwing.index] : null,
        candle2PreviousStatus,
        candle2PreviousVshape,
        candle2PreviousBreakout,
        candle2PreviousRetest,
        candle2PreviousRetestVshape,
        candle2PreviousMitigationIndex: candle2PreviousMitigation ? candle2PreviousMitigation.index : null,
        candle2PreviousMitigationFormattedTime: candle2PreviousMitigation ? this._formatTime(candle2PreviousMitigation.time) : null,
        candle2PreviousMitigationStatus,
        candle2Next: null,
        candle2NextStatus: null,
        candle2NextVshape: null,
        candle2NextBreakout: null,
        candle2NextRetest: null,
        candle2NextRetestVshape: null,
        candle2NextMitigationIndex: null,
        candle2NextMitigationFormattedTime: null,
        candle2NextMitigationStatus: null,
        candle2PreviousOBIndex: candle2PreviousOB ? candle2PreviousOB.index : null,
        candle2PreviousOBFormattedTime: candle2PreviousOB ? candle2PreviousOB.formattedTime : null,
        candle2PreviousOBStatus,
        candle2NextOBIndex: null,
        candle2NextOBFormattedTime: null,
        candle2NextOBStatus: false,
        level: direction === 'bullish' ? extremeCandle.low : extremeCandle.high,
        levelType: direction === 'bullish' ? 'support' : 'resistance'
      };
    }

    const nextSwingCandle = candles[nextSwing.index];
    return {
      type: 'confirmedSetup',
      direction,
      candle1: extremeCandle,
      candle2Previous: previousSwing ? candles[previousSwing.index] : null,
      candle2PreviousStatus,
      candle2PreviousVshape,
      candle2PreviousBreakout,
      candle2PreviousRetest,
      candle2PreviousRetestVshape,
      candle2PreviousMitigationIndex: candle2PreviousMitigation ? candle2PreviousMitigation.index : null,
      candle2PreviousMitigationFormattedTime: candle2PreviousMitigation ? this._formatTime(candle2PreviousMitigation.time) : null,
      candle2PreviousMitigationStatus,
      candle2Next: nextSwingCandle,
      candle2NextStatus,
      candle2NextVshape,
      candle2NextBreakout,
      candle2NextRetest,
      candle2NextRetestVshape,
      candle2NextMitigationIndex: candle2NextMitigation ? candle2NextMitigation.index : null,
      candle2NextMitigationFormattedTime: candle2NextMitigation ? this._formatTime(candle2NextMitigation.time) : null,
      candle2NextMitigationStatus,
      candle2PreviousOBIndex: candle2PreviousOB ? candle2PreviousOB.index : null,
      candle2PreviousOBFormattedTime: candle2PreviousOB ? candle2PreviousOB.formattedTime : null,
      candle2PreviousOBStatus,
      candle2NextOBIndex: candle2NextOB ? candle2NextOB.index : null,
      candle2NextOBFormattedTime: candle2NextOB ? candle2NextOB.formattedTime : null,
      candle2NextOBStatus,
      level: direction === 'bullish' ? Math.min(extremeCandle.low, nextSwingCandle.low) : Math.max(extremeCandle.high, nextSwingCandle.high),
      levelType: direction === 'bullish' ? 'support' : 'resistance'
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
      bearish: patterns.filter(p => p.direction === 'bearish').length,
      byStage: {
        stage1: patterns.filter(p => p.stage === 1).length,
        stage2: patterns.filter(p => p.stage === 2).length,
        stage3: patterns.filter(p => p.stage === 3).length,
      },
      complete: patterns.filter(p => p.isComplete).length
    };
  }

  clearOld(symbol, granularity, maxAge) {
    const patterns = this.get(symbol, granularity);
    const cutoff = Date.now() - maxAge;
    this.store[symbol][granularity] = patterns.filter(p => p.timestamp > cutoff);
  }
}

module.exports = new PatternEngine();