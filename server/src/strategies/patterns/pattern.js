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
      vShapeWickRatio: 2.0,        // V-shape wick must be 2x body size
      equalLevelTolerance: 0.002,   // 0.2% tolerance for equal highs/lows
      minCandlesBetweenSwings: 3,   // Minimum candles between swings
      retestScanRange: 30,          // Maximum candles to scan for retest
      obScanRange: 10,              // Maximum candles to scan for OB after a swing
      ...options.config
    };
  }

  _initStore(symbol, granularity) {
    if (!this.store[symbol]) {
      this.store[symbol] = {};
    }
    if (!this.store[symbol][granularity]) {
      this.store[symbol][granularity] = [];
    }
  }

  _buildSwingIndex(swings) {
    const indexMap = new Map();
    const prevSameType = new Map();
    const nextSameType = new Map();

    let lastHigh = null;
    let lastLow = null;

    // Forward pass (build previous same-type)
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

    // Backward pass (build next same-type)
    lastHigh = null;
    lastLow = null;

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

    return {
      indexMap,
      prevSameType,
      nextSameType
    };
  }

  /**
   * Enriches candles with index property and computed values
   */
  _enrichCandles(candles) {
    return candles.map((candle, index) => ({
      ...candle,
      index,
      bodySize: Math.abs(candle.open - candle.close),
      upperWick: candle.high - Math.max(candle.open, candle.close),
      lowerWick: Math.min(candle.open, candle.close) - candle.low
    }));
  }

  /**
   * Format timestamp to human-readable date
   */
  _formatTime(timestamp) {
    if (!timestamp) return null;
    const date = new Date(Number(timestamp) * 1000);
    return date.toISOString().replace('T', ' ').substring(0, 19);
  }

  /**
   * Get direction from swing sequence
   * For a sweep to occur, both swings must be the SAME type:
   * - HIGH sweeps previous HIGH = bearish (liquidity grab above, then down)
   * - LOW sweeps previous LOW = bullish (liquidity grab below, then up)
   */
  _getPatternDirection(currentSwing, previousSwing) {
    if (currentSwing.type === 'low' && previousSwing.type === 'low') {
      return 'bullish';
    }
    if (currentSwing.type === 'high' && previousSwing.type === 'high') {
      return 'bearish';
    }
    return null; // Should never happen - we filter for same types
  }

  /**
   * Find the most recent swing of the same type
   */
  _findPreviousSameTypeSwing(swings, currentIndex, type) {
    for (let i = currentIndex - 1; i >= 0; i--) {
      if (swings[i].type === type) {
        return swings[i];
      }
    }
    return null;
  }

  /**
   * Find the next swing of the same type after a given index
   */
  _findNextSameTypeSwing(swings, currentIndex, type) {
    for (let i = currentIndex + 1; i < swings.length; i++) {
      if (swings[i].type === type) {
        return swings[i];
      }
    }
    return null;
  }

  /**
   * Find the previous extreme swing before a given index
   * For bullish: Find the swing low with the lowest low value (but must be ABOVE the current swing low)
   * For bearish: Find the swing high with the highest high value (but must be BELOW the current swing high)
   * @param {number} minIndex - Optional minimum index boundary (swing must be at or after this index)
   */
  _findPreviousExtremeSwing(swings, currentIndex, direction, currentSwingLevel, minIndex = 0) {
    const type = direction === 'bullish' ? 'low' : 'high';
    let extremeSwing = null;
    let extremeValue = direction === 'bullish' ? Infinity : -Infinity;

    for (let i = currentIndex - 1; i >= 0; i--) {
      if (swings[i].type === type) {
        // Skip swings that are before the minimum index boundary
        if (swings[i].index < minIndex) {
          continue;
        }
        
        if (direction === 'bullish') {
          // For bullish: find the lowest low that is ABOVE the current swing low
          if (swings[i].low > currentSwingLevel && swings[i].low < extremeValue) {
            extremeValue = swings[i].low;
            extremeSwing = swings[i];
          }
        } else {
          // For bearish: find the highest high that is BELOW the current swing high
          if (swings[i].high < currentSwingLevel && swings[i].high > extremeValue) {
            extremeValue = swings[i].high;
            extremeSwing = swings[i];
          }
        }
      }
    }

    return extremeSwing;
  }

  /**
   * Find retest after breakout for confirmed setup
   * Bullish: extreme low between currentSwingPrice and vshapePrice after breakout
   * Bearish: extreme high between vshapePrice and currentSwingPrice after breakout
   */
  _findConfirmedSetupRetest(vshape, currentSwing, breakout, candles, direction) {
    if (!breakout || !vshape || !currentSwing) return null;
    
    const startIdx = breakout.index + 1;
    if (startIdx >= candles.length) return null;

    const vshapePrice = direction === 'bullish' ? vshape.high : vshape.low;
    const currentSwingPrice = direction === 'bullish' ? currentSwing.low : currentSwing.high;

    let retestCandle = null;

    if (direction === 'bullish') {
      // Find extreme low between currentSwingPrice and vshapePrice
      let extremeLow = Infinity;
      for (let i = startIdx; i < candles.length; i++) {
        const c = candles[i];
        if (!c) continue;
        
        // Check if low is between currentSwing and vShape
        if (c.low >= currentSwingPrice && c.low <= vshapePrice) {
          if (c.low < extremeLow) {
            extremeLow = c.low;
            retestCandle = c;
          }
        }
      }
    } else {
      // Bearish: Find extreme high between vshapePrice and currentSwingPrice
      let extremeHigh = -Infinity;
      for (let i = startIdx; i < candles.length; i++) {
        const c = candles[i];
        if (!c) continue;
        
        // Check if high is between vShape and currentSwing
        if (c.high <= currentSwingPrice && c.high >= vshapePrice) {
          if (c.high > extremeHigh) {
            extremeHigh = c.high;
            retestCandle = c;
          }
        }
      }
    }

    return retestCandle;
  }

  /**
   * Helper to find the first OB satisfying price conditions relative to a V-shape,
   * within a limited number of candles after startIndex.
   * @param {number} startIndex - Starting candle index (inclusive)
   * @param {Map} obMap - Map of OB entries keyed by candle index
   * @param {string} direction - 'bullish' or 'bearish'
   * @param {object} vShapeCandle - The V-shape candle object
   * @param {number} maxScanCandles - Maximum number of candles to scan after startIndex
   * @returns {object|null} - OB entry if found, else null
   */
  _findOBForSetup(startIndex, obMap, direction, vShapeCandle, maxScanCandles = this.config.obScanRange) {
    if (!vShapeCandle || !obMap) return null;
    
    const vShapeLow = vShapeCandle.low;
    const vShapeHigh = vShapeCandle.high;
    const endIndex = startIndex + maxScanCandles;
    
    // Filter OBs that are within the scan range and sort by index
    const relevantOBs = Array.from(obMap.entries())
      .filter(([idx]) => idx >= startIndex && idx <= endIndex)
      .sort((a, b) => a[0] - b[0]);

    for (const [idx, ob] of relevantOBs) {
      if (direction === 'bullish') {
        // For bullish: OB low must be below V-shape low
        if (ob.low < vShapeLow) {
          return ob;
        }
      } else {
        // For bearish: OB high must be above V-shape high
        if (ob.high > vShapeHigh) {
          return ob;
        }
      }
    }
    return null;
  }

  /**
   * Find mitigation candle for candle2Previous
   * For bullish: First green candle between vshape and candle1
   * For bearish: First red candle between vshape and candle1
   */
  _findCandle2PreviousMitigation(vShapeIndex, candle1Index, candles, direction) {
    if (!vShapeIndex || !candle1Index) return null;
    
    let mitigationCandle = null;
    
    if (direction === 'bullish') {
      // First green candle between vshape and candle1
      for (let i = vShapeIndex; i < candle1Index; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.close >= candle.open) {
          mitigationCandle = candle;
          break;
        }
      }
    } else {
      // First red candle between vshape and candle1
      for (let i = vShapeIndex; i < candle1Index; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.close < candle.open) {
          mitigationCandle = candle;
          break;
        }
      }
    }
    
    return mitigationCandle;
  }

  /**
   * Find mitigation candle for candle2Next
   * For bullish: First green candle between vshape and candle2Next
   * For bearish: First red candle between vshape and candle2Next
   */
  _findCandle2NextMitigation(vShapeIndex, candle2NextIndex, candles, direction) {
    if (!vShapeIndex || !candle2NextIndex) return null;
    
    let mitigationCandle = null;
    
    if (direction === 'bullish') {
      // First green candle between vshape and candle2Next
      for (let i = vShapeIndex; i < candle2NextIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.close >= candle.open) {
          mitigationCandle = candle;
          break;
        }
      }
    } else {
      // First red candle between vshape and candle2Next
      for (let i = vShapeIndex; i < candle2NextIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.close < candle.open) {
          mitigationCandle = candle;
          break;
        }
      }
    }
    
    return mitigationCandle;
  }

  /**
   * Check if a retest of a mitigation block is valid, starting the scan from the given index.
   * Returns:
   *   - 'ACTIVE'   : the first crossing after startIndex is valid (no subsequent invalidating close/open).
   *   - 'EXPIRED'  : the first crossing after startIndex was later invalidated.
   *   - null       : no crossing of the mitigation block occurred after startIndex.
   */
  _checkMitigationRetest(mtCandle, startIndex, candles, direction) {
    if (!mtCandle) return null;

    const mtLow = mtCandle.low;
    const mtHigh = mtCandle.high;

    // Find the first candle at or after startIndex that crosses the mitigation block
    let firstCrossCandle = null;

    for (let i = startIndex; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === 'bullish') {
        if (c.low < mtLow) {
          firstCrossCandle = c;
          break;
        }
      } else {
        if (c.high > mtHigh) {
          firstCrossCandle = c;
          break;
        }
      }
    }

    if (!firstCrossCandle) return null; // never reached

    // Now check subsequent candles: any close/open beyond the first cross candle's extreme?
    const refValue = direction === 'bullish' ? firstCrossCandle.low : firstCrossCandle.high;

    for (let i = firstCrossCandle.index + 1; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === 'bullish') {
        if (c.close < refValue || c.open < refValue) {
          return 'EXPIRED';
        }
      } else {
        if (c.close > refValue || c.open > refValue) {
          return 'EXPIRED';
        }
      }
    }

    return 'ACTIVE';
  }

  /**
   * Main detection method with detailed metadata
   */
  async detect(symbol, granularity, candles) {
    this._initStore(symbol, granularity);

    // Ensure swings are detected before proceeding
    await swingEngine.detectAll(symbol, granularity, candles.slice(0, -1), 1);

    const swings = swingEngine.get(symbol, granularity) || [];
    const swingIndex = this._buildSwingIndex(swings);
    
    // Debug logging
    this.logger.info(`[PatternEngine] Detecting patterns for ${symbol} ${granularity}`);
    this.logger.info(`[PatternEngine] Candles: ${candles.length}, Swings: ${swings.length}`);

    if (swings.length < 2) {
      this.logger.warn(`[PatternEngine] Not enough swings (${swings.length}) for pattern detection`);
      return [];
    }

    const enrichedCandles = this._enrichCandles(candles);

    // Pre-calculate OB data and map to indices for efficient lookup
    const oblvData = processOBLV(symbol, granularity, enrichedCandles);
    const obMap = new Map();
    oblvData.forEach(entry => {
      if (entry.OB && entry.OBFormattedTime) {
        const candle = enrichedCandles.find(c => c.formattedTime === entry.OBFormattedTime);
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

    const patterns = [];
    
    let rejectionStats = {
      noPreviousSameType: 0,
      noSweep: 0,
      noVShape: 0,
      noBreakout: 0,
      total: 0
    };

    for (let i = 1; i < swings.length; i++) {
      const currentSwing = swings[i];
      
      rejectionStats.total++;

      // Find the most recent swing of the same type
      const previousSwing = this._findPreviousSameTypeSwing(swings, i, currentSwing.type);
      
      if (!previousSwing) {
        rejectionStats.noPreviousSameType++;
        this.logger.debug(`[PatternEngine] No previous ${currentSwing.type} swing found for index ${i}`);
        continue;
      }

      // Check minimum candles requirement
      const candlesBetween = currentSwing.index - previousSwing.index;
      if (candlesBetween < this.config.minCandlesBetweenSwings) {
        this.logger.debug(`[PatternEngine] Not enough candles between swings: ${candlesBetween} < ${this.config.minCandlesBetweenSwings}`);
        continue;
      }

      const direction = this._getPatternDirection(currentSwing, previousSwing);
      
      const setup = this.identifySetup(
        currentSwing, 
        previousSwing, 
        enrichedCandles,
        direction,
        swings,
        swingIndex
      );
      
      if (setup) {
        // Build the full pattern with all stages (up to stage 3)
        this._buildPatternStages(setup, enrichedCandles, direction, swings, swingIndex, obMap);
        
        // Add rich metadata
        const enrichedPattern = this._enrichPatternMetadata(setup, enrichedCandles);
        patterns.push(enrichedPattern);
        
        this.logger.info(`[PatternEngine] Found ${direction} pattern from swing ${previousSwing.index} (${previousSwing.type}) to ${currentSwing.index} (${currentSwing.type})`);
        
        if (this.emitEvents) {
          this.emit('patternDetected', {
            symbol,
            granularity,
            pattern: enrichedPattern
          });
        }
      }
    }

    this.logger.info(`[PatternEngine] Total patterns detected: ${patterns.length}`);
    this.logger.info(`[PatternEngine] Rejection stats:`, rejectionStats);
    this.store[symbol][granularity] = patterns;
    return patterns;
  }

  /**
   * Enrich pattern with detailed metadata similar to levels route
   */
  _enrichPatternMetadata(pattern, candles) {
    const result = {
      type: 'PATTERN',
      direction: pattern.direction,
      
      // Stage 1: Initial Setup
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
      
      // Sweep data
      firstSweepCandleIndex: pattern.sweepData?.firstSweepCandleIndex || null,
      firstSweepCandleClose: pattern.sweepData?.firstSweepCandleClose || null,
      
      // V-shape candle
      vShapeCandleIndex: pattern.vShapeCandle?.index || null,
      vShapeCandlePrice: pattern.direction === 'bullish' 
        ? pattern.vShapeCandle?.high 
        : pattern.vShapeCandle?.low,
      vShapeCandleTime: pattern.vShapeCandle?.time || null,
      vShapeCandleFormattedTime: this._formatTime(pattern.vShapeCandle?.time),
      vShapeWickSize: pattern.direction === 'bullish'
        ? pattern.vShapeCandle?.lowerWick
        : pattern.vShapeCandle?.upperWick,
      
      // Initial breakout
      breakoutIndex: pattern.breakout?.index || null,
      breakoutPrice: pattern.breakout?.close || null,
      breakoutTime: pattern.breakout?.time || null,
      breakoutFormattedTime: this._formatTime(pattern.breakout?.time),
      
      // Stage 2: Retest
      retestIndex: pattern.retest?.index || null,
      retestPrice: pattern.retest?.close || null,
      retestTime: pattern.retest?.time || null,
      retestFormattedTime: this._formatTime(pattern.retest?.time),
      retestLevel: pattern.retest?.retestLevel || null,
      retestType: pattern.retest?.retestType || null,
      
      // Retest V-shape candle
      retestVshapeIndex: pattern.retest?.vShapeCandle?.index || null,
      retestVshapePrice: pattern.direction === 'bullish'
        ? pattern.retest?.vShapeCandle?.high
        : pattern.retest?.vShapeCandle?.low,
      retestVshapeTime: pattern.retest?.vShapeCandle?.time || null,
      retestVshapeFormattedTime: this._formatTime(pattern.retest?.vShapeCandle?.time),
      retestVshapeLevel: pattern.direction === 'bullish'
        ? pattern.retest?.vShapeCandle?.high
        : pattern.retest?.vShapeCandle?.low,
      
      // Retest Breakout
      retestBreakoutIndex: pattern.retest?.breakout?.index || null,
      retestBreakoutPrice: pattern.retest?.breakout?.close || null,
      retestBreakoutTime: pattern.retest?.breakout?.time || null,
      retestBreakoutFormattedTime: this._formatTime(pattern.retest?.breakout?.time),
      
      // Stage 3: Confirmed Setup
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

      confirmedSetupCandle2NextMitigationIndex: pattern.confirmedSetup?.candle2NextMitigationIndex || null,
      confirmedSetupCandle2NextMitigationFormattedTime: pattern.confirmedSetup?.candle2NextMitigationFormattedTime || null,
      confirmedSetupCandle2NextMitigationStatus: pattern.confirmedSetup?.candle2NextMitigationStatus ?? null,
      
      confirmedSetupCandle2NextOBIndex: pattern.confirmedSetup?.candle2NextOBIndex || null,
      confirmedSetupCandle2NextOBFormattedTime: pattern.confirmedSetup?.candle2NextOBFormattedTime || null,
      confirmedSetupCandle2NextOBStatus: pattern.confirmedSetup?.candle2NextOBStatus ?? false,
      
      // Pattern completion status (now stage 3 is considered complete)
      stage: this._getPatternStage(pattern),
      isComplete: !!(pattern.confirmedSetup),
      
      // Timestamps for sorting/filtering
      formattedTime: this._formatTime(pattern.currentSwing?.time),
      time: pattern.currentSwing?.time || null,
      timestamp: pattern.timestamp || Date.now(),
    };

    return result;
  }

  /**
   * Determine which stage the pattern has reached (max stage 3 now)
   */
  _getPatternStage(pattern) {
    if (pattern.confirmedSetup) return 3;
    if (pattern.retest) return 2;
    return 1;
  }

  /**
   * Recursively builds all pattern stages (up to stage 3)
   */
  _buildPatternStages(setup, candles, direction, swings, swingIndex, obMap) {
    // Stage 2: Retest
    const retest = this.identifyRetest(
      { ...setup, context: 'primary' },
      candles,
      direction
    );
    if (!retest) return;
    setup.retest = retest;

    // Stage 3: Confirmed Setup
    const confirmedSetup = this.identifyConfirmedSetup(retest, candles, direction, swings, swingIndex, obMap);
    if (!confirmedSetup) return;
    setup.confirmedSetup = confirmedSetup;
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
    const breakout = this.identifyBreakoutOfLevel(
      breakoutLevel,
      candles,
      currentSwing.index,
      direction,
      currentSwing
    );
    
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
    const result = {
      isSweep: false,
      firstSweepCandleIndex: null,
      firstSweepCandleClose: null
    };

    // Both swings must be the SAME type
    if (currentSwing.type !== previousSwing.type) {
      return result;
    }

    const startIndex = previousSwing.index + 1;
    const endIndex = currentSwing.index;

    if (direction === 'bullish') {
      // Bullish: Both are LOWs
      // Find first candle that sweeps previousSwing.low (by wick or body)
      let firstSweepCandle = null;
      
      for (let i = startIndex; i <= endIndex; i++) {
        const candle = candles[i];
        // Sweep can happen by wick (low goes below) OR body (close goes below)
        if (candle.low < previousSwing.low) {
          firstSweepCandle = candle;
          break;
        }
      }

      if (!firstSweepCandle) {
        return result; // No sweep occurred
      }

      // After finding the first sweep candle, check subsequent candles
      // Subsequent candles can WICK below the first sweep candle's low
      // BUT they must NOT CLOSE or OPEN below the first sweep candle's low
      for (let i = firstSweepCandle.index + 1; i <= endIndex; i++) {
        const candle = candles[i];
        
        // Invalid if close or open goes below the first sweep candle's low
        if (candle.close < firstSweepCandle.low || candle.open < firstSweepCandle.low) {
          this.logger.debug(`[PatternEngine] Sweep invalidated: candle ${i} close/open went below first sweep candle low (${firstSweepCandle.low})`);
          return result;
        }
      }
      
      // Valid sweep
      result.isSweep = true;
      result.firstSweepCandleIndex = firstSweepCandle.index;
      result.firstSweepCandleClose = firstSweepCandle.close;

    } else {
      // Bearish: Both are HIGHs
      // Find first candle that sweeps previousSwing.high (by wick or body)
      let firstSweepCandle = null;
      
      for (let i = startIndex; i <= endIndex; i++) {
        const candle = candles[i];
        // Sweep can happen by wick (high goes above) OR body (close goes above)
        if (candle.high > previousSwing.high) {
          firstSweepCandle = candle;
          break;
        }
      }

      if (!firstSweepCandle) {
        return result; // No sweep occurred
      }

      // After finding the first sweep candle, check subsequent candles
      // Subsequent candles can WICK above the first sweep candle's high
      // BUT they must NOT CLOSE or OPEN above the first sweep candle's high
      for (let i = firstSweepCandle.index + 1; i <= endIndex; i++) {
        const candle = candles[i];
        
        // Invalid if close or open goes above the first sweep candle's high
        if (candle.close > firstSweepCandle.high || candle.open > firstSweepCandle.high) {
          this.logger.debug(`[PatternEngine] Sweep invalidated: candle ${i} close/open went above first sweep candle high (${firstSweepCandle.high})`);
          return result;
        }
      }
      
      // Valid sweep
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

    if (endIndex <= startIndex) {
      return null;
    }

    let extremumCandle = null;
    
    if (direction === 'bullish') {
      // For bullish: find highest high between swings
      let maxHigh = -Infinity;
      
      // Check candles between the swings
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.high > maxHigh) {
          maxHigh = candle.high;
          extremumCandle = candle;
        }
      }
      
      // Also check if the previousSwing (first swing) has a higher high
      const previousSwingCandle = candles[minSwingIndex];
      if (previousSwingCandle && previousSwingCandle.high > maxHigh) {
        extremumCandle = previousSwingCandle;
      }
    } else { // bearish
      // For bearish: find lowest low between swings
      let minLow = Infinity;
      
      // Check candles between the swings
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.low < minLow) {
          minLow = candle.low;
          extremumCandle = candle;
        }
      }
      
      // Also check if the previousSwing (first swing) has a lower low
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
      
      // Skip if this is the currentSwing candle itself
      if (candle.index === currentSwing.index) {
        continue;
      }
      
      if (direction === 'bullish') {
        // Invalid if price wick crossed below currentSwing low before breakout
        if (candle.low < currentSwing.low) {
          return null; // Invalid breakout
        }
        // Breakout happens when price closes OR opens above level
        if (candle.close > level || candle.open > level) {
          return candle;
        }
      } else { // bearish
        // Invalid if price wick crossed above currentSwing high before breakout
        if (candle.high > currentSwing.high) {
          return null; // Invalid breakout
        }
        // Breakout happens when price closes OR opens below level
        if (candle.close < level || candle.open < level) {
          return candle;
        }
      }
    }
    return null;
  }

  identifyRetest(setup, candles, direction) {
    if (!setup.breakout) {
      return null;
    }

    const startIndex = setup.breakout.index + 1;

    if (startIndex >= candles.length) {
      return null;
    }

    // Limit scan range to configured number of candles (default 30)
    const maxScanRange = this.config.retestScanRange;
    const endIndex = Math.min(startIndex + maxScanRange, candles.length);

    // Get currentSwing level for invalidation check (if available)
    const currentSwing = setup.currentSwing;
    let currentSwingLevel = null;
    
    if (currentSwing) {
      currentSwingLevel = direction === 'bullish' ? currentSwing.low : currentSwing.high;
    }

    let extremumCandle = null;

    if (direction === 'bullish') {
      let minLow = Infinity;
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        
        // Invalidate if price crosses below currentSwing low (only if currentSwing exists)
        if (currentSwingLevel !== null && candle.low < currentSwingLevel) {
          this.logger.debug(`[PatternEngine] Retest invalidated: price crossed below currentSwing low at candle ${i}`);
          return null;
        }
        
        if (candle.low < minLow) {
          minLow = candle.low;
          extremumCandle = candle;
        }
      }
    } else { // bearish
      let maxHigh = -Infinity;
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        
        // Invalidate if price crosses above currentSwing high (only if currentSwing exists)
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
      // Find the V-shape candle after the retest extremum
      const vShapeCandle = this.findRetestVShapeCandle(
        setup.breakout,
        extremumCandle,
        candles,
        direction
      );

      // Find the breakout of the V-shape level (if V-shape exists)
      let retestBreakout = null;
      if (vShapeCandle) {
        const vShapeLevel = direction === 'bullish' ? vShapeCandle.high : vShapeCandle.low;
        retestBreakout = this.findRetestBreakout(
          vShapeLevel,
          candles,
          extremumCandle.index,
          direction
        );
      }

      return {
        ...extremumCandle,
        retestLevel: direction === 'bullish' ? extremumCandle.low : extremumCandle.high,
        retestType: direction === 'bullish' ? 'support' : 'resistance',
        vShapeCandle: vShapeCandle,
        breakout: retestBreakout,
        context: setup.context || 'unknown'
      };
    }

    return null;
  }

  /**
   * Find the breakout of the retest V-shape level
   * Similar to identifyBreakoutOfLevel but specifically for the retest phase
   */
  findRetestBreakout(level, candles, startIndex, direction) {
    for (let i = startIndex + 1; i < candles.length; i++) {
      const candle = candles[i];
      if (!candle) continue;
      
      if (direction === 'bullish') {
        // Breakout happens when price closes above the V-shape level
        if (candle.close > level) {
          return candle;
        }
      } else { // bearish
        // Breakout happens when price closes below the V-shape level
        if (candle.close < level) {
          return candle;
        }
      }
    }
    return null;
  }

  /**
   * Find the V-shape candle between breakout and retest extremum
   * Similar to findVShapeCandle but specifically for the retest phase
   */
  findRetestVShapeCandle(breakoutCandle, retestCandle, candles, direction) {
    const startIndex = breakoutCandle.index + 1;
    const endIndex = retestCandle.index;

    if (endIndex <= startIndex) {
      return null;
    }

    let extremumCandle = null;
    
    if (direction === 'bullish') {
      // For bullish retest: find highest high between breakout and retest
      let maxHigh = -Infinity;
      
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.high > maxHigh) {
          maxHigh = candle.high;
          extremumCandle = candle;
        }
      }
      
      // Also check the breakout candle itself
      if (breakoutCandle.high > maxHigh) {
        extremumCandle = breakoutCandle;
      }
    } else { // bearish
      // For bearish retest: find lowest low between breakout and retest
      let minLow = Infinity;
      
      for (let i = startIndex; i < endIndex; i++) {
        const candle = candles[i];
        if (!candle) continue;
        if (candle.low < minLow) {
          minLow = candle.low;
          extremumCandle = candle;
        }
      }
      
      // Also check the breakout candle itself
      if (breakoutCandle.low < minLow) {
        extremumCandle = breakoutCandle;
      }
    }

    return extremumCandle;
  }

  /**
   * Find the extreme candle after retest breakout that may cross the retest level
   * Rules:
   * 1. Can cross retest level by WICK only (no body cross)
   * 2. OR one candlestick can close/open beyond retest level
   * 3. BUT no subsequent candlesticks can close/open beyond that first crossing candlestick
   */
  findExtremeCandleAfterRetestBreakout(retest, candles, direction) {
    if (!retest.breakout) {
      return null;
    }

    const startIndex = retest.breakout.index + 1;
    const retestLevel = direction === 'bullish' ? retest.low : retest.high;

    if (startIndex >= candles.length) {
      return null;
    }

    if (direction === 'bullish') {
      // For bullish: find candle with lowest low after retest breakout
      let extremeCandle = null;
      let minLow = Infinity;
      let firstBodyCrossCandle = null; // First candle whose close or open crosses below retest level

      // Find the candle with the lowest low AND track the first candle whose body crosses retest level
      for (let i = startIndex; i < candles.length; i++) {
        const candle = candles[i];
        if (!candle) continue;
        
        // Track first candle whose BODY (close or open) crosses below retest level
        if (!firstBodyCrossCandle && (candle.close < retestLevel || candle.open < retestLevel)) {
          firstBodyCrossCandle = candle;
        }
        
        if (candle.low < minLow) {
          minLow = candle.low;
          extremeCandle = candle;
        }
      }

      if (!extremeCandle) {
        return null;
      }

      // If a candle's body crossed below the retest level
      // Check that no subsequent candles close or open below the FIRST body cross candle's low
      if (firstBodyCrossCandle) {
        for (let i = firstBodyCrossCandle.index + 1; i < candles.length; i++) {
          const candle = candles[i];
          if (!candle) continue;
          
          // Invalid if close or open goes below the first body cross candle's low
          if (candle.close < firstBodyCrossCandle.low || candle.open < firstBodyCrossCandle.low) {
            this.logger.debug(`[PatternEngine] Extreme candle invalidated: candle ${i} close/open went below first body cross candle low (${firstBodyCrossCandle.low})`);
            return null;
          }
        }
      }

      return extremeCandle;

    } else { // bearish
      // For bearish: find candle with highest high after retest breakout
      let extremeCandle = null;
      let maxHigh = -Infinity;
      let firstBodyCrossCandle = null; // First candle whose close or open crosses above retest level

      // Find the candle with the highest high AND track the first candle whose body crosses retest level
      for (let i = startIndex; i < candles.length; i++) {
        const candle = candles[i];
        if (!candle) continue;
        
        // Track first candle whose BODY (close or open) crosses above retest level
        if (!firstBodyCrossCandle && (candle.close > retestLevel || candle.open > retestLevel)) {
          firstBodyCrossCandle = candle;
        }
        
        if (candle.high > maxHigh) {
          maxHigh = candle.high;
          extremeCandle = candle;
        }
      }

      if (!extremeCandle) {
        return null;
      }

      // If a candle's body crossed above the retest level
      // Check that no subsequent candles close or open above the FIRST body cross candle's high
      if (firstBodyCrossCandle) {
        for (let i = firstBodyCrossCandle.index + 1; i < candles.length; i++) {
          const candle = candles[i];
          
          // Invalid if close or open goes above the first body cross candle's high
          if (candle.close > firstBodyCrossCandle.high || candle.open > firstBodyCrossCandle.high) {
            this.logger.debug(`[PatternEngine] Extreme candle invalidated: candle ${i} close/open went above first body cross candle high (${firstBodyCrossCandle.high})`);
            return null;
          }
        }
      }

      return extremeCandle;
    }
  }

  /**
   * Check if two swings form an S SETUP (valid sweep pattern)
   * Returns 'S SETUP' if valid, null otherwise
   */
  _checkSSetup(swing1, swing2, candles, direction) {
    const sweepData = this.isWickSweep(swing2, swing1, candles, direction);
    return sweepData.isSweep ? 'S SETUP' : null;
  }

  /**
   * Check if nextSwing forms a DOUBLE EQ with candle1
   * Returns 'DOUBLE EQ' if valid, null otherwise
   */
  _checkDoubleEq(candle1, nextSwing, direction) {
    if (!candle1 || !nextSwing) {
      return null;
    }

    const isRedCandle = candle1.close < candle1.open;
    const isGreenCandle = candle1.close >= candle1.open;

    if (direction === 'bullish') {
      // For bullish, nextSwing should be a low
      const nextSwingLow = nextSwing.low;
      
      if (isRedCandle) {
        // Between low and close of candle1
        if (nextSwingLow >= candle1.low && nextSwingLow <= candle1.close) {
          return 'DOUBLE EQ';
        }
      } else if (isGreenCandle) {
        // Between low and open of candle1
        if (nextSwingLow >= candle1.low && nextSwingLow <= candle1.open) {
          return 'DOUBLE EQ';
        }
      }
    } else { // bearish
      // For bearish, nextSwing should be a high
      const nextSwingHigh = nextSwing.high;
      
      if (isRedCandle) {
        // Between high and open of candle1 (for bearish, red candle open is higher)
        if (nextSwingHigh <= candle1.high && nextSwingHigh >= candle1.open) {
          return 'DOUBLE EQ';
        }
      } else if (isGreenCandle) {
        // Between high and close of candle1
        if (nextSwingHigh <= candle1.high && nextSwingHigh >= candle1.close) {
          return 'DOUBLE EQ';
        }
      }
    }

    return null;
  }

  identifyConfirmedSetup(retest, candles, direction, swings, swingIndex, obMap) {
    // First, validate that retest has both vShapeCandle and breakout
    if (!retest.vShapeCandle || !retest.breakout) {
      this.logger.debug(`[PatternEngine] Retest must have vShapeCandle and breakout before confirmed setup can be identified`);
      return null;
    }

    // Find the extreme candle after retest breakout
    const extremeCandle = this.findExtremeCandleAfterRetestBreakout(retest, candles, direction);
    
    if (!extremeCandle) {
      this.logger.debug(`[PatternEngine] No valid extreme candle found after retest breakout`);
      return null;
    }

    // Find the extreme candle as a swing
    const extremeCandleSwing = swings.find(s => s.index === extremeCandle.index);
    
    if (!extremeCandleSwing) {
      this.logger.debug(`[PatternEngine] Extreme candle at index ${extremeCandle.index} is not a swing`);
      return null;
    }

    // Find previous EXTREME swing (lowest low for bullish, highest high for bearish)
    const swingPosition = swings.findIndex(s => s.index === extremeCandleSwing.index);
    const currentSwingLevel = direction === 'bullish' ? extremeCandle.low : extremeCandle.high;
    
    const previousSwing = this._findPreviousExtremeSwing(
      swings,
      swingPosition,
      direction,
      currentSwingLevel,
      retest.breakout.index
    );
    
    // Check S SETUP status
    const candle2PreviousStatus = previousSwing 
      ? this._checkSSetup(previousSwing, extremeCandleSwing, candles, direction)
      : null;

    // Find V-shape and breakout for candle2Previous
    let candle2PreviousVshape = null;
    let candle2PreviousBreakout = null;
    let candle2PreviousRetest = null;
    
    if (candle2PreviousStatus === 'S SETUP' && previousSwing) {
      candle2PreviousVshape = this.findVShapeCandle(
        previousSwing,
        extremeCandleSwing,
        candles,
        direction
      );
      
      if (candle2PreviousVshape) {
        const vShapeLevel = direction === 'bullish' ? candle2PreviousVshape.high : candle2PreviousVshape.low;
        candle2PreviousBreakout = this.identifyBreakoutOfLevel(
          vShapeLevel,
          candles,
          extremeCandleSwing.index,
          direction,
          extremeCandleSwing
        );
        
        // Find retest for candle2Previous
        if (candle2PreviousBreakout) {
          candle2PreviousRetest = this._findConfirmedSetupRetest(
            candle2PreviousVshape,
            extremeCandleSwing,
            candle2PreviousBreakout,
            candles,
            direction
          );
        }
      }
    }

    // Find Mitigation for candle2Previous
    let candle2PreviousMitigation = null;
    if (candle2PreviousVshape) {
      candle2PreviousMitigation = this._findCandle2PreviousMitigation(
        candle2PreviousVshape.index,
        extremeCandle.index,
        candles,
        direction
      );
    }

    // Compute candle2PreviousMitigationStatus using the new method that starts from the V-shape index
    let candle2PreviousMitigationStatus = null;
    if (candle2PreviousMitigation && candle2PreviousVshape) {
      candle2PreviousMitigationStatus = this._checkMitigationRetest(
        candle2PreviousMitigation,
        candle2PreviousVshape.index,
        candles,
        direction
      );
    }

    // Find Previous OB - Start scan from candle1 (extremeCandle)
    let candle2PreviousOB = null;
    let candle2PreviousOBStatus = false;
    if (candle2PreviousVshape) {
      candle2PreviousOB = this._findOBForSetup(extremeCandle.index, obMap, direction, candle2PreviousVshape, this.config.obScanRange);
      if (candle2PreviousOB && candle2PreviousRetest) {
        const retestPrice = direction === 'bullish' ? candle2PreviousRetest.low : candle2PreviousRetest.high;
        candle2PreviousOBStatus = retestPrice >= candle2PreviousOB.low && retestPrice <= candle2PreviousOB.high;
      }
    }

    // Find the EXTREME swing after extremeCandle (candle1)
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
    
    // Check DOUBLE EQ status (without tolerance - pure logic only)
    const candle2NextStatus = nextSwing
      ? this._checkDoubleEq(extremeCandle, nextSwing, direction)
      : null;

    // Find V-shape and breakout for candle2Next
    let candle2NextVshape = null;
    let candle2NextBreakout = null;
    let candle2NextRetest = null;
    
    if (nextSwing) {
      const extremeCandleAsSwing = extremeCandleSwing;
      const nextSwingAsSwing = swings.find(s => s.index === nextSwing.index);
      
      if (nextSwingAsSwing) {
        candle2NextVshape = this.findVShapeCandle(
          extremeCandleAsSwing,
          nextSwingAsSwing,
          candles,
          direction
        );
        
        if (candle2NextVshape) {
          const vShapeLevel = direction === 'bullish' ? candle2NextVshape.high : candle2NextVshape.low;
          candle2NextBreakout = this.identifyBreakoutOfLevel(
            vShapeLevel,
            candles,
            nextSwingAsSwing.index,
            direction,
            nextSwingAsSwing
          );
          
          // Find retest for candle2Next
          if (candle2NextBreakout) {
            candle2NextRetest = this._findConfirmedSetupRetest(
              candle2NextVshape,
              nextSwingAsSwing,
              candle2NextBreakout,
              candles,
              direction
            );
          }
        }
      }
    }

    // Find Mitigation for candle2Next
    let candle2NextMitigation = null;
    if (nextSwing && candle2NextVshape) {
      candle2NextMitigation = this._findCandle2NextMitigation(
        candle2NextVshape.index,
        nextSwing.index,
        candles,
        direction
      );
    }

    // Compute candle2NextMitigationStatus using the new method that starts from the V-shape index
    let candle2NextMitigationStatus = null;
    if (candle2NextMitigation && candle2NextVshape) {
      candle2NextMitigationStatus = this._checkMitigationRetest(
        candle2NextMitigation,
        candle2NextVshape.index,
        candles,
        direction
      );
    }

    // Find Next OB - Start scan from nextSwing (candle2Next)
    let candle2NextOB = null;
    let candle2NextOBStatus = false;
    if (nextSwing && candle2NextVshape) {
      candle2NextOB = this._findOBForSetup(nextSwing.index, obMap, direction, candle2NextVshape, this.config.obScanRange);
      if (candle2NextOB && candle2NextRetest) {
        const retestPrice = direction === 'bullish' ? candle2NextRetest.low : candle2NextRetest.high;
        candle2NextOBStatus = retestPrice >= candle2NextOB.low && retestPrice <= candle2NextOB.high;
      }
    }

    // If no nextSwing found, return with just candle1
    if (!nextSwing) {
      this.logger.debug(`[PatternEngine] No next swing found after extreme candle`);
      return {
        type: 'confirmedSetup',
        direction,
        candle1: extremeCandle,
        candle2Previous: previousSwing ? candles[previousSwing.index] : null,
        candle2PreviousStatus,
        candle2PreviousVshape,
        candle2PreviousBreakout,
        candle2PreviousRetest,
        candle2PreviousMitigationIndex: candle2PreviousMitigation ? candle2PreviousMitigation.index : null,
        candle2PreviousMitigationFormattedTime: candle2PreviousMitigation ? this._formatTime(candle2PreviousMitigation.time) : null,
        candle2PreviousMitigationStatus,
        candle2Next: null,
        candle2NextStatus: null,
        candle2NextVshape: null,
        candle2NextBreakout: null,
        candle2NextRetest: null,
        candle2NextMitigationIndex: null,
        candle2NextMitigationFormattedTime: null,
        candle2NextMitigationStatus: null,
        candle2PreviousOBIndex: candle2PreviousOB ? candle2PreviousOB.index : null,
        candle2PreviousOBFormattedTime: candle2PreviousOB ? candle2PreviousOB.formattedTime : null,
        candle2PreviousOBStatus,
        candle2NextOBIndex: null,
        candle2NextOBFormattedTime: null,
        candle2NextOBStatus: false,
        level: direction === 'bullish' 
          ? extremeCandle.low 
          : extremeCandle.high,
        levelType: direction === 'bullish' ? 'support' : 'resistance'
      };
    }

    // Always return candle2Next if found (no tolerance check)
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
      candle2PreviousMitigationIndex: candle2PreviousMitigation ? candle2PreviousMitigation.index : null,
      candle2PreviousMitigationFormattedTime: candle2PreviousMitigation ? this._formatTime(candle2PreviousMitigation.time) : null,
      candle2PreviousMitigationStatus,
      candle2Next: nextSwingCandle,
      candle2NextStatus,
      candle2NextVshape,
      candle2NextBreakout,
      candle2NextRetest,
      candle2NextMitigationIndex: candle2NextMitigation ? candle2NextMitigation.index : null,
      candle2NextMitigationFormattedTime: candle2NextMitigation ? this._formatTime(candle2NextMitigation.time) : null,
      candle2NextMitigationStatus,
      candle2PreviousOBIndex: candle2PreviousOB ? candle2PreviousOB.index : null,
      candle2PreviousOBFormattedTime: candle2PreviousOB ? candle2PreviousOB.formattedTime : null,
      candle2PreviousOBStatus,
      candle2NextOBIndex: candle2NextOB ? candle2NextOB.index : null,
      candle2NextOBFormattedTime: candle2NextOB ? candle2NextOB.formattedTime : null,
      candle2NextOBStatus,
      level: direction === 'bullish' 
        ? Math.min(extremeCandle.low, nextSwingCandle.low)
        : Math.max(extremeCandle.high, nextSwingCandle.high),
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
    
    this.store[symbol][granularity] = patterns.filter(
      p => p.timestamp > cutoff
    );
  }
}

module.exports = new PatternEngine();