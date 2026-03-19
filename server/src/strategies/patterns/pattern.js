const EventEmitter = require('events');
const swingEngine = require('../../signals/dataProcessor/swings');
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
    const date = new Date(timestamp * 1000);
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
   * Main detection method with detailed metadata
   */
  async detect(symbol, granularity, candles) {
    this._initStore(symbol, granularity);

    // Ensure swings are detected before proceeding
    await swingEngine.detectAll(symbol, granularity, candles.slice(0, -1), 1);

    const swings = swingEngine.get(symbol, granularity);
    
    // Debug logging
    this.logger.info(`[PatternEngine] Detecting patterns for ${symbol} ${granularity}`);
    this.logger.info(`[PatternEngine] Candles: ${candles.length}, Swings: ${swings.length}`);

    if (swings.length < 2) {
      this.logger.warn(`[PatternEngine] Not enough swings (${swings.length}) for pattern detection`);
      return [];
    }

    const enrichedCandles = this._enrichCandles(candles);
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
        direction
      );
      
      if (setup) {
        // Build the full pattern with all stages
        this._buildPatternStages(setup, enrichedCandles, direction);
        
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
      
      // Stage 3: Confirmed Setup
      confirmedSetupCandle1Index: pattern.confirmedSetup?.candle1?.index || null,
      confirmedSetupCandle1Price: pattern.confirmedSetup?.candle1?.close || null,
      confirmedSetupCandle1Time: pattern.confirmedSetup?.candle1?.time|| null,
      confirmedSetupCandle1FormattedTime: this._formatTime(pattern.confirmedSetup?.candle1?.time),
      
      confirmedSetupCandle2Index: pattern.confirmedSetup?.candle2?.index || null,
      confirmedSetupCandle2Price: pattern.confirmedSetup?.candle2?.close || null,
      confirmedSetupCandle2Time: pattern.confirmedSetup?.candle2?.time|| null,
      confirmedSetupCandle2FormattedTime: this._formatTime(pattern.confirmedSetup?.candle2?.time),
      
      confirmedSetupLevel: pattern.confirmedSetup?.level || null,
      confirmedSetupLevelType: pattern.confirmedSetup?.levelType || null,
      
      // Stage 4: Confirmed Setup Breakout
      confirmedSetupBreakoutIndex: pattern.confirmedSetup?.breakout?.index || null,
      confirmedSetupBreakoutPrice: pattern.confirmedSetup?.breakout?.close || null,
      confirmedSetupBreakoutTime: pattern.confirmedSetup?.breakout?.time || null,
      confirmedSetupBreakoutFormattedTime: this._formatTime(pattern.confirmedSetup?.breakout?.time),
      confirmedSetupBreakoutType: pattern.confirmedSetup?.breakout?.breakoutType || null,
      
      // Stage 5: Confirmed Setup Retest
      confirmedSetupRetestIndex: pattern.confirmedSetup?.retest?.index || null,
      confirmedSetupRetestPrice: pattern.confirmedSetup?.retest?.close || null,
      confirmedSetupRetestTime: pattern.confirmedSetup?.retest?.time || null,
      confirmedSetupRetestFormattedTime: this._formatTime(pattern.confirmedSetup?.retest?.time),
      
      // Stage 6: Second Confirmed Setup
      secondConfirmedSetupCandle1Index: pattern.confirmedSetup?.secondConfirmedSetup?.candle1?.index || null,
      secondConfirmedSetupCandle1Price: pattern.confirmedSetup?.secondConfirmedSetup?.candle1?.close || null,
      secondConfirmedSetupCandle1Time: pattern.confirmedSetup?.secondConfirmedSetup?.candle1?.time || null,
      secondConfirmedSetupCandle1FormattedTime: this._formatTime(pattern.confirmedSetup?.secondConfirmedSetup?.candle1?.time),
      
      secondConfirmedSetupCandle2Index: pattern.confirmedSetup?.secondConfirmedSetup?.candle2?.index || null,
      secondConfirmedSetupCandle2Price: pattern.confirmedSetup?.secondConfirmedSetup?.candle2?.close || null,
      secondConfirmedSetupCandle2Time: pattern.confirmedSetup?.secondConfirmedSetup?.candle2?.time || null,
      secondConfirmedSetupCandle2FormattedTime: this._formatTime(pattern.confirmedSetup?.secondConfirmedSetup?.candle2?.time),
      
      secondConfirmedSetupLevel: pattern.confirmedSetup?.secondConfirmedSetup?.level || null,
      
      // Stage 7: Final Breakout
      finalBreakoutIndex: pattern.confirmedSetup?.secondConfirmedSetup?.breakout?.index || null,
      finalBreakoutPrice: pattern.confirmedSetup?.secondConfirmedSetup?.breakout?.close || null,
      finalBreakoutTime: pattern.confirmedSetup?.secondConfirmedSetup?.breakout?.time || null,
      finalBreakoutFormattedTime: this._formatTime(pattern.confirmedSetup?.secondConfirmedSetup?.breakout?.time),
      
      // Pattern completion status
      stage: this._getPatternStage(pattern),
      isComplete: !!(pattern.confirmedSetup?.secondConfirmedSetup?.breakout),
      
      // Timestamps for sorting/filtering
      formattedTime: this._formatTime(pattern.currentSwing?.time),
      time: pattern.currentSwing?.time || null,
      timestamp: pattern.timestamp || Date.now(),
    };

    return result;
  }

  /**
   * Determine which stage the pattern has reached
   */
  _getPatternStage(pattern) {
    if (pattern.confirmedSetup?.secondConfirmedSetup?.breakout) return 7;
    if (pattern.confirmedSetup?.secondConfirmedSetup) return 6;
    if (pattern.confirmedSetup?.retest) return 5;
    if (pattern.confirmedSetup?.breakout) return 4;
    if (pattern.confirmedSetup) return 3;
    if (pattern.retest) return 2;
    return 1;
  }

  /**
   * Recursively builds all pattern stages
   */
  _buildPatternStages(setup, candles, direction) {
    // Stage 2: Retest
    const retest = this.identifyRetest(setup, candles, direction);
    if (!retest) return;
    setup.retest = retest;

    // Stage 3: Confirmed Setup
    const confirmedSetup = this.identifyConfirmedSetup(retest, candles, direction);
    if (!confirmedSetup) return;
    setup.confirmedSetup = confirmedSetup;

    // Stage 4: Breakout of Confirmed Setup
    const confirmedSetupBreakout = this.identifyBreakout(confirmedSetup, candles, direction);
    if (!confirmedSetupBreakout) return;
    confirmedSetup.breakout = confirmedSetupBreakout;

    // Stage 5: Retest of Confirmed Setup
    const confirmedSetupRetest = this.identifyRetest(
      { ...confirmedSetup, breakout: confirmedSetupBreakout },
      candles,
      direction
    );
    if (!confirmedSetupRetest) return;
    confirmedSetup.retest = confirmedSetupRetest;

    // Stage 6: Second Confirmed Setup
    const secondConfirmedSetup = this.identifyConfirmedSetup(
      confirmedSetupRetest,
      candles,
      direction
    );
    if (!secondConfirmedSetup) return;
    confirmedSetup.secondConfirmedSetup = secondConfirmedSetup;

    // Stage 7: Final Breakout
    const finalBreakout = this.identifyBreakout(secondConfirmedSetup, candles, direction);
    if (finalBreakout) {
      secondConfirmedSetup.breakout = finalBreakout;
    }
  }

  identifySetup(currentSwing, previousSwing, candles, direction) {
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
    // Find first candle that CLOSES below previousSwing.low
    let firstSweepCandle = null;
    
    for (let i = startIndex; i <= endIndex; i++) {
      const candle = candles[i];
      if (candle.close < previousSwing.low) {
        firstSweepCandle = candle;
        break;
      }
    }

    if (!firstSweepCandle) {
      return result; // No sweep occurred
    }

    // REMOVED THE RESTRICTIVE CHECK - price can continue lower
    // That's actually expected as liquidity gets grabbed
    
    // Valid sweep
    result.isSweep = true;
    result.firstSweepCandleIndex = firstSweepCandle.index;
    result.firstSweepCandleClose = firstSweepCandle.close;

  } else {
    // Bearish: Both are HIGHs
    // Find first candle that CLOSES above previousSwing.high
    let firstSweepCandle = null;
    
    for (let i = startIndex; i <= endIndex; i++) {
      const candle = candles[i];
      if (candle.close > previousSwing.high) {
        firstSweepCandle = candle;
        break;
      }
    }

    if (!firstSweepCandle) {
      return result; // No sweep occurred
    }

    // REMOVED THE RESTRICTIVE CHECK - price can continue higher
    // That's actually expected as liquidity gets grabbed
    
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
        // Breakout happens when price closes above level
        if (candle.close > level) {
          return candle;
        }
      } else { // bearish
        // Invalid if price wick crossed above currentSwing high before breakout
        if (candle.high > currentSwing.high) {
          return null; // Invalid breakout
        }
        // Breakout happens when price closes below level
        if (candle.close < level) {
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

    let extremumCandle = null;

    if (direction === 'bullish') {
      let minLow = Infinity;
      for (let i = startIndex; i < candles.length; i++) {
        const candle = candles[i];
        if (candle.low < minLow) {
          minLow = candle.low;
          extremumCandle = candle;
        }
      }
    } else { // bearish
      let maxHigh = -Infinity;
      for (let i = startIndex; i < candles.length; i++) {
        const candle = candles[i];
        if (candle.high > maxHigh) {
          maxHigh = candle.high;
          extremumCandle = candle;
        }
      }
    }

    if (extremumCandle) {
        return {
            ...extremumCandle,
            retestLevel: direction === 'bullish' ? extremumCandle.low : extremumCandle.high,
            retestType: direction === 'bullish' ? 'support' : 'resistance'
        };
    }

    return null;
  }

  identifyConfirmedSetup(retest, candles, direction) {
    const startIndex = retest.index + 1;
    
    for (let i = startIndex; i < candles.length - 1; i++) {
      const candle1 = candles[i];
      const candle2 = candles[i + 1];

      if (direction === 'bullish') {
        const lowDifference = Math.abs(candle1.low - candle2.low);
        const tolerance = candle1.low * this.config.equalLevelTolerance;
        
        if (lowDifference <= tolerance) {
          return {
            type: 'confirmedSetup',
            direction,
            candle1,
            candle2,
            level: Math.min(candle1.low, candle2.low),
            levelType: 'support'
          };
        }
      } else {
        const highDifference = Math.abs(candle1.high - candle2.high);
        const tolerance = candle1.high * this.config.equalLevelTolerance;
        
        if (highDifference <= tolerance) {
          return {
            type: 'confirmedSetup',
            direction,
            candle1,
            candle2,
            level: Math.max(candle1.high, candle2.high),
            levelType: 'resistance'
          };
        }
      }
    }
    return null;
  }

  identifyBreakout(setup, candles, direction) {
    if (!setup.candle1 || !setup.candle2) return null;
    
    const level = direction === 'bullish'
      ? Math.min(setup.candle1.low, setup.candle2.low)
      : Math.max(setup.candle1.high, setup.candle2.high);
    
    const startIndex = setup.candle2.index + 1;

    for (let i = startIndex; i < candles.length; i++) {
      const candle = candles[i];
      
      if (direction === 'bullish') {
        if (candle.close > level) {
          return {
            ...candle,
            breakoutLevel: level,
            breakoutType: 'bullish'
          };
        }
      } else {
        if (candle.close < level) {
          return {
            ...candle,
            breakoutLevel: level,
            breakoutType: 'bearish'
          };
        }
      }
    }
    return null;
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
        stage4: patterns.filter(p => p.stage === 4).length,
        stage5: patterns.filter(p => p.stage === 5).length,
        stage6: patterns.filter(p => p.stage === 6).length,
        stage7: patterns.filter(p => p.stage === 7).length,
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