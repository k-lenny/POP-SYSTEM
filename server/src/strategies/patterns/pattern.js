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
   * Main detection method with detailed metadata
   */
  async detect(symbol, granularity, candles) {
    this._initStore(symbol, granularity);

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

    for (let i = 1; i < swings.length; i++) {
      const currentSwing = swings[i];
      const previousSwing = swings[i - 1];

      // Only process SAME swing types (sweep pattern requirement)
      // A HIGH can only sweep a previous HIGH
      // A LOW can only sweep a previous LOW
      if (currentSwing.type !== previousSwing.type) {
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
        
        this.logger.info(`[PatternEngine] Found ${direction} pattern at index ${currentSwing.index}`);
        
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
      previousSwingTime: pattern.previousSwing?.timestamp || null,
      previousSwingFormattedTime: this._formatTime(pattern.previousSwing?.time),
      
      currentSwingIndex: pattern.currentSwing?.index || null,
      currentSwingPrice: pattern.currentSwing?.type === 'high'
        ? pattern.currentSwing?.high
        : pattern.currentSwing?.low,
      currentSwingType: pattern.currentSwing?.type || null,
      currentSwingTime: pattern.currentSwing?.timestamp || null,
      currentSwingFormattedTime: this._formatTime(pattern.currentSwing?.time),
      
      // Sweep data
      sweepDistance: pattern.sweepData?.sweepDistance || null,
      
      // V-shape candle
      vShapeCandleIndex: pattern.vShapeCandle?.index || null,
      vShapeCandlePrice: pattern.direction === 'bullish' 
        ? pattern.vShapeCandle?.low 
        : pattern.vShapeCandle?.high,
      vShapeCandleTime: pattern.vShapeCandle?.timestamp || null,
      vShapeCandleFormattedTime: this._formatTime(pattern.vShapeCandle?.time),
      vShapeWickSize: pattern.direction === 'bullish'
        ? pattern.vShapeCandle?.lowerWick
        : pattern.vShapeCandle?.upperWick,
      
      // Initial breakout
      breakoutIndex: pattern.breakout?.index || null,
      breakoutPrice: pattern.breakout?.close || null,
      breakoutTime: pattern.breakout?.timestamp || null,
      breakoutFormattedTime: this._formatTime(pattern.breakout?.time),
      
      // Stage 2: Retest
      retestIndex: pattern.retest?.index || null,
      retestPrice: pattern.retest?.close || null,
      retestTime: pattern.retest?.timestamp || null,
      retestFormattedTime: this._formatTime(pattern.retest?.time),
      retestLevel: pattern.retest?.retestLevel || null,
      retestType: pattern.retest?.retestType || null,
      
      // Stage 3: Confirmed Setup
      confirmedSetupCandle1Index: pattern.confirmedSetup?.candle1?.index || null,
      confirmedSetupCandle1Price: pattern.confirmedSetup?.candle1?.close || null,
      confirmedSetupCandle1Time: pattern.confirmedSetup?.candle1?.timestamp || null,
      confirmedSetupCandle1FormattedTime: this._formatTime(pattern.confirmedSetup?.candle1?.time),
      
      confirmedSetupCandle2Index: pattern.confirmedSetup?.candle2?.index || null,
      confirmedSetupCandle2Price: pattern.confirmedSetup?.candle2?.close || null,
      confirmedSetupCandle2Time: pattern.confirmedSetup?.candle2?.timestamp || null,
      confirmedSetupCandle2FormattedTime: this._formatTime(pattern.confirmedSetup?.candle2?.time),
      
      confirmedSetupLevel: pattern.confirmedSetup?.level || null,
      confirmedSetupLevelType: pattern.confirmedSetup?.levelType || null,
      
      // Stage 4: Confirmed Setup Breakout
      confirmedSetupBreakoutIndex: pattern.confirmedSetup?.breakout?.index || null,
      confirmedSetupBreakoutPrice: pattern.confirmedSetup?.breakout?.close || null,
      confirmedSetupBreakoutTime: pattern.confirmedSetup?.breakout?.timestamp || null,
      confirmedSetupBreakoutFormattedTime: this._formatTime(pattern.confirmedSetup?.breakout?.time),
      confirmedSetupBreakoutType: pattern.confirmedSetup?.breakout?.breakoutType || null,
      
      // Stage 5: Confirmed Setup Retest
      confirmedSetupRetestIndex: pattern.confirmedSetup?.retest?.index || null,
      confirmedSetupRetestPrice: pattern.confirmedSetup?.retest?.close || null,
      confirmedSetupRetestTime: pattern.confirmedSetup?.retest?.timestamp || null,
      confirmedSetupRetestFormattedTime: this._formatTime(pattern.confirmedSetup?.retest?.time),
      
      // Stage 6: Second Confirmed Setup
      secondConfirmedSetupCandle1Index: pattern.confirmedSetup?.secondConfirmedSetup?.candle1?.index || null,
      secondConfirmedSetupCandle1Price: pattern.confirmedSetup?.secondConfirmedSetup?.candle1?.close || null,
      secondConfirmedSetupCandle1Time: pattern.confirmedSetup?.secondConfirmedSetup?.candle1?.timestamp || null,
      secondConfirmedSetupCandle1FormattedTime: this._formatTime(pattern.confirmedSetup?.secondConfirmedSetup?.candle1?.time),
      
      secondConfirmedSetupCandle2Index: pattern.confirmedSetup?.secondConfirmedSetup?.candle2?.index || null,
      secondConfirmedSetupCandle2Price: pattern.confirmedSetup?.secondConfirmedSetup?.candle2?.close || null,
      secondConfirmedSetupCandle2Time: pattern.confirmedSetup?.secondConfirmedSetup?.candle2?.timestamp || null,
      secondConfirmedSetupCandle2FormattedTime: this._formatTime(pattern.confirmedSetup?.secondConfirmedSetup?.candle2?.time),
      
      secondConfirmedSetupLevel: pattern.confirmedSetup?.secondConfirmedSetup?.level || null,
      
      // Stage 7: Final Breakout
      finalBreakoutIndex: pattern.confirmedSetup?.secondConfirmedSetup?.breakout?.index || null,
      finalBreakoutPrice: pattern.confirmedSetup?.secondConfirmedSetup?.breakout?.close || null,
      finalBreakoutTime: pattern.confirmedSetup?.secondConfirmedSetup?.breakout?.timestamp || null,
      finalBreakoutFormattedTime: this._formatTime(pattern.confirmedSetup?.secondConfirmedSetup?.breakout?.time),
      
      // Pattern completion status
      stage: this._getPatternStage(pattern),
      isComplete: !!(pattern.confirmedSetup?.secondConfirmedSetup?.breakout),
      
      // Timestamps for sorting/filtering
      formattedTime: this._formatTime(pattern.currentSwing?.time),
      time: pattern.currentSwing?.timestamp || null,
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
    const sweepData = this.isWickSweep(currentSwing, previousSwing, direction);
    if (!sweepData.isSweep) {
      return null;
    }

    const vShapeCandle = this.findVShapeCandle(previousSwing, currentSwing, candles, direction);
    if (!vShapeCandle) {
      return null;
    }

    const breakoutLevel = direction === 'bullish' ? vShapeCandle.low : vShapeCandle.high;
    const breakout = this.identifyBreakoutOfLevel(
      breakoutLevel,
      candles,
      currentSwing.index,
      direction
    );
    
    if (!breakout) {
      return null;
    }

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

  isWickSweep(currentSwing, previousSwing, direction) {
    const result = {
      isSweep: false,
      sweepDistance: 0
    };

    // Both swings must be the SAME type
    if (currentSwing.type !== previousSwing.type) {
      return result;
    }

    if (direction === 'bullish') {
      // Bullish: Both are LOWs
      // Current LOW's wick goes below previous LOW's wick
      if (currentSwing.type === 'low') {
        const sweepDistance = previousSwing.low - currentSwing.low;
        
        if (sweepDistance > 0) {
          result.isSweep = true;
          result.sweepDistance = sweepDistance;
        }
      }
    } else {
      // Bearish: Both are HIGHs  
      // Current HIGH's wick goes above previous HIGH's wick
      if (currentSwing.type === 'high') {
        const sweepDistance = currentSwing.high - previousSwing.high;
        
        if (sweepDistance > 0) {
          result.isSweep = true;
          result.sweepDistance = sweepDistance;
        }
      }
    }

    return result;
  }

  findVShapeCandle(swing1, swing2, candles, direction) {
    const startIndex = Math.min(swing1.index, swing2.index);
    const endIndex = Math.max(swing1.index, swing2.index);

    if (endIndex - startIndex < this.config.minCandlesBetweenSwings) {
      return null;
    }

    let bestCandle = null;
    let maxWickRatio = 0;

    for (let i = startIndex + 1; i < endIndex; i++) {
      const candle = candles[i];
      
      if (direction === 'bullish') {
        const wickRatio = candle.bodySize > 0 ? candle.lowerWick / candle.bodySize : 0;
        
        if (wickRatio >= this.config.vShapeWickRatio && wickRatio > maxWickRatio) {
          bestCandle = candle;
          maxWickRatio = wickRatio;
        }
      } else {
        const wickRatio = candle.bodySize > 0 ? candle.upperWick / candle.bodySize : 0;
        
        if (wickRatio >= this.config.vShapeWickRatio && wickRatio > maxWickRatio) {
          bestCandle = candle;
          maxWickRatio = wickRatio;
        }
      }
    }

    return bestCandle;
  }

  identifyBreakoutOfLevel(level, candles, startIndex, direction) {
    for (let i = startIndex; i < candles.length; i++) {
      const candle = candles[i];
      
      if (direction === 'bullish') {
        if (candle.close < level) {
          return candle;
        }
      } else {
        if (candle.close > level) {
          return candle;
        }
      }
    }
    return null;
  }

  identifyRetest(setup, candles, direction) {
    const level = direction === 'bullish' 
      ? setup.vShapeCandle?.low 
      : setup.vShapeCandle?.high;
    
    if (!level) return null;
    
    const startIndex = setup.breakout.index + 1;

    for (let i = startIndex; i < candles.length; i++) {
      const candle = candles[i];
      
      const touchesLevel = candle.low <= level && candle.high >= level;
      
      if (touchesLevel) {
        return {
          ...candle,
          retestLevel: level,
          retestType: direction === 'bullish' ? 'support' : 'resistance'
        };
      }
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