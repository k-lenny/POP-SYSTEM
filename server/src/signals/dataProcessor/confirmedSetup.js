// server/src/signals/dataProcessor/confirmedSetup.js
const setupEngine = require('./setup');
const signalEngine = require('../signalEngine');
const swingEngine = require('./swings');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');
const { processOBLV } = require('./OBLV');

class ConfirmedSetupEngine {
  constructor() {
    // Cache for locked setupStatus once breakout is confirmed (YES)
    // Key: `${symbol}_${granularity}_${brokenIndex}` → setupStatus string
    this._lockedStatuses = {};
  }

  _lockKey(symbol, granularity, setup) {
    return `${symbol}_${granularity}_${setup.brokenIndex}`;
  }

  /**
   * Takes setups and classifies them into confirmed patterns like OTE, DOUBLE EQ, or S-SETUP.
   * @param {string} symbol The symbol to check.
   * @param {number} granularity The granularity in seconds.
   * @returns {Array<Object>} An array of confirmed setup objects.
   */
  getConfirmedSetups(symbol, granularity) {
    const setups = setupEngine.getSetups(symbol, granularity);
    if (!setups.length) {
      return [];
    }

    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length) {
      return [];
    }

    const candleIndexMap = buildCandleIndexMap(candles);

    // Build formattedTime → candle map so we can resolve OB candle indexes
    const ftMap = new Map();
    for (const c of candles) ftMap.set(c.formattedTime, c);

    const oblvData = processOBLV(symbol, granularity, candles);

    const confirmedSetups = [];

    for (const setup of setups) {
      // A setup must have these values to be classifiable
      if (setup.setupVshapeDepth === null || setup.preBreakoutVDepth === null || setup.impulseExtremeDepth === null || !isFinite(setup.preBreakoutVDepth)) {
        continue;
      }

      const lockKey = this._lockKey(symbol, granularity, setup);
      const lockedStatus = this._lockedStatuses[lockKey];

      let status;
      let isValid;

      if (lockedStatus) {
        // Breakout was previously confirmed — setupStatus is locked
        status = lockedStatus;
        isValid = true;
      } else {
        const result = this._getSetupStatus(setup, candles, candleIndexMap);
        status = result.status;
        isValid = result.isValid;
      }

      // If the setup status is invalid (e.g., a failed S-SETUP), skip it.
      if (!isValid) {
        continue;
      }

      // If no specific status was matched, it's not a "confirmed" setup type we're looking for.
      if (status === null) {
        continue;
      }

      const breakoutResult = this._getBreakoutStatus(setup, candles, candleIndexMap);

      // Lock the setupStatus once breakout is confirmed
      if (breakoutResult.status === 'YES' && !lockedStatus) {
        this._lockedStatuses[lockKey] = status;
      }

      const setupOB = this._findSetupOB(
        oblvData,
        ftMap,
        setup.setupVshapeIndex,
        breakoutResult.index
      );

      const OBSwing = breakoutResult.status === 'YES' && setupOB
        ? this._findOBSwing(symbol, granularity, setup.type, setupOB, breakoutResult.index, candleIndexMap, candles)
        : null;

      confirmedSetups.push({
        ...setup,
        setupStatus: status,
        setupStatusIndex: setup.setupVshapeIndex,
        setupStatusFormattedTime: setup.setupVshapeFormattedTime,
        ConfirmedSetupBreakoutStatus: breakoutResult.status,
        ConfirmedSetupBreakoutStatusIndex: breakoutResult.index,
        ConfirmedSetupBreakoutStatusFormattedTime: breakoutResult.formattedTime,
        setupOB,
        OBSwing,
      });
    }

    return confirmedSetups;
  }

  /**
   * Determines the classification (OTE, DOUBLE EQ, S SETUP) of a setup.
   * @private
   */
  _getSetupStatus(setup, candles, candleIndexMap) {
    const {
      type,
      setupVshapeDepth,
      preBreakoutVDepth,
      impulseExtremeDepth,
      preBreakoutVIndex,
      setupVshapeIndex,
    } = setup;

    // --- OTE Check ---
    const impulseRange = Math.abs(preBreakoutVDepth - impulseExtremeDepth);
    if (impulseRange > 0) {
      const oteLowerBound = type === 'EQL' ? impulseExtremeDepth + (impulseRange * 0.625) : impulseExtremeDepth - (impulseRange * 0.79);
      const oteUpperBound = type === 'EQL' ? impulseExtremeDepth + (impulseRange * 0.79) : impulseExtremeDepth - (impulseRange * 0.625);

      if (type === 'EQL' && setupVshapeDepth >= oteLowerBound && setupVshapeDepth <= oteUpperBound) {
        return { status: 'OTE', isValid: true };
      }
      if (type === 'EQH' && setupVshapeDepth <= oteUpperBound && setupVshapeDepth >= oteLowerBound) {
        return { status: 'OTE', isValid: true };
      }
    }

    // --- DOUBLE EQ Check ---
    const preBreakoutVPos = candleIndexMap.get(preBreakoutVIndex);
    if (preBreakoutVPos !== undefined) {
      const preBreakoutVCandle = candles[preBreakoutVPos];
      if (type === 'EQL') {
        const preBreakoutVKeyPrice = Math.max(preBreakoutVCandle.open, preBreakoutVCandle.close);
        if (setupVshapeDepth < preBreakoutVDepth && setupVshapeDepth > preBreakoutVKeyPrice) {
          return { status: 'DOUBLE EQ', isValid: true };
        }
      } else { // EQH
        const preBreakoutVKeyPrice = Math.min(preBreakoutVCandle.open, preBreakoutVCandle.close);
        if (setupVshapeDepth > preBreakoutVDepth && setupVshapeDepth < preBreakoutVKeyPrice) {
          return { status: 'DOUBLE EQ', isValid: true };
        }
      }
    }

    // --- S SETUP (Sweep) Check ---
    const setupVPos = candleIndexMap.get(setupVshapeIndex);
    if (setupVPos !== undefined) {
      const setupVCandle = candles[setupVPos];
      const isSweep = (type === 'EQL' && setupVshapeDepth > preBreakoutVDepth) || (type === 'EQH' && setupVshapeDepth < preBreakoutVDepth);

      if (isSweep) {
        const closedBackInside = (type === 'EQL' && setupVCandle.close < preBreakoutVDepth) || (type === 'EQH' && setupVCandle.close > preBreakoutVDepth);
        if (closedBackInside) {
          const nextCandlePos = setupVPos + 1;
          if (nextCandlePos < candles.length) {
            const nextCandle = candles[nextCandlePos];
            let isInvalidated = false;
            if (type === 'EQL' && Math.max(nextCandle.open, nextCandle.close) > Math.max(setupVCandle.open, setupVCandle.close)) {
              isInvalidated = true;
            } else if (type === 'EQH' && Math.min(nextCandle.open, nextCandle.close) < Math.min(setupVCandle.open, setupVCandle.close)) {
              isInvalidated = true;
            }
            if (isInvalidated) return { status: 'S SETUP FAILED', isValid: false };
          }
          return { status: 'S SETUP', isValid: true };
        } else {
          return { status: 'S SETUP FAILED', isValid: false };
        }
      }
    }

    return { status: null, isValid: true }; // No specific status matched, but not explicitly invalid.
  }

  /**
   * Checks if the price has broken the impulse extreme after the setup formed.
   * Uses strict validation: first candle that crosses by body confirms breakout,
   * but if crossed by wick only, subsequent body crosses must exceed all previous crossing candles' extremes.
   * @private
   */
  _getBreakoutStatus(setup, candles, candleIndexMap) {
    const startScanPos = nextArrayIdx(candleIndexMap, candles, setup.setupVshapeIndex);
    if (startScanPos === undefined) {
      return { status: 'NO', index: null, formattedTime: null };
    }

    const isEQH = setup.type === 'EQH';
    const impulseExtreme = setup.impulseExtremeDepth;
    const crossingCandles = []; // Track all candles that crossed the impulse extreme

    for (let i = startScanPos; i < candles.length; i++) {
      const candle = candles[i];
      
      // Check if this candle crosses the impulse extreme
      const crossesByWickOrBody = isEQH 
        ? candle.high > impulseExtreme 
        : candle.low < impulseExtreme;
      
      if (!crossesByWickOrBody) {
        continue; // This candle doesn't cross, skip it
      }

      // Check if it crosses by body (close is beyond the impulse extreme)
      const crossesByBody = isEQH 
        ? candle.close > impulseExtreme 
        : candle.close < impulseExtreme;

      if (crossesByBody) {
        // Body cross detected
        if (crossingCandles.length === 0) {
          // First candle that crossed - if it's by body, immediate breakout
          return { status: 'YES', index: candle.index, formattedTime: candle.formattedTime };
        } else {
          // Not the first crossing candle - must exceed all previous crossing candles' extremes
          let exceedsAllPrevious = true;
          
          for (const prevCandle of crossingCandles) {
            if (isEQH) {
              // For EQH: current close must be above all previous crossing candles' highs
              if (candle.close <= prevCandle.high) {
                exceedsAllPrevious = false;
                break;
              }
            } else {
              // For EQL: current close must be below all previous crossing candles' lows
              if (candle.close >= prevCandle.low) {
                exceedsAllPrevious = false;
                break;
              }
            }
          }

          if (exceedsAllPrevious) {
            // Valid breakout - close exceeded all previous crossing candles' extremes
            return { status: 'YES', index: candle.index, formattedTime: candle.formattedTime };
          }
        }
      }

      // Track this crossing candle (whether by wick or body)
      crossingCandles.push(candle);
    }

    return { status: 'NO', index: null, formattedTime: null };
  }

  /**
   * Finds the first OB (from OBLV data) whose candle index falls strictly
   * between setupStatusIndex and breakoutStatusIndex (end of candles if no breakout).
   * @private
   */
  _findSetupOB(oblvData, ftMap, setupStatusIndex, breakoutStatusIndex) {
    for (const oblv of oblvData) {
      if (!oblv.OB || !oblv.OBFormattedTime) continue;

      const obCandle = ftMap.get(oblv.OBFormattedTime);
      if (!obCandle) continue;

      const obIdx = obCandle.index;

      // The setupStatusIndex candle itself is allowed to be the OB
      if (obIdx < setupStatusIndex) continue;

      // Must be strictly before breakout (if one exists)
      if (breakoutStatusIndex !== null && obIdx >= breakoutStatusIndex) continue;

      return {
        index: obCandle.index,
        formattedTime: obCandle.formattedTime,
        open: oblv.OB.open,
        high: oblv.OB.high,
        low: oblv.OB.low,
        close: oblv.OB.close,
      };
    }
    return null;
  }

  /**
   * Finds the OBSwing — the extreme swing found between the setupOB's high and low.
   * Only called after breakout is confirmed (YES).
   * For EQL: looks for swing highs whose price falls between setupOB.low and setupOB.high,
   *          returns the one with the highest candle high (extreme high).
   * For EQH: looks for swing lows whose price falls between setupOB.low and setupOB.high,
   *          returns the one with the lowest candle low (extreme low).
   * @private
   */
  _findOBSwing(symbol, granularity, type, setupOB, breakoutIndex, candleIndexMap, candles) {
    const swings = swingEngine.get(symbol, granularity);
    if (!swings.length) return null;

    const obHigh = setupOB.high;
    const obLow = setupOB.low;
    const isEQL = type === 'EQL';

    // Find the first candle after breakout that crosses the setupOB
    // For EQL: first candle whose high crosses above obHigh
    // For EQH: first candle whose low crosses below obLow
    const startPos = nextArrayIdx(candleIndexMap, candles, breakoutIndex);
    let obCrossIndex = null;
    if (startPos !== undefined) {
      for (let i = startPos; i < candles.length; i++) {
        const c = candles[i];
        if (isEQL && c.high > obHigh) {
          obCrossIndex = c.index;
          break;
        }
        if (!isEQL && c.low < obLow) {
          obCrossIndex = c.index;
          break;
        }
      }
    }

    // OB must have been crossed; if not yet crossed, no OBSwing
    if (obCrossIndex === null) return null;

    // Filter swings: behind (before) the first OB-crossing candle, correct type, price within OB range
    const relevantSwings = swings.filter(s => {
      if (isEQL && s.type !== 'high') return false;
      if (!isEQL && s.type !== 'low') return false;

      // Swing price must be between setupOB low and high
      if (s.price < obLow || s.price > obHigh) return false;

      // Must be behind (before) the first candle that crossed the OB
      if (s.candleIndex >= obCrossIndex) return false;

      return true;
    });

    if (!relevantSwings.length) return null;

    // Find the extreme swing
    let extremeSwing;
    if (isEQL) {
      // For EQL: highest candle high
      extremeSwing = relevantSwings.reduce((best, s) => s.high > best.high ? s : best);
    } else {
      // For EQH: lowest candle low
      extremeSwing = relevantSwings.reduce((best, s) => s.low < best.low ? s : best);
    }

    return {
      index: extremeSwing.candleIndex,
      price: extremeSwing.price,
      data: {
        open: extremeSwing.open,
        high: extremeSwing.high,
        low: extremeSwing.low,
        close: extremeSwing.close,
        formattedTime: extremeSwing.formattedTime,
        type: extremeSwing.type,
        direction: extremeSwing.direction,
      },
    };
  }
}

module.exports = new ConfirmedSetupEngine();