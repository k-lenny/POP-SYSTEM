// server/src/signals/dataProcessor/confirmedSetup.js
const setupEngine = require('./setup');
const signalEngine = require('../signalEngine');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');

class ConfirmedSetupEngine {
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
    const confirmedSetups = [];

    for (const setup of setups) {
      // A setup must have these values to be classifiable
      if (setup.setupVshapeDepth === null || setup.preBreakoutVDepth === null || setup.impulseExtremeDepth === null || !isFinite(setup.preBreakoutVDepth)) {
        continue;
      }

      const { status, isValid } = this._getSetupStatus(setup, candles, candleIndexMap);

      // If the setup status is invalid (e.g., a failed S-SETUP), skip it.
      if (!isValid) {
        continue;
      }
      
      // If no specific status was matched, it's not a "confirmed" setup type we're looking for.
      if (status === null) {
        continue;
      }

      const breakoutResult = this._getBreakoutStatus(setup, candles, candleIndexMap);

      confirmedSetups.push({
        ...setup,
        setupStatus: status,
        setupStatusIndex: setup.setupVshapeIndex,
        setupStatusFormattedTime: setup.setupVshapeFormattedTime,
        ConfirmedSetupBreakoutStatus: breakoutResult.status,
        ConfirmedSetupBreakoutStatusIndex: breakoutResult.index,
        ConfirmedSetupBreakoutStatusFormattedTime: breakoutResult.formattedTime,
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
}

module.exports = new ConfirmedSetupEngine();