// server/src/signals/dataProcessor/setup.js
const eqhEqlEngine = require('./eqhEql');
const signalEngine = require('../signalEngine');
const { getConfig } = require('../../config');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');

class SetupEngine {
  /**
   * Finds setups based on broken EQH/EQL levels.
   * A setup is a broken level followed by a "V-shape" pullback/retracement.
   * @param {string} symbol The symbol to check (e.g., 'BTCUSD').
   * @param {number} granularity The granularity in seconds (e.g., 3600).
   * @returns {Array<Object>} An array of setup objects.
   */
  getSetups(symbol, granularity) {
    const brokenLevels = eqhEqlEngine.getBroken(symbol, granularity);
    if (!brokenLevels.length) {
      return [];
    }

    const config = getConfig(symbol, granularity);
    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length) {
      return [];
    }

    const candleIndexMap = buildCandleIndexMap(candles);
    const setups = [];

    // How many candles to scan after a breakout to find the extreme.
    const scanCandles = config.MAX_SETUP_SCAN_CANDLES || 50;

    for (const level of brokenLevels) {
      // Find the array position of the candle that broke the level.
      const breakArrayPos = candleIndexMap.get(level.brokenIndex);

      let extremeCandle = null;

      if (breakArrayPos !== undefined) {
        const scanStart = breakArrayPos + 1;
        const scanEnd = Math.min(candles.length, scanStart + scanCandles);

        if (level.type === 'EQH') {
          // For a broken EQH (highs taken), the "setup V-shape" is the lowest low (pullback) after the breakout.
          let minLow = Infinity;
          for (let i = scanStart; i < scanEnd; i++) {
            const candle = candles[i];
            if (candle.low < minLow) {
              minLow = candle.low;
              extremeCandle = candle;
            }
          }
        } else { // EQL
          // For a broken EQL (lows taken), the "setup V-shape" is the highest high (retracement) after the breakout.
          let maxHigh = -Infinity;
          for (let i = scanStart; i < scanEnd; i++) {
            const candle = candles[i];
            if (candle.high > maxHigh) {
              maxHigh = candle.high;
              extremeCandle = candle;
            }
          }
        }
      }

      // Find the extreme impulse candle between the original V-shape and the post-breakout setup V-shape.
      let impulseExtremeCandle = null;
      const vShapeArrayPos = candleIndexMap.get(level.vShapeIndex);
      const setupVshapeArrayPos = extremeCandle ? candleIndexMap.get(extremeCandle.index) : undefined;
    
      if (vShapeArrayPos !== undefined && setupVshapeArrayPos !== undefined && vShapeArrayPos < setupVshapeArrayPos) {
        const impulseScanStart = vShapeArrayPos + 1;
        const impulseScanEnd = setupVshapeArrayPos;
    
        if (level.type === 'EQH') {
          // Between the original V-low and the setup V-low, find the highest high (the impulse).
          let maxHigh = -Infinity;
          for (let i = impulseScanStart; i < impulseScanEnd; i++) {
            const candle = candles[i];
    
            if (candle.high > maxHigh) {
              maxHigh = candle.high;
              impulseExtremeCandle = candle;
            }
          }
          
        } else { // EQL
          // Between the original V-high and the setup V-high, find the lowest low (the impulse).
          let minLow = Infinity;
          for (let i = impulseScanStart; i < impulseScanEnd; i++) {
            const candle = candles[i];
    
            if (candle.low < minLow) {
              minLow = candle.low;
              impulseExtremeCandle = candle;
            }
          }
        }
    
        // Post-verification: Ensure the found impulse extreme is more extreme than the setupVshapeDepth
        if (impulseExtremeCandle) {
          }
      
      }

      // Create a new setup object with all original level info + the new extreme info
      const setup = {
        ...level,
        setupVshapeDepth: extremeCandle ? (level.type === 'EQH' ? extremeCandle.low : extremeCandle.high) : null,
        setupVshapeTime: extremeCandle ? extremeCandle.time : null,
        setupVshapeFormattedTime: extremeCandle ? extremeCandle.formattedTime : null,
        setupVshapeIndex: extremeCandle ? extremeCandle.index : null,
        impulseExtremeDepth: impulseExtremeCandle ? (level.type === 'EQH' ? impulseExtremeCandle.high : impulseExtremeCandle.low) : null,
        impulseExtremeTime: impulseExtremeCandle ? impulseExtremeCandle.time : null,
        impulseExtremeFormattedTime: impulseExtremeCandle ? impulseExtremeCandle.formattedTime : null,
        impulseExtremeIndex: impulseExtremeCandle ? impulseExtremeCandle.index : null,
      };
      setups.push(setup);
    }

    return setups;
  }
}


module.exports = new SetupEngine();
