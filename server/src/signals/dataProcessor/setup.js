// server/src/signals/dataProcessor/setup.js
const eqhEqlEngine = require('./eqhEql');
const signalEngine = require('../signalEngine');
const { getConfig } = require('../../config');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');

class SetupEngine {
  /**
   * Finds setups based on broken EQH/EQL levels.
   * A setup is a broken level followed by a "V-shape" pullback/retracement.
   * This requires a "sustained" breakout before scanning for the V-shape.
   * @param {string} symbol The symbol to check (e.g., 'BTCUSD').
   * @param {number} granularity The granularity in seconds (e.g., 3600).
   * @returns {Array<Object>} An array of setup objects.
   */
  getSetups(symbol, granularity) {
    const brokenLevels = eqhEqlEngine.getBroken(symbol, granularity)
      .filter(l => l.validityStatus !== 'INVALID');
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

    const scanCandles = config.MAX_SETUP_SCAN_CANDLES || 1000;
    const bosScanLimit = config.MAX_BOS_SCAN_CANDLES || 10;

    for (const level of brokenLevels) {
      // 1. Find the initial breaking candle.
      const breakArrayPos = candleIndexMap.get(level.brokenIndex);
      if (breakArrayPos === undefined) {
        continue;
      }
      const breakingCandle = candles[breakArrayPos];

      // 2. Find the second, "confirming" candle based on the new "BOS Sustained" rule.
      // This candle must close past the HIGH (for EQH) or LOW (for EQL) of the initial breaking candle.
      let confirmingCandle = null;
      let confirmingCandlePos = -1;
      const confirmationScanStart = breakArrayPos + 1;
      const confirmationScanEnd = Math.min(candles.length, confirmationScanStart + bosScanLimit);

      for (let i = confirmationScanStart; i < confirmationScanEnd; i++) {
        const c = candles[i];
        if (level.type === 'EQH' && c.close > breakingCandle.high) {
          confirmingCandle = c;
          confirmingCandlePos = i;
          break; // Found it
        } else if (level.type === 'EQL' && c.close < breakingCandle.low) {
          confirmingCandle = c;
          confirmingCandlePos = i;
          break; // Found it
        }
      }

      // If no confirming candle is found, this is not a valid setup under the new rules. Skip it.
      if (!confirmingCandle) {
        continue;
      }

      // 3. Scan for the setup V-shape, starting AFTER the confirming candle.
      let extremeCandle = null;
      const vShapeScanStart = confirmingCandlePos + 1;
      const vShapeScanEnd = Math.min(candles.length, vShapeScanStart + scanCandles);

      if (level.type === 'EQH') {
        // For a broken EQH, the "setup V-shape" is the lowest low (pullback) after the sustained break.
        let minLow = Infinity;
        for (let i = vShapeScanStart; i < vShapeScanEnd; i++) {
          const candle = candles[i];
          if (candle.low < minLow) {
            minLow = candle.low;
            extremeCandle = candle;
          }
        }
      } else { // EQL
        // For a broken EQL, the "setup V-shape" is the highest high (retracement) after the sustained break.
        let maxHigh = -Infinity;
        for (let i = vShapeScanStart; i < vShapeScanEnd; i++) {
          const candle = candles[i];
          if (candle.high > maxHigh) {
            maxHigh = candle.high;
            extremeCandle = candle;
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
