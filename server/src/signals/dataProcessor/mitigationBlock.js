const retestEngine = require('./retest');
const signalEngine = require('../signalEngine');
const { buildCandleIndexMap } = require('../../utils/dataProcessorUtils');

class MitigationBlockEngine {
  /**
   * Finds mitigation blocks based on retest data.
   * @param {string} symbol
   * @param {number} granularity
   * @returns {Array<Object>}
   */
  getMitigationBlocks(symbol, granularity) {
    const retests = retestEngine.getRetests(symbol, granularity);
    if (!retests.length) {
      return [];
    }

    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length) {
      return [];
    }

    const candleIndexMap = buildCandleIndexMap(candles);
    const results = [];

    for (const retest of retests) {
      const setupMitigationBlock = this._findSetupMitigationBlock(retest, candles, candleIndexMap);
      const prevMitigationBlock = this._findPrevMitigationBlock(retest, candles, candleIndexMap);
      const nextMitigationBlock = this._findNextMitigationBlock(retest, candles, candleIndexMap);

      results.push({
        ...retest,
        SetupMitigationBlock: setupMitigationBlock,
        PrevMitigationBlock: prevMitigationBlock,
        NextMitigationBlock: nextMitigationBlock,
      });
    }

    return results;
  }

  /**
   * Finds the last red/green candle between the original V-shape and the impulse extreme.
   * @private
   */
  _findSetupMitigationBlock(retest, candles, candleIndexMap) {
    const { type, preBreakoutVIndex, impulseExtremeIndex } = retest;

    if (preBreakoutVIndex === null || impulseExtremeIndex === null) {
      return null;
    }

    const isBuy = type === 'EQH';
    const pos1 = candleIndexMap.get(preBreakoutVIndex);
    const pos2 = candleIndexMap.get(impulseExtremeIndex);

    if (pos1 === undefined || pos2 === undefined) {
      return null;
    }

    const startPos = Math.min(pos1, pos2);
    const endPos = Math.max(pos1, pos2);

    for (let i = endPos; i >= startPos; i--) {
      const candle = candles[i];
      if (!candle) continue;

      const isGreen = candle.close > candle.open;
      const isRed = candle.close < candle.open;

      if ((isBuy && isGreen) || (!isBuy && isRed)) {
        return { high: candle.high, low: candle.low, formattedTime: candle.formattedTime };
      }
    }

    return null;
  }

  /**
   * Finds the mitigation block for the "Previous" setup (S-Setup).
   * @private
   */
  _findPrevMitigationBlock(retest, candles, candleIndexMap) {
    if (retest.PreviousStatus !== 'RIGHT S SETUP') {
      return null;
    }

    const { type, RetestExtremeSwingIndex, PreviousMSSExtremeIndex } = retest;

    if (RetestExtremeSwingIndex === null || PreviousMSSExtremeIndex === null) {
      return null;
    }

    const isBuy = type === 'EQH';
    const pos1 = candleIndexMap.get(RetestExtremeSwingIndex);
    const pos2 = candleIndexMap.get(PreviousMSSExtremeIndex);

    if (pos1 === undefined || pos2 === undefined) {
      return null;
    }

    const startPos = Math.min(pos1, pos2);
    const endPos = Math.max(pos1, pos2);

    for (let i = endPos; i >= startPos; i--) {
      const candle = candles[i];
      if (!candle) continue;

      const isGreen = candle.close > candle.open;
      const isRed = candle.close < candle.open;

      if ((isBuy && isGreen) || (!isBuy && isRed)) {
        return { high: candle.high, low: candle.low, formattedTime: candle.formattedTime };
      }
    }

    return null;
  }

  /**
   * Finds the mitigation block for the "Next" setup (OTE/DOUBLE EQ).
   * @private
   */
  _findNextMitigationBlock(retest, candles, candleIndexMap) {
    if (!['OTE', 'DOUBLE EQ'].includes(retest.NextStatus)) {
      return null;
    }

    const { type, RetestExtremeSwingIndex, NextMSSExtremeIndex } = retest;

    if (RetestExtremeSwingIndex === null || NextMSSExtremeIndex === null) {
      return null;
    }

    const isBuy = type === 'EQH';
    const pos1 = candleIndexMap.get(RetestExtremeSwingIndex);
    const pos2 = candleIndexMap.get(NextMSSExtremeIndex);

    if (pos1 === undefined || pos2 === undefined) {
      return null;
    }

    const startPos = Math.min(pos1, pos2);
    const endPos = Math.max(pos1, pos2);

    for (let i = endPos; i >= startPos; i--) {
      const candle = candles[i];
      if (!candle) continue;

      const isGreen = candle.close > candle.open;
      const isRed = candle.close < candle.open;

      if ((isBuy && isGreen) || (!isBuy && isRed)) {
        return { high: candle.high, low: candle.low, formattedTime: candle.formattedTime };
      }
    }

    return null;
  }
}

module.exports = new MitigationBlockEngine();