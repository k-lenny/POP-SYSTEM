// server/src/final/final.js
// Imports and exposes everything from confirmedSetup engine as the "final" data layer.

const confirmedSetupEngine = require('../signals/dataProcessor/confirmedSetup');
const patternEngine = require('../strategies/patterns/pattern');
const pattern2Engine = require('../strategies/patterns/pattern2');
const pattern3Engine = require('../strategies/patterns/pattern3');
const swingEngine = require('../signals/dataProcessor/swings');
const signalEngine = require('../signals/signalEngine');
const { buildCandleIndexMap } = require('../utils/dataProcessorUtils');

class FinalEngine {
  /**
   * Returns all confirmed setups (OTE, DOUBLE EQ, S-SETUP) for a given symbol
   * and granularity, including the setupOB field from OBLV.
   * Also checks if OBSetupExtreme's price matches the currentSwingPrice
   * in a pattern or the firstSwingPrice in a pattern2, and if so attaches
   * the matched pattern's data.
   */
  async getConfirmedSetups(symbol, granularity) {
    const setups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);

    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (candles && candles.length) {
      if (swingEngine.get(symbol, granularity).length === 0) {
        await swingEngine.detectAll(symbol, granularity, candles);
      }
      if (patternEngine.get(symbol, granularity).length === 0) {
        await patternEngine.detect(symbol, granularity, candles);
      }
      if (pattern2Engine.get(symbol, granularity).length === 0) {
        await pattern2Engine.detect(symbol, granularity, candles);
      }
      if (pattern3Engine.get(symbol, granularity).length === 0) {
        await pattern3Engine.detect(symbol, granularity, candles);
      }
    }

    const pattern1Patterns = patternEngine.get(symbol, granularity);
    const pattern2Patterns = pattern2Engine.get(symbol, granularity);
    const pattern3Patterns = pattern3Engine.get(symbol, granularity);

    const candleIndexMap = candles && candles.length ? buildCandleIndexMap(candles) : null;

    return setups.map(setup => {
      const patternMatch =
        this._findPatternByCurrentSwing(pattern1Patterns, setup.OBSetupExtreme?.price);
      const pattern2Match =
        this._findPattern2ByFirstSwing(pattern2Patterns, setup.OBSetupExtreme?.price);
      const pattern3Match =
        this._findPattern3ByFirstSwing(pattern3Patterns, setup.OBSetupExtreme?.price);

      const OBOppositeExtreme = this._findOBOppositeExtreme(
        setup.type,
        setup.OBSetupExtreme,
        candleIndexMap,
        candles
      );

      const oppositePrice = OBOppositeExtreme?.price;
      const OBOppositeExtremePatternMatch =
        this._findPatternByCurrentSwing(pattern1Patterns, oppositePrice);
      const OBOppositeExtremePattern2Match =
        this._findPattern2ByFirstSwing(pattern2Patterns, oppositePrice);
      const OBOppositeExtremePattern3Match =
        this._findPattern3ByFirstSwing(pattern3Patterns, oppositePrice);

      return {
        ...setup,
        OBOppositeExtreme,
        patternMatch,
        pattern2Match,
        pattern3Match,
        OBOppositeExtremePatternMatch,
        OBOppositeExtremePattern2Match,
        OBOppositeExtremePattern3Match,
      };
    });
  }

  /**
   * Finds a pattern3 pattern whose firstSwingPrice matches the given price.
   * Returns the breakout, firstSwing, secondSwing, vShape and OBFound from that pattern, or null.
   * @private
   */
  _findPattern3ByFirstSwing(patterns, firstSwingPrice) {
    if (firstSwingPrice == null) return null;

    for (const p of patterns) {
      if (p.firstSwingPrice === firstSwingPrice) {
        return {
          direction: p.direction ?? null,
          firstSwing: {
            type: p.firstSwingType ?? null,
            price: p.firstSwingPrice ?? null,
            index: p.firstSwingIndex ?? null,
            formattedTime: p.firstSwingFormattedTime ?? null,
          },
          secondSwing: {
            type: p.secondSwingType ?? null,
            price: p.secondSwingPrice ?? null,
            index: p.secondSwingIndex ?? null,
            formattedTime: p.secondSwingFormattedTime ?? null,
          },
          vShapeCandle: {
            index: p.vShapeCandleIndex ?? null,
            price: p.vShapeCandlePrice ?? null,
            open: p.vShapeCandleOpen ?? null,
            high: p.vShapeCandleHigh ?? null,
            low: p.vShapeCandleLow ?? null,
            close: p.vShapeCandleClose ?? null,
            formattedTime: p.vShapeCandleFormattedTime ?? null,
          },
          breakoutCandle: {
            index: p.breakoutCandleIndex ?? null,
            price: p.breakoutCandlePrice ?? null,
            open: p.breakoutCandleOpen ?? null,
            high: p.breakoutCandleHigh ?? null,
            low: p.breakoutCandleLow ?? null,
            close: p.breakoutCandleClose ?? null,
            formattedTime: p.breakoutCandleFormattedTime ?? null,
          },
          NumberOfNoLv: p.NumberOfNoLv ?? null,
          OBFound: p.OBFound ?? null,
        };
      }
    }
    return null;
  }

  /**
   * Finds the OBOppositeExtreme — the extreme candle on the OPPOSITE side found
   * after OBSetupExtreme. For an EQL setup, OBSetupExtreme is the lowest low, so
   * the opposite extreme is the highest high after it. For an EQH setup, it is
   * the lowest low after the highest high.
   * @private
   */
  _findOBOppositeExtreme(type, OBSetupExtreme, candleIndexMap, candles) {
    if (!OBSetupExtreme || !candleIndexMap || !candles || !candles.length) return null;

    const startPos = candleIndexMap.get(OBSetupExtreme.index);
    if (startPos === undefined) return null;

    const isEQL = type === 'EQL';
    let extremeCandle = null;

    for (let i = startPos + 1; i < candles.length; i++) {
      const c = candles[i];
      if (!extremeCandle) {
        extremeCandle = c;
        continue;
      }
      if (isEQL && c.high > extremeCandle.high) {
        extremeCandle = c;
      } else if (!isEQL && c.low < extremeCandle.low) {
        extremeCandle = c;
      }
    }

    if (!extremeCandle) return null;

    return {
      index: extremeCandle.index,
      price: isEQL ? extremeCandle.high : extremeCandle.low,
      formattedTime: extremeCandle.formattedTime,
    };
  }

  /**
   * Finds a pattern whose currentSwingPrice matches the given price.
   * Returns the breakout, currentSwing, and previousSwing from that pattern, or null.
   * @private
   */
  _findPatternByCurrentSwing(patterns, currentSwingPrice) {
    if (currentSwingPrice == null) return null;

    for (const p of patterns) {
      if (p.currentSwingPrice === currentSwingPrice) {
        return {
          breakoutData: {
            price: p.breakoutPrice ?? null,
            index: p.breakoutIndex ?? null,
            formattedTime: p.breakoutFormattedTime ?? null,
          },
          currentSwing: {
            price: p.currentSwingPrice ?? null,
            index: p.currentSwingIndex ?? null,
            formattedTime: p.currentSwingFormattedTime ?? null,
          },
          previousSwing: {
            price: p.previousSwingPrice ?? null,
            index: p.previousSwingIndex ?? null,
            formattedTime: p.previousSwingFormattedTime ?? null,
          },
        };
      }
    }
    return null;
  }

  /**
   * Finds a pattern2 pattern whose firstSwingPrice matches the given price.
   * Returns the breakout, firstSwing, and secondSwing from that pattern, or null.
   * @private
   */
  _findPattern2ByFirstSwing(patterns, firstSwingPrice) {
    if (firstSwingPrice == null) return null;

    for (const p of patterns) {
      if (p.firstSwingPrice === firstSwingPrice) {
        return {
          breakoutData: p.breakoutData ?? null,
          firstSwing: {
            price: p.firstSwingPrice ?? null,
            index: p.firstSwingIndex ?? null,
            formattedTime: p.firstSwingFormattedTime ?? null,
          },
          secondSwing: {
            price: p.secondSwingPrice ?? null,
            index: p.secondSwingIndex ?? null,
            formattedTime: p.secondSwingFormattedTime ?? null,
          },
        };
      }
    }
    return null;
  }
}

module.exports = new FinalEngine();
