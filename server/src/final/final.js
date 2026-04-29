// server/src/final/final.js
// Imports and exposes everything from confirmedSetup engine as the "final" data layer.

const confirmedSetupEngine = require('../signals/dataProcessor/confirmedSetup');
const patternEngine = require('../strategies/patterns/pattern');
const pattern2Engine = require('../strategies/patterns/pattern2');
const pattern3Engine = require('../strategies/patterns/pattern3');
const swingEngine = require('../signals/dataProcessor/swings');
const signalEngine = require('../signals/signalEngine');
const { findConsolidations } = require('../signals/dataProcessor/Consolidation');
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

    // Patterns must be detected BEFORE this call — confirmedSetupEngine reads
    // them to identify the pattern-matched OBSetupExtreme.
    const setups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);

    const pattern1Patterns = patternEngine.get(symbol, granularity);
    const pattern2Patterns = pattern2Engine.get(symbol, granularity);
    const pattern3Patterns = pattern3Engine.get(symbol, granularity);

    const candleIndexMap = candles && candles.length ? buildCandleIndexMap(candles) : null;
    const consolidations = candles && candles.length ? findConsolidations(candles) : [];

    // If a pattern's retest candle index falls inside any consolidation's
    // [start.index, end.index] window, return that consolidation's
    // confirmationConsolidation.breakout.
    const findConfirmationBreakout = (retestIndex) => {
      if (retestIndex == null) return null;
      for (const z of consolidations) {
        const s = z.start?.index;
        const e = z.end?.index;
        if (s == null || e == null) continue;
        if (retestIndex >= s && retestIndex <= e) {
          return z.retest?.confirmationConsolidation?.breakout ?? null;
        }
      }
      return null;
    };

    return setups.map(setup => {
      const extremePrice = setup.OBSetupExtreme?.price;
      let patternMatch =
        this._findPatternByCurrentSwing(pattern1Patterns, extremePrice);
      // Invalidate patternMatch if its previousSwing formed before OBCross —
      // the pattern must be discovered after the OB was crossed.
      const obCrossFT = setup.OBCross?.formattedTime;
      if (
        patternMatch &&
        obCrossFT &&
        patternMatch.previousSwing?.formattedTime &&
        patternMatch.previousSwing.formattedTime < obCrossFT
      ) {
        patternMatch = null;
      }
      const pattern2Match =
        this._findPattern2ByFirstSwing(pattern2Patterns, extremePrice);
      const pattern3Match =
        this._findPattern3ByFirstSwing(pattern3Patterns, extremePrice);

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

      if (patternMatch) {
        patternMatch.confirmationConsolidationBreakout =
          findConfirmationBreakout(patternMatch.retest?.index);
      }
      if (pattern2Match) {
        pattern2Match.confirmationConsolidationBreakout =
          findConfirmationBreakout(pattern2Match.retestData?.index);
      }
      if (pattern3Match) {
        pattern3Match.confirmationConsolidationBreakout =
          findConfirmationBreakout(pattern3Match.retest?.index);
      }
      if (OBOppositeExtremePatternMatch) {
        OBOppositeExtremePatternMatch.confirmationConsolidationBreakout =
          findConfirmationBreakout(OBOppositeExtremePatternMatch.retest?.index);
      }
      if (OBOppositeExtremePattern2Match) {
        OBOppositeExtremePattern2Match.confirmationConsolidationBreakout =
          findConfirmationBreakout(OBOppositeExtremePattern2Match.retestData?.index);
      }
      if (OBOppositeExtremePattern3Match) {
        OBOppositeExtremePattern3Match.confirmationConsolidationBreakout =
          findConfirmationBreakout(OBOppositeExtremePattern3Match.retest?.index);
      }

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
          retest: p.retest ?? null,
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
    const setupPrice = OBSetupExtreme.price;
    let extremeCandle = null;

    for (let i = startPos + 1; i < candles.length; i++) {
      const c = candles[i];
      // Stop scanning once price crosses back through the OBSetupExtreme —
      // the opposite extreme must be found before that cross.
      if (isEQL && setupPrice != null && c.low < setupPrice) break;
      if (!isEQL && setupPrice != null && c.high > setupPrice) break;

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
          retest: {
            price: p.retestPrice ?? null,
            index: p.retestIndex ?? null,
            time: p.retestTime ?? null,
            formattedTime: p.retestFormattedTime ?? null,
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
          retestData: p.retestData ?? null,
          retestStatus: p.retestStatus ?? null,
        };
      }
    }
    return null;
  }
}

module.exports = new FinalEngine();
