// server/src/final/final.js
// Imports and exposes everything from confirmedSetup engine as the "final" data layer.

const confirmedSetupEngine = require('../signals/dataProcessor/confirmedSetup');
const patternEngine = require('../strategies/patterns/pattern');
const pattern2Engine = require('../strategies/patterns/pattern2');

class FinalEngine {
  /**
   * Returns all confirmed setups (OTE, DOUBLE EQ, S-SETUP) for a given symbol
   * and granularity, including the setupOB field from OBLV.
   * Also checks if OBSetupExtreme's price matches the currentSwingPrice
   * in a pattern or the firstSwingPrice in a pattern2, and if so attaches
   * the matched pattern's data.
   */
  getConfirmedSetups(symbol, granularity) {
    const setups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);
    const pattern1Patterns = patternEngine.get(symbol, granularity);
    const pattern2Patterns = pattern2Engine.get(symbol, granularity);

    return setups.map(setup => {
      const patternMatch =
        this._findPatternByCurrentSwing(pattern1Patterns, setup.OBSetupExtreme?.price);
      const pattern2Match =
        this._findPattern2ByFirstSwing(pattern2Patterns, setup.OBSetupExtreme?.price);

      return { ...setup, patternMatch, pattern2Match };
    });
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
