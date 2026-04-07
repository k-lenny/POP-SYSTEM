// server/src/final/final.js
// Imports and exposes everything from confirmedSetup engine as the "final" data layer.

const confirmedSetupEngine = require('../signals/dataProcessor/confirmedSetup');

class FinalEngine {
  /**
   * Returns all confirmed setups (OTE, DOUBLE EQ, S-SETUP) for a given symbol
   * and granularity, including the setupOB field from OBLV.
   * Delegates fully to confirmedSetupEngine.
   */
  getConfirmedSetups(symbol, granularity) {
    return confirmedSetupEngine.getConfirmedSetups(symbol, granularity);
  }
}

module.exports = new FinalEngine();
