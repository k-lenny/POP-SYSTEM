// server/src/signals/dataProcessor/Logic.js
const eqhEqlEngine = require('./eqhEql');

class LogicEngine {
  get(symbol, granularity) {
    const allLevels = eqhEqlEngine.get(symbol, granularity);
    return allLevels.filter(level => level.brokenBosType === 'BOS_SUSTAINED');
  }

  // You can add more methods here to expose other parts of the eqhEqlEngine if needed
  // For example, to get all data without filtering:
  getAll(symbol, granularity) {
    return eqhEqlEngine.get(symbol, granularity);
  }
}

module.exports = new LogicEngine();
