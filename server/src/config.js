// server/src/config.js

const defaultConfig = {
  // Maximum number of candles after a break to wait for sustained confirmation
  MAX_BOS_SCAN_CANDLES: 10,

  // Maximum number of candles to scan for a setup V-shape after a breakout
  MAX_SETUP_SCAN_CANDLES: 50,

  // Maximum number of EQH/EQL levels to retain per symbol/granularity
  MAX_LEVELS_PER_SYMBOL: 1000,

  // Maximum age of levels to retain (in milliseconds). Set to 0 to disable.
  MAX_LEVEL_AGE_MS: 30 * 24 * 60 * 60 * 1000, // 30 days

  // Logging level (debug, info, warn, error)
  LOG_LEVEL: 'info',

  // Whether to emit events
  ENABLE_EVENTS: true,

  // Confidence threshold for considering a level "high confidence"
  CONFIDENCE_THRESHOLD: 0.7,
};

// Per‑symbol/granularity overrides (symbol_granularity as key)
const overrides = {
  // Example: 'BTCUSDT_60': { MAX_BOS_SCAN_CANDLES: 15 },
  // 'EURUSD_300': { MAX_LEVELS_PER_SYMBOL: 500 },
};

function getConfig(symbol, granularity) {
  const key = `${symbol}_${granularity}`;
  const override = overrides[key] || {};
  return { ...defaultConfig, ...override };
}

module.exports = { defaultConfig, getConfig, overrides };