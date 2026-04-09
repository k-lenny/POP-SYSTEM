// server/src/routes/setupRoutes.js
const express = require('express');
const router = express.Router();

const setupEngine = require('../signals/dataProcessor/setup');
const eqhEqlEngine = require('../signals/dataProcessor/eqhEql');
const swingEngine = require('../signals/dataProcessor/swings');
const breakoutEngine = require('../signals/dataProcessor/breakouts');
const signalEngine = require('../signals/signalEngine');
const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../utils/resolvers');

async function ensureEqhEqlLevels(symbol, granularity) {
  let levels = eqhEqlEngine.get(symbol, granularity);
  if (levels.length > 0) return levels;

  console.log(`[SetupRoute] No EQH/EQL levels in memory for ${symbol} @ ${granularity}s, running detection...`);

  let candles = signalEngine.getCandles(symbol, granularity, true);

  if (!candles || candles.length === 0) {
    try { signalEngine.subscribeSymbol(symbol, granularity); } catch (e) {}
    const timeoutMs = 60000;
    const intervalMs = 1000;
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      await new Promise(r => setTimeout(r, intervalMs));
      candles = signalEngine.getCandles(symbol, granularity, true);
      if (candles && candles.length >= 10) break;
    }
  }

  if (candles && candles.length) {
    try {
      const swings = swingEngine.get(symbol, granularity);
      if (!swings.length) await swingEngine.detectAll(symbol, granularity, candles);
    } catch (e) {}

    try {
      const breakouts = breakoutEngine.get(symbol, granularity);
      if (!breakouts.length) await breakoutEngine.detectAll(symbol, granularity, candles);
    } catch (e) {}

    await eqhEqlEngine.detectAll(symbol, granularity, candles);
  }

  return eqhEqlEngine.get(symbol, granularity);
}

router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) {
      return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });
    }

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) {
      return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });
    }

    // Ensure EQH/EQL levels exist before computing setups
    await ensureEqhEqlLevels(symbol, granularity);

    const setups = setupEngine.getSetups(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      count: setups.length,
      setups,
    });

  } catch (err) {
    console.error('[SetupRoute] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;
