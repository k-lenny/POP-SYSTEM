// server/src/routes/majorSwingsRoutes.js
const express = require('express');
const router = express.Router();

const majorSwingsEngine = require('../signals/dataProcessor/majorSwings');
const swingEngine = require('../signals/dataProcessor/swings');
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

// Helper to ensure data is loaded
async function ensureDataLoaded(symbol, granularity) {
  // 1. Ensure Candles
  let candles = signalEngine.getCandles(symbol, granularity, true);
  if (!candles || candles.length === 0) {
    try { signalEngine.subscribeSymbol(symbol, granularity); } catch (e) {}
    
    // Wait for data
    const start = Date.now();
    while(Date.now() - start < 5000) {
         await new Promise(r => setTimeout(r, 500));
         candles = signalEngine.getCandles(symbol, granularity, true);
         if(candles && candles.length > 10) break;
    }
  }

  // 2. Ensure Swings
  if (candles && candles.length) {
    const swings = swingEngine.get(symbol, granularity);
    if (swings.length === 0) {
      await swingEngine.detectAll(symbol, granularity, candles);
    }
  }
}

// GET /:symbol/:granularity
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

    await ensureDataLoaded(symbol, granularity);

    const majorSwings = majorSwingsEngine.getMajorSwings(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      count: majorSwings.length,
      majorSwings
    });

  } catch (err) {
    console.error('[MajorSwingsRoute] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;
