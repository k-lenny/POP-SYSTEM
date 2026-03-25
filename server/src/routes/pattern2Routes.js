// server/src/routes/pattern2Routes.js
const express = require('express');
const router = express.Router();

const pattern2Engine = require('../strategies/patterns/pattern2');
const swingEngine = require('../signals/dataProcessor/swings'); // Dependency for Pattern2Engine
const signalEngine = require('../signals/signalEngine'); // For getting candles

const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../utils/resolvers');

/**
 * Helper to ensure all necessary data (candles, swings) is loaded
 * before trying to find patterns. This is crucial for on-demand API calls.
 */
async function ensureDataLoaded(symbol, granularity) {
  try {
    // First, ensure we have candles to work with.
    let candles = signalEngine.getCandles(symbol, granularity, true);

    if (!candles || candles.length === 0) {
      console.log(`[Pattern2Route] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`);
      try {
        signalEngine.subscribeSymbol(symbol, granularity);
      } catch (e) { /* Ignore if already subscribed */ }

      const minCandles = 10;
      const timeoutMs = 30000;
      const intervalMs = 1000;
      const start = Date.now();

      while (Date.now() - start < timeoutMs) {
        await new Promise(r => setTimeout(r, intervalMs));
        candles = signalEngine.getCandles(symbol, granularity, true);
        if (candles && candles.length >= minCandles) {
          console.log(`[Pattern2Route] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`);
          break;
        }
      }
    }

    // If we have candles, proceed with detection/update.
    if (candles && candles.length) {
      // Ensure swings are detected as Pattern2Engine depends on them
      if (swingEngine.get(symbol, granularity).length === 0) {
        console.log(`[Pattern2Route] No swings found. Running swing detection for ${symbol} @ ${granularity}s`);
        await swingEngine.detectAll(symbol, granularity, candles);
      }
      // Run pattern2 detection
      await pattern2Engine.detect(symbol, granularity, candles);
      console.log(`[Pattern2Route] Pattern2 detection complete.`);
    } else {
      console.log(`[Pattern2Route] WARNING: No candles available for ${symbol} @ ${granularity}s after waiting.`);
    }
  } catch (err) {
    console.error(`[Pattern2Route] Error in ensureDataLoaded for ${symbol}@${granularity}:`, err);
  }
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

    // Ensure all prerequisite data is loaded before getting patterns
    await ensureDataLoaded(symbol, granularity);

    const patterns = pattern2Engine.get(symbol, granularity);
    const stats = pattern2Engine.getStats(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      count: patterns.length,
      stats,
      patterns,
    });

  } catch (err) {
    console.error('[Pattern2Route] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;