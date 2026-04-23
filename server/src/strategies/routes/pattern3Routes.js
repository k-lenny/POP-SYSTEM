// server/src/strategies/routes/pattern3Routes.js
const express = require('express');
const router = express.Router();

const pattern3Engine = require('../patterns/pattern3');
const swingEngine = require('../../signals/dataProcessor/swings');
const signalEngine = require('../../signals/signalEngine');

const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../../utils/resolvers');

async function ensureDataLoaded(symbol, granularity) {
  try {
    let candles = signalEngine.getCandles(symbol, granularity, true);

    if (!candles || candles.length === 0) {
      console.log(`[Pattern3Route] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`);
      try {
        signalEngine.subscribeSymbol(symbol, granularity);
      } catch (e) { /* already subscribed */ }

      const minCandles = 10;
      const timeoutMs = 30000;
      const intervalMs = 1000;
      const start = Date.now();

      while (Date.now() - start < timeoutMs) {
        await new Promise(r => setTimeout(r, intervalMs));
        candles = signalEngine.getCandles(symbol, granularity, true);
        if (candles && candles.length >= minCandles) {
          console.log(`[Pattern3Route] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`);
          break;
        }
      }
    }

    if (candles && candles.length) {
      if (swingEngine.get(symbol, granularity).length === 0) {
        console.log(`[Pattern3Route] No swings found. Running swing detection for ${symbol} @ ${granularity}s`);
        await swingEngine.detectAll(symbol, granularity, candles);
      }
      await pattern3Engine.detect(symbol, granularity, candles);
      console.log(`[Pattern3Route] Pattern3 detection complete.`);
    } else {
      console.log(`[Pattern3Route] WARNING: No candles available for ${symbol} @ ${granularity}s after waiting.`);
    }
  } catch (err) {
    console.error(`[Pattern3Route] Error in ensureDataLoaded for ${symbol}@${granularity}:`, err);
  }
}

router.get('/:symbol/:granularity/stats', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const stats = pattern3Engine.getStats(symbol, granularity);
    return sendSuccess(res, { symbol, granularity, stats });
  } catch (err) {
    console.error('[Pattern3Route] Stats error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

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

    let patterns = pattern3Engine.get(symbol, granularity);
    const stats = pattern3Engine.getStats(symbol, granularity);

    if (req.query.direction) {
      const direction = req.query.direction.toLowerCase();
      patterns = patterns.filter(p => p.direction === direction);
    }

    const limit = req.query.limit ? parseInt(req.query.limit) : undefined;
    const resultPatterns = limit && limit > 0 ? patterns.slice(-limit) : patterns;

    return sendSuccess(res, {
      symbol,
      granularity,
      count: resultPatterns.length,
      stats,
      patterns: resultPatterns,
    });
  } catch (err) {
    console.error('[Pattern3Route] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;
