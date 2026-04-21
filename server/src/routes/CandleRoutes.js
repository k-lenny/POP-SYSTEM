// server/src/routes/CandleRoutes.js
const express = require('express');
const router  = express.Router();

const { findPatterns } = require('../signals/dataProcessor/candle');
const signalEngine     = require('../signals/signalEngine');

const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../utils/resolvers');

// Fetch candles — subscribe and wait if not already in memory.
async function fetchCandles(symbol, granularity) {
  let candles = signalEngine.getCandles(symbol, granularity, true);
  if (candles && candles.length >= 1) return candles;

  console.log(`[CandleRoute] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`);
  try { signalEngine.subscribeSymbol(symbol, granularity); } catch (e) {}

  const timeoutMs  = 60000;
  const intervalMs = 1000;
  const start      = Date.now();

  while (Date.now() - start < timeoutMs) {
    await new Promise(r => setTimeout(r, intervalMs));
    candles = signalEngine.getCandles(symbol, granularity, true);
    if (candles && candles.length >= 1) {
      console.log(`[CandleRoute] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`);
      return candles;
    }
  }

  return candles || [];
}

// GET /candles/:symbol/:granularity
// Query:
//   pattern=doji         — filter to a single pattern type
//   limit=<n>            — return most recent N detections
//   bodyRatio=<float>    — override doji body/range cutoff (default 0.1)
//   minWickRatio=<float> — override doji per-wick minimum (default 0.2)
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const candles = await fetchCandles(symbol, granularity);
    if (!candles.length) {
      return sendSuccess(res, { symbol, granularity, count: 0, patterns: [], note: 'No candles available yet' });
    }

    const dojiOpts = {};
    if (req.query.bodyRatio)    dojiOpts.bodyRatio    = parseFloat(req.query.bodyRatio);
    if (req.query.minWickRatio) dojiOpts.minWickRatio = parseFloat(req.query.minWickRatio);

    const allPatterns = findPatterns(candles, { doji: dojiOpts });

    const counts = {};
    for (const p of allPatterns) counts[p.pattern] = (counts[p.pattern] || 0) + 1;

    let patterns = allPatterns;
    const pattern = req.query.pattern;
    if (pattern) patterns = patterns.filter(p => p.pattern === pattern);

    const limit = req.query.limit ? parseInt(req.query.limit, 10) : null;
    if (limit && limit > 0) patterns = patterns.slice(-limit);

    return sendSuccess(res, {
      symbol,
      granularity,
      candleCount: candles.length,
      counts,
      count:       patterns.length,
      patterns,
    });
  } catch (err) {
    console.error('[CandleRoute] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

// GET /candles/:symbol/:granularity/latest — most recent pattern detection
router.get('/:symbol/:granularity/latest', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const candles  = await fetchCandles(symbol, granularity);
    const patterns = findPatterns(candles);
    const latest   = patterns[patterns.length - 1] || null;

    return sendSuccess(res, { symbol, granularity, latest });
  } catch (err) {
    console.error('[CandleRoute] Latest error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

// GET /candles/:symbol/:granularity/summary — counts per pattern type
router.get('/:symbol/:granularity/summary', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const candles  = await fetchCandles(symbol, granularity);
    const patterns = findPatterns(candles);

    const byPattern = {};
    for (const p of patterns) {
      byPattern[p.pattern] = (byPattern[p.pattern] || 0) + 1;
    }

    return sendSuccess(res, {
      summary: {
        symbol,
        granularity,
        candleCount: candles.length,
        total:       patterns.length,
        byPattern,
        latest:      patterns[patterns.length - 1] || null,
      },
    });
  } catch (err) {
    console.error('[CandleRoute] Summary error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;
