// server/src/routes/ConsolidationRoutes.js
const express = require('express');
const router  = express.Router();

const { findConsolidations } = require('../signals/dataProcessor/Consolidation');
const signalEngine           = require('../signals/signalEngine');

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
  if (candles && candles.length >= 3) return candles;

  console.log(`[ConsolidationRoute] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`);
  try { signalEngine.subscribeSymbol(symbol, granularity); } catch (e) {}

  const minCandles = 3;
  const timeoutMs  = 60000;
  const intervalMs = 1000;
  const start      = Date.now();

  while (Date.now() - start < timeoutMs) {
    await new Promise(r => setTimeout(r, intervalMs));
    candles = signalEngine.getCandles(symbol, granularity, true);
    if (candles && candles.length >= minCandles) {
      console.log(`[ConsolidationRoute] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`);
      return candles;
    }
  }

  if (typeof signalEngine.subscribeToAllSymbols === 'function') {
    try { signalEngine.subscribeToAllSymbols(); } catch (e) {}
  }

  const extraTimeout = 30000;
  const extraStart   = Date.now();
  while (Date.now() - extraStart < extraTimeout) {
    await new Promise(r => setTimeout(r, intervalMs));
    candles = signalEngine.getCandles(symbol, granularity, true);
    if (candles && candles.length >= minCandles) return candles;
  }

  return candles || [];
}

// GET /consolidations/:symbol/:granularity
// Query:
//   type=high|low|both       — filter by pattern type
//   limit=<n>                — return most recent N zones
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const candles = await fetchCandles(symbol, granularity);
    if (!candles.length) {
      return sendSuccess(res, { symbol, granularity, count: 0, consolidations: [], note: 'No candles available yet' });
    }

    let zones = findConsolidations(candles);

    const type = req.query.type;
    if (type === 'high' || type === 'low' || type === 'both') {
      zones = zones.filter(z => z.type === type);
    }

    const limit = req.query.limit ? parseInt(req.query.limit, 10) : null;
    if (limit && limit > 0) zones = zones.slice(-limit);

    return sendSuccess(res, {
      symbol,
      granularity,
      candleCount:    candles.length,
      count:          zones.length,
      consolidations: zones,
    });
  } catch (err) {
    console.error('[ConsolidationRoute] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

// GET /consolidations/:symbol/:granularity/latest — most recent zone
router.get('/:symbol/:granularity/latest', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const candles = await fetchCandles(symbol, granularity);
    const zones   = findConsolidations(candles);
    const latest  = zones[zones.length - 1] || null;

    return sendSuccess(res, { symbol, granularity, latest });
  } catch (err) {
    console.error('[ConsolidationRoute] Latest error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

// GET /consolidations/:symbol/:granularity/summary — counts per type
router.get('/:symbol/:granularity/summary', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const candles = await fetchCandles(symbol, granularity);
    const zones   = findConsolidations(candles);

    const highCount = zones.filter(z => z.type === 'high').length;
    const lowCount  = zones.filter(z => z.type === 'low').length;
    const bothCount = zones.filter(z => z.type === 'both').length;

    return sendSuccess(res, {
      summary: {
        symbol,
        granularity,
        total:  zones.length,
        high:   highCount,
        low:    lowCount,
        both:   bothCount,
        latest: zones[zones.length - 1] || null,
      },
    });
  } catch (err) {
    console.error('[ConsolidationRoute] Summary error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;
