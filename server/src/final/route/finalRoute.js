// server/src/final/route/finalRoute.js
const express = require('express');
const router = express.Router();

const finalEngine = require('../final'); // FinalEngine wrapper
const eqhEqlEngine = require('../../signals/dataProcessor/eqhEql');
const swingEngine = require('../../signals/dataProcessor/swings');
const breakoutEngine = require('../../signals/dataProcessor/breakouts');
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

/**
 * Ensure candles + prerequisite levels are loaded before querying the final engine.
 */
async function ensureDataLoaded(symbol, granularity) {
  try {
    let candles = signalEngine.getCandles(symbol, granularity, true);

    if (!candles || candles.length === 0) {
      console.log(`[FinalRoute] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`);
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
          console.log(`[FinalRoute] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`);
          break;
        }
      }
    }

    if (candles && candles.length) {
      const levels = eqhEqlEngine.get(symbol, granularity);

      if (levels.length === 0) {
        console.log(`[FinalRoute] No levels found. Running full detection for ${symbol} @ ${granularity}s`);
        if (swingEngine.get(symbol, granularity).length === 0) {
          await swingEngine.detectAll(symbol, granularity, candles);
        }
        if (breakoutEngine.get(symbol, granularity).length === 0) {
          await breakoutEngine.detectAll(symbol, granularity, candles);
        }
        await eqhEqlEngine.detectAll(symbol, granularity, candles);
        console.log(`[FinalRoute] Full detection complete.`);
      } else {
        console.log(`[FinalRoute] Levels exist. Running incremental update for ${symbol} @ ${granularity}s`);
        await eqhEqlEngine.detectLatest(symbol, granularity, candles);
        console.log(`[FinalRoute] Incremental update complete.`);
      }
    } else {
      console.log(`[FinalRoute] WARNING: No candles available for ${symbol} @ ${granularity}s after waiting.`);
    }
  } catch (err) {
    console.error(`[FinalRoute] Error in ensureDataLoaded for ${symbol}@${granularity}:`, err);
  }
}

router.get('/all', async (req, res) => {
  logRequest(req);
  try {
    const symbols = getValidSymbols().map(s => s.code);
    const rawGranularities = getValidGranularities();
    const resultMap = {};
    let totalSetups = 0;

    for (const symbol of symbols) {
      for (const rawGranularity of rawGranularities) {
        const granularity = resolveGranularity(rawGranularity);
        if (!granularity) continue;

        await ensureDataLoaded(symbol, granularity);

        const setups = finalEngine.getConfirmedSetups(symbol, granularity);
        if (setups && setups.length > 0) {
          if (!resultMap[symbol]) resultMap[symbol] = {};
          resultMap[symbol][granularity] = setups;
          totalSetups += setups.length;
        }
      }
    }

    return sendSuccess(res, {
      count: totalSetups,
      map: resultMap,
    });
  } catch (err) {
    console.error('[FinalRoute] Error /all:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

router.get('/all/:granularity', async (req, res) => {
  logRequest(req);
  try {
    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) {
      return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });
    }

    const symbols = getValidSymbols().map(s => s.code);
    const results = {};
    let totalSetups = 0;

    await Promise.all(symbols.map(async (symbol) => {
      await ensureDataLoaded(symbol, granularity);
      const setups = finalEngine.getConfirmedSetups(symbol, granularity);
      if (setups && setups.length > 0) {
        results[symbol] = setups;
        totalSetups += setups.length;
      }
    }));

    return sendSuccess(res, {
      granularity,
      totalSetups,
      symbolsWithSetups: Object.keys(results).length,
      ...(totalSetups === 0 && {
        reason: 'No confirmed setups found for any symbol at this granularity.'
      }),
      data: results,
    });
  } catch (err) {
    console.error('[FinalRoute] Error /all/:granularity:', err);
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

    const setups = finalEngine.getConfirmedSetups(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      count: setups.length,
      setups,
    });
  } catch (err) {
    console.error('[FinalRoute] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;
