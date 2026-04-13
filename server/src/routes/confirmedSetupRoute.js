// server/src/routes/confirmedSetupRoute.js
const express = require('express');
const router = express.Router();

const confirmedSetupEngine = require('../signals/dataProcessor/confirmedSetup'); // The engine that finds confirmed setups
const eqhEqlEngine = require('../signals/dataProcessor/eqhEql'); // Dependency for setups
const swingEngine = require('../signals/dataProcessor/swings'); // Dependency for eqhEql
const breakoutEngine = require('../signals/dataProcessor/breakouts'); // Dependency for eqhEql
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
 * Helper to ensure all necessary data (candles, swings, breakouts, eqh/eql) is loaded
 * before trying to find setups. This is crucial for on-demand API calls.
 */
async function ensureDataLoaded(symbol, granularity) {
  try {
    // First, ensure we have candles to work with.
    let candles = signalEngine.getCandles(symbol, granularity, true);

    if (!candles || candles.length === 0) {
      console.log(`[ConfirmedSetupRoute] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`);
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
          console.log(`[ConfirmedSetupRoute] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`);
          break;
        }
      }
    }

    // If we have candles, proceed with detection/update.
    if (candles && candles.length) {
      const levels = eqhEqlEngine.get(symbol, granularity);

      if (levels.length === 0) {
        // No levels exist at all. Run the full, expensive detection pipeline once.
        console.log(`[ConfirmedSetupRoute] No levels found. Running full detection for ${symbol} @ ${granularity}s`);
        
        // The dependencies (swings, breakouts) must also be checked and run if empty.
        if (swingEngine.get(symbol, granularity).length === 0) {
          await swingEngine.detectAll(symbol, granularity, candles);
        }
        if (breakoutEngine.get(symbol, granularity).length === 0) {
          await breakoutEngine.detectAll(symbol, granularity, candles);
        }
        await eqhEqlEngine.detectAll(symbol, granularity, candles);
        console.log(`[ConfirmedSetupRoute] Full detection complete.`);
      } else {
        // Levels exist, but their status might be stale relative to the latest candles.
        // Run an incremental update to catch any new breaks or sweeps.
        console.log(`[ConfirmedSetupRoute] Levels exist. Running incremental update for ${symbol} @ ${granularity}s`);
        await eqhEqlEngine.detectLatest(symbol, granularity, candles);
        console.log(`[ConfirmedSetupRoute] Incremental update complete.`);
      }
    } else {
      console.log(`[ConfirmedSetupRoute] WARNING: No candles available for ${symbol} @ ${granularity}s after waiting.`);
    }
  } catch (err) {
    console.error(`[ConfirmedSetupRoute] Error in ensureDataLoaded for ${symbol}@${granularity}:`, err);
  }
}

router.get('/all', async (req, res) => {
  logRequest(req);
  try {
    const symbols = getValidSymbols().map(s => s.code);
    const rawGranularities = getValidGranularities();
    const resultMap = {};
    let totalSetups = 0;
    let obCrossCount = 0;
    const obCrossSymbols = new Set();
    let pattern2MatchCount = 0;
    const pattern2MatchSymbols = new Set();

    for (const symbol of symbols) {
      for (const rawGranularity of rawGranularities) {
        const granularity = resolveGranularity(rawGranularity);
        if (!granularity) continue;

        // Ensure all prerequisite data is loaded before getting setups
        await ensureDataLoaded(symbol, granularity);

        const setups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);
        if (setups && setups.length > 0) {
          if (!resultMap[symbol]) resultMap[symbol] = {};
          resultMap[symbol][granularity] = setups;
          totalSetups += setups.length;
          for (const s of setups) {
            if (s.OBCross) {
              obCrossCount++;
              obCrossSymbols.add(symbol);
            }
            if (s.pattern2Match) {
              pattern2MatchCount++;
              pattern2MatchSymbols.add(symbol);
            }
          }
        }
      }
    }

    return sendSuccess(res, {
      count: totalSetups,
      obCrossTotal: obCrossCount,
      obCrossSymbols: [...obCrossSymbols],
      pattern2MatchTotal: pattern2MatchCount,
      pattern2MatchSymbols: [...pattern2MatchSymbols],
      map: resultMap,
    });

  } catch (err) {
    console.error('[ConfirmedSetupRoute] Error /all:', err);
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
    let obCrossCount = 0;
    const obCrossSymbols = new Set();
    let pattern2MatchCount = 0;
    const pattern2MatchSymbols = new Set();

    await Promise.all(symbols.map(async (symbol) => {
      await ensureDataLoaded(symbol, granularity);
      const setups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);
      if (setups && setups.length > 0) {
        results[symbol] = setups;
        totalSetups += setups.length;
        for (const s of setups) {
          if (s.OBCross) {
            obCrossCount++;
            obCrossSymbols.add(symbol);
          }
          if (s.pattern2Match) {
            pattern2MatchCount++;
            pattern2MatchSymbols.add(symbol);
          }
        }
      }
    }));

    return sendSuccess(res, {
      granularity,
      totalSetups,
      symbolsWithSetups: Object.keys(results).length,
      obCrossTotal: obCrossCount,
      obCrossSymbols: [...obCrossSymbols],
      pattern2MatchTotal: pattern2MatchCount,
      pattern2MatchSymbols: [...pattern2MatchSymbols],
      ...(totalSetups === 0 && {
        reason: 'No confirmed setups found for any symbol at this granularity. EQH/EQL levels may not have formed or been broken yet.'
      }),
      data: results,
    });

  } catch (err) {
    console.error('[ConfirmedSetupRoute] Error /all/:granularity:', err);
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

    // Ensure all prerequisite data is loaded before getting setups
    await ensureDataLoaded(symbol, granularity);

    const confirmedSetups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      count: confirmedSetups.length,
      setups: confirmedSetups,
    });

  } catch (err) {
    console.error('[ConfirmedSetupRoute] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;