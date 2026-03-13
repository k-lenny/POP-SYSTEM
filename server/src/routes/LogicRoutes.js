// server/src/routes/LogicRoutes.js
const express = require('express');
const router = express.Router();

const logicEngine = require('../signals/dataProcessor/Logic');
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

// Helper: ensure levels exist in memory for this symbol/granularity.
// If none present, run detection.
async function ensureLevelsInMemory(symbol, granularity) {
  // Check if already in memory (filtered view)
  let levels = eqhEqlEngine.get(symbol, granularity);
  if (levels.length > 0) {
    console.log(`[LogicRoute] Using ${levels.length} cached levels for ${symbol} @ ${granularity}s`);
    return levels;
  }

  // No levels in memory – run detection
  console.log(`[LogicRoute] No levels in memory for ${symbol} @ ${granularity}s, starting detection...`);
  
  try {
    let candles = signalEngine.getCandles(symbol, granularity, true);
    
    // If no candles yet, subscribe and WAIT for them
    if (!candles || candles.length === 0) {
      console.log(`[LogicRoute] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`);
      
      try { 
        signalEngine.subscribeSymbol(symbol, granularity); 
      } catch (e) {}

      const minCandles = 10;
      const timeoutMs = 60000; // Wait up to 60 seconds
      const intervalMs = 1000;
      const start = Date.now();
      
      while (Date.now() - start < timeoutMs) {
        await new Promise(r => setTimeout(r, intervalMs));
        candles = signalEngine.getCandles(symbol, granularity, true);
        
        if (candles && candles.length >= minCandles) {
          console.log(`[LogicRoute] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`);
          break;
        }
        
        if ((Date.now() - start) % 5000 < 1000) {
          console.log(`[LogicRoute] Still waiting for ${symbol} @ ${granularity}s... (${Date.now() - start}ms elapsed)`);
        }
      }

      if (!candles || candles.length === 0) {
        console.log(`[LogicRoute] Timeout waiting for ${symbol} @ ${granularity}s, trying to force subscription...`);
        
        if (typeof signalEngine.subscribeToAllSymbols === 'function') {
          try {
            signalEngine.subscribeToAllSymbols();
          } catch (e) {}
        }
        
        const extraTimeout = 30000;
        const extraStart = Date.now();
        while (Date.now() - extraStart < extraTimeout) {
          await new Promise(r => setTimeout(r, intervalMs));
          candles = signalEngine.getCandles(symbol, granularity, true);
          if (candles && candles.length >= minCandles) break;
        }
      }
    }

    if (candles && candles.length) {
      console.log(`[LogicRoute] Running detections for ${symbol} @ ${granularity}s with ${candles.length} candles`);
      
      // Ensure swings exist
      try {
        const swings = swingEngine.get(symbol, granularity);
        if (!swings.length) {
          console.log(`[LogicRoute] No swings found, detecting swings first...`);
          await swingEngine.detectAll(symbol, granularity, candles);
        }
      } catch (e) {
        console.error('[LogicRoute] swing detection error:', e);
      }

      // Ensure breakouts exist (optional, but used for bias)
      try {
        const breakouts = breakoutEngine.get(symbol, granularity);
        if (!breakouts.length) {
          await breakoutEngine.detectAll(symbol, granularity, candles);
        }
      } catch (e) {
        console.error('[LogicRoute] breakout detection error:', e);
      }

      // Run EQH/EQL detection
      const detectedLevels = await eqhEqlEngine.detectAll(symbol, granularity, candles);
      console.log(`[LogicRoute] Detection complete: ${detectedLevels.length} levels found for ${symbol} @ ${granularity}s`);
      
      // After detection, check store again
      const newStore = eqhEqlEngine.store[symbol]?.[granularity];
      console.log(`[LogicRoute] Store now has ${newStore ? newStore.length : 0} levels`);
    } else {
      console.log(`[LogicRoute] WARNING: No candles available for ${symbol} @ ${granularity}s after all attempts`);
    }
  } catch (err) {
    console.error('[LogicRoute] ensureLevelsInMemory error:', err);
  }
  
  // Return filtered view
  return eqhEqlEngine.get(symbol, granularity);
}

// GET /logic/:symbol/:granularity – returns all levels with brokenBosType 'BOS_SUSTAINED'
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    await ensureLevelsInMemory(symbol, granularity);
    const levels = logicEngine.get(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      count: levels.length,
      levels: levels,
    });

  } catch (err) {
    console.error('[LogicRoute] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;
