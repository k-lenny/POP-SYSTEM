const express = require('express');
const router = express.Router();
const retestEngine = require('../signals/dataProcessor/retest');
const signalEngine = require('../signals/signalEngine');
const eqhEqlEngine = require('../signals/dataProcessor/eqhEql');
const swingEngine = require('../signals/dataProcessor/swings');
const breakoutEngine = require('../signals/dataProcessor/breakouts');

const { volatilitySymbols, timeframes } = signalEngine;

// Helper to validate symbol/granularity
const validateParams = (req, res, next) => {
  const { symbol, granularity } = req.params;
  
  // Check if symbol exists in our map (values)
  const validSymbols = Object.values(volatilitySymbols);
  if (!validSymbols.includes(symbol)) {
    return res.status(400).json({ error: `Invalid symbol: ${symbol}` });
  }

  // Check if granularity is valid
  const validGrans = Object.values(timeframes);
  if (!validGrans.includes(Number(granularity))) {
    return res.status(400).json({ error: `Invalid granularity: ${granularity}` });
  }

  next();
};

/**
 * Helper to ensure all necessary data is loaded before processing retests.
 */
async function ensureDataLoaded(symbol, granularity) {
  try {
    // 1. Ensure Candles
    let candles = signalEngine.getCandles(symbol, granularity, true);

    if (!candles || candles.length === 0) {
      console.log(`[RetestRoute] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`);
      try {
        signalEngine.subscribeSymbol(symbol, granularity);
      } catch (e) { /* Ignore if already subscribed */ }

      const minCandles = 10;
      const timeoutMs = 15000; // 15 seconds wait
      const intervalMs = 500;
      const start = Date.now();

      while (Date.now() - start < timeoutMs) {
        await new Promise(r => setTimeout(r, intervalMs));
        candles = signalEngine.getCandles(symbol, granularity, true);
        if (candles && candles.length >= minCandles) {
          break;
        }
      }
    }

    // 2. Ensure Derived Data (Swings, Breakouts, Levels)
    if (candles && candles.length) {
      const levels = eqhEqlEngine.get(symbol, granularity);

      if (levels.length === 0) {
        console.log(`[RetestRoute] No levels found. Running full detection for ${symbol} @ ${granularity}s`);
        
        // Ensure swings
        if (swingEngine.get(symbol, granularity).length === 0) {
          await swingEngine.detectAll(symbol, granularity, candles);
        }
        // Ensure breakouts
        if (breakoutEngine.get(symbol, granularity).length === 0) {
          await breakoutEngine.detectAll(symbol, granularity, candles);
        }
        // Detect levels
        await eqhEqlEngine.detectAll(symbol, granularity, candles);
      } else {
        // Incremental update
        await eqhEqlEngine.detectLatest(symbol, granularity, candles);
      }
    }
  } catch (err) {
    console.error(`[RetestRoute] Error in ensureDataLoaded:`, err);
  }
}

/**
 * GET /retests/:symbol/:granularity
 * Returns all retest setups for the given symbol and timeframe.
 */
router.get('/:symbol/:granularity', validateParams, async (req, res) => {
  try {
    const { symbol, granularity } = req.params;
    
    // Ensure data is loaded before querying
    await ensureDataLoaded(symbol, Number(granularity));

    const data = retestEngine.getRetests(symbol, Number(granularity));
    res.json(data);
  } catch (error) {
    console.error('Error fetching retests:', error);
    res.status(500).json({ error: 'Internal server error processing retests' });
  }
});

module.exports = router;