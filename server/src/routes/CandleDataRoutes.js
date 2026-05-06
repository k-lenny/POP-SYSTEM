// server/src/routes/CandleDataRoutes.js
//
// Routes for accessing candle data with body percentage calculations

const express = require('express');
const router = express.Router();

// Import our candle data processor
const { 
  calculateBodyPercentage, 
  calculateBodyPercentages, 
  getCandlesWithBodyPercentage,
  getBodyPercentageStats 
} = require('../signals/dataProcessor/CandleData');

// Import signal engine for fetching raw candle data
const signalEngine = require('../signals/signalEngine');

/**
 * Helper: parse count query parameter
 * - missing or invalid → returns null (means "return all candles")
 * - valid positive integer → returns that number
 */
function parseCountParam(queryCount) {
  if (queryCount === undefined || queryCount === '') return null;
  const parsed = parseInt(queryCount, 10);
  if (isNaN(parsed) || parsed <= 0) return null;
  return parsed;
}

/**
 * GET /candlestat/:symbol/:granularity
 * Fetch candles with body percentage data
 * Query parameters:
 * - count: (optional) number of most recent candles to return. Omit to get all.
 */
router.get('/:symbol/:granularity', async (req, res) => {
  try {
    const { symbol, granularity } = req.params;
    const count = parseCountParam(req.query.count);
    
    // Validate inputs
    if (!symbol || !granularity) {
      return res.status(400).json({ 
        error: 'Symbol and granularity are required' 
      });
    }
    
    // Fetch candles with body percentages (pass count = null for all)
    const candles = getCandlesWithBodyPercentage(symbol, granularity, count);
    
    res.json({
      symbol,
      granularity,
      count: candles.length,
      candles
    });
  } catch (error) {
    console.error('Error fetching candles:', error);
    res.status(500).json({ 
      error: 'Failed to fetch candles',
      message: error.message 
    });
  }
});

/**
 * GET /candlestat/body-percentages/:symbol/:granularity
 * Fetch body percentages for candles (no other candle data)
 * Query parameters:
 * - count: (optional) number of most recent candles. Omit to get all.
 */
router.get('/body-percentages/:symbol/:granularity', async (req, res) => {
  try {
    const { symbol, granularity } = req.params;
    const count = parseCountParam(req.query.count);
    
    // Validate inputs
    if (!symbol || !granularity) {
      return res.status(400).json({ 
        error: 'Symbol and granularity are required' 
      });
    }
    
    // Fetch raw candles from signalEngine.
    // If count is null, call with two arguments (assume signalEngine returns all).
    // If count is a number, pass it as third argument (if signalEngine respects it).
    let candles;
    if (count === null) {
      candles = signalEngine.getCandles(symbol, granularity);
    } else {
      candles = signalEngine.getCandles(symbol, granularity, count);
    }
    
    if (!Array.isArray(candles)) {
      return res.status(500).json({ 
        error: 'Failed to fetch valid candle data' 
      });
    }
    
    // Manually enforce count limit if signalEngine ignored it and count is provided
    if (count !== null && candles.length > count) {
      candles = candles.slice(-count);
    }
    
    // Calculate body percentages
    const bodyPercentages = calculateBodyPercentages(candles);
    
    res.json({
      symbol,
      granularity,
      count: bodyPercentages.length,
      bodyPercentages
    });
  } catch (error) {
    console.error('Error calculating body percentages:', error);
    res.status(500).json({ 
      error: 'Failed to calculate body percentages',
      message: error.message 
    });
  }
});

/**
 * GET /candlestat/stats/:symbol/:granularity
 * Fetch statistics for body percentages (avg, min, max, etc.)
 * Query parameters:
 * - count: (optional) number of most recent candles. Omit to get all.
 */
router.get('/stats/:symbol/:granularity', async (req, res) => {
  try {
    const { symbol, granularity } = req.params;
    const count = parseCountParam(req.query.count);
    
    // Validate inputs
    if (!symbol || !granularity) {
      return res.status(400).json({ 
        error: 'Symbol and granularity are required' 
      });
    }
    
    // Fetch raw candles from signalEngine
    let candles;
    if (count === null) {
      candles = signalEngine.getCandles(symbol, granularity);
    } else {
      candles = signalEngine.getCandles(symbol, granularity, count);
    }
    
    if (!Array.isArray(candles)) {
      return res.status(500).json({ 
        error: 'Failed to fetch valid candle data' 
      });
    }
    
    // Enforce count limit if signalEngine ignored it
    if (count !== null && candles.length > count) {
      candles = candles.slice(-count);
    }
    
    // Calculate statistics
    const stats = getBodyPercentageStats(candles);
    
    res.json({
      symbol,
      granularity,
      count: candles.length,
      stats
    });
  } catch (error) {
    console.error('Error calculating statistics:', error);
    res.status(500).json({ 
      error: 'Failed to calculate statistics',
      message: error.message 
    });
  }
});

/**
 * POST /candlestat/body-percentage
 * Calculate body percentage for a single candle
 * Request body: { open, high, low, close }
 */
router.post('/body-percentage', (req, res) => {
  try {
    const candle = req.body;
    
    if (!candle || typeof candle !== 'object') {
      return res.status(400).json({ 
        error: 'Candle data is required in request body' 
      });
    }
    
    const requiredFields = ['open', 'high', 'low', 'close'];
    for (const field of requiredFields) {
      if (typeof candle[field] !== 'number') {
        return res.status(400).json({ 
          error: `Candle must contain valid numeric value for ${field}` 
        });
      }
    }
    
    const bodyPercentage = calculateBodyPercentage(candle);
    
    res.json({
      bodyPercentage,
      isGreen: candle.close > candle.open,
      isRed: candle.close < candle.open,
      isOpen: candle.close === candle.open
    });
  } catch (error) {
    console.error('Error calculating body percentage:', error);
    res.status(500).json({ 
      error: 'Failed to calculate body percentage',
      message: error.message 
    });
  }
});

module.exports = router;