// server/src/signals/dataProcessor/CandleData.js
//
// Utility functions for candle data processing
// Calculates body percentage and other candle metrics

const signalEngine = require('../signalEngine');

/**
 * Calculate the body percentage of a candle (absolute size).
 * Formula: |close - open| / (high - low) * 100
 * 
 * @param {Object} candle - OHLC candle object with open, high, low, close properties
 * @returns {number} Body percentage (0 to 100)
 */
function calculateBodyPercentage(candle) {
  if (!candle || typeof candle !== 'object') {
    throw new Error('Invalid candle data provided');
  }
  
  const { open, high, low, close } = candle;
  
  // Validate required fields
  if ([open, high, low, close].some(field => typeof field !== 'number')) {
    throw new Error('Candle must contain valid numeric values for open, high, low, close');
  }
  
  const range = high - low;
  
  // Avoid division by zero
  if (range === 0) {
    return 0;
  }
  
  // Use absolute difference to always return a positive percentage
  const body = Math.abs(close - open);
  const bodyPercentage = (body / range) * 100;
  
  return parseFloat(bodyPercentage.toFixed(2));
}

/**
 * Calculate body percentage for an array of candles
 * 
 * @param {Array} candles - Array of OHLC candle objects
 * @returns {Array} Array of body percentages (positive values)
 */
function calculateBodyPercentages(candles) {
  if (!Array.isArray(candles)) {
    throw new Error('Candles must be an array');
  }
  
  return candles.map((candle, index) => {
    try {
      return {
        index,
        time: candle.time,
        bodyPercentage: calculateBodyPercentage(candle),
        isGreen: candle.close > candle.open,
        isRed: candle.close < candle.open,
        isOpen: candle.close === candle.open
      };
    } catch (error) {
      return {
        index,
        time: candle.time,
        error: error.message
      };
    }
  });
}

/**
 * Fetch candles from signalEngine and calculate body percentages
 * 
 * @param {string} symbol - Trading symbol
 * @param {string} granularity - Timeframe granularity
 * @param {number|null} [count] - Number of candles to fetch. If omitted, returns all candles.
 * @returns {Array} Array of candles with body percentage data
 */
function getCandlesWithBodyPercentage(symbol, granularity, count = null) {
  try {
    // Fetch candles from signalEngine (may ignore count parameter)
    let candles = signalEngine.getCandles(symbol, granularity, count);
    
    if (!Array.isArray(candles)) {
      throw new Error('Failed to fetch valid candle data from signalEngine');
    }
    
    // Only slice if count is explicitly provided and is a positive number
    if (typeof count === 'number' && !isNaN(count) && count > 0 && candles.length > count) {
      candles = candles.slice(-count);   // take the most recent 'count' candles
    }
    
    // Calculate body percentages for all candles
    const candlesWithPercentages = candles.map((candle, index) => {
      return {
        ...candle,
        bodyPercentage: calculateBodyPercentage(candle),
        isGreen: candle.close > candle.open,
        isRed: candle.close < candle.open,
        isOpen: candle.close === candle.open
      };
    });
    
    return candlesWithPercentages;
  } catch (error) {
    console.error('Error fetching candles with body percentages:', error);
    throw error;
  }
}

/**
 * Get statistics about body percentages in a set of candles
 * 
 * @param {Array} candles - Array of OHLC candle objects
 * @returns {Object} Statistics object
 */
function getBodyPercentageStats(candles) {
  if (!Array.isArray(candles) || candles.length === 0) {
    return {
      average: 0,
      max: 0,
      min: 0,
      greenCandleCount: 0,
      redCandleCount: 0,
      openCandleCount: 0,
      totalCandles: 0
    };
  }
  
  const bodyPercentages = candles.map(c => calculateBodyPercentage(c));
  const greenCandles = candles.filter(c => c.close > c.open).length;
  const redCandles = candles.filter(c => c.close < c.open).length;
  const openCandles = candles.filter(c => c.close === c.open).length;
  
  const sum = bodyPercentages.reduce((a, b) => a + b, 0);
  const avg = sum / bodyPercentages.length;
  
  return {
    average: parseFloat(avg.toFixed(2)),
    max: parseFloat(Math.max(...bodyPercentages).toFixed(2)),
    min: parseFloat(Math.min(...bodyPercentages).toFixed(2)),
    greenCandleCount: greenCandles,
    redCandleCount: redCandles,
    openCandleCount: openCandles,
    totalCandles: candles.length
  };
}

module.exports = {
  calculateBodyPercentage,
  calculateBodyPercentages,
  getCandlesWithBodyPercentage,
  getBodyPercentageStats
};