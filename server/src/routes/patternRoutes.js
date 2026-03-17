const express = require('express');
const router = express.Router();

const patternEngine = require('../strategies/patterns/pattern');
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

// GET /pattern/:symbol/:granularity/stats – returns pattern statistics
router.get('/:symbol/:granularity/stats', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const stats = patternEngine.getStats(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      stats,
    });

  } catch (err) {
    console.error('[PatternRoute] Stats error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

// GET /pattern/:symbol/:granularity – returns ALL patterns (with optional limit and filters)
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req);
  try {
    const symbol = resolveSymbol(req.params.symbol);
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() });

    const granularity = resolveGranularity(req.params.granularity);
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() });

    const candles = signalEngine.getCandles(symbol, granularity, true);

    console.log(`[PatternRoute] Candles loaded: ${candles.length}`);

    if (!candles.length) {
      return sendSuccess(res, {
        message: 'No candles loaded yet — wait a few seconds and refresh',
        symbol,
        granularity,
        candleCount: 0,
        patternCount: 0,
        patterns: [],
      });
    }
    
    // Detect patterns
    const patterns = await patternEngine.detect(symbol, granularity, candles);
    
    console.log(`[PatternRoute] Patterns detected: ${patterns.length}`);

    // Apply filters
    let filteredPatterns = patterns;

    // Filter by direction
    if (req.query.direction) {
      const direction = req.query.direction.toLowerCase();
      filteredPatterns = filteredPatterns.filter(p => p.direction === direction);
    }

    // Filter by stage
    if (req.query.stage) {
      const stage = parseInt(req.query.stage);
      filteredPatterns = filteredPatterns.filter(p => p.stage === stage);
    }

    // Filter by completion status
    if (req.query.complete !== undefined) {
      const complete = req.query.complete === 'true';
      filteredPatterns = filteredPatterns.filter(p => p.isComplete === complete);
    }

    // Apply limit (default: return all, or use query param)
    const limit = req.query.limit ? parseInt(req.query.limit) : undefined;
    let resultPatterns = filteredPatterns;
    if (limit && limit > 0) {
      resultPatterns = filteredPatterns.slice(-limit);
    }

    // Get statistics
    const stats = patternEngine.getStats(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      candleCount: candles.length,
      totalPatterns: patterns.length,
      filteredCount: filteredPatterns.length,
      count: resultPatterns.length,
      stats,
      patterns: resultPatterns,
    });

  } catch (err) {
    console.error('[PatternRoute] Error:', err);
    console.error('[PatternRoute] Stack:', err.stack);
    return sendError(res, 500, 'Internal server error', { message: err.message, stack: err.stack });
  }
});

module.exports = router;