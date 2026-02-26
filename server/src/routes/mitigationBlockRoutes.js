const express = require('express');
const mitigationBlockEngine = require('../signals/dataProcessor/mitigationBlock');

const router = express.Router();

/**
 * GET /api/mitigation-blocks/:symbol/:granularity
 * Retrieves mitigation blocks for a given symbol and granularity.
 */
router.get('/:symbol/:granularity', (req, res) => {
  try {
    const { symbol, granularity: granularityStr } = req.params;
    const granularity = parseInt(granularityStr, 10);

    if (!symbol) {
      return res.status(400).json({ error: 'Symbol parameter is required.' });
    }
    if (isNaN(granularity) || granularity <= 0) {
      return res.status(400).json({ error: 'Granularity must be a positive integer.' });
    }

    const mitigationBlocks = mitigationBlockEngine.getMitigationBlocks(symbol.toUpperCase(), granularity);
    res.json(mitigationBlocks);
  } catch (err) {
    console.error(`[MitigationBlockRoutes] Error fetching mitigation blocks: ${err.message}`, err.stack);
    res.status(500).json({ error: 'An internal server error occurred.' });
  }
});

module.exports = router;