// server/src/routes/setupRoutes.js
const express = require('express');
const router = express.Router();

const setupEngine = require('../signals/dataProcessor/setup');
const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../utils/resolvers');

router.get('/:symbol/:granularity', (req, res) => {
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

    const setups = setupEngine.getSetups(symbol, granularity);

    return sendSuccess(res, {
      symbol,
      granularity,
      count: setups.length,
      setups,
    });

  } catch (err) {
    console.error('[SetupRoute] Error:', err);
    return sendError(res, 500, 'Internal server error', { message: err.message });
  }
});

module.exports = router;
