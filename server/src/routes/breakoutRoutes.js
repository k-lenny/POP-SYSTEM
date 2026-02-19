// server/src/routes/breakoutRoutes.js
const express = require('express')
const router  = express.Router()

const breakoutEngine = require('../signals/dataProcessor/breakouts')

const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../utils/resolvers')

// Helper: ensure data is loaded from disk for this symbol/granularity
async function ensureBreakoutsLoaded(symbol, granularity) {
  const existing = breakoutEngine.get(symbol, granularity)
  if (existing.length === 0) {
    await breakoutEngine._loadFromDisk(symbol, granularity)
  }
}

// GET /breakouts/:symbol/:granularity – returns ALL breakouts (with optional limit)
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureBreakoutsLoaded(symbol, granularity)

    const limit = req.query.limit ? parseInt(req.query.limit) : undefined
    let breakouts = breakoutEngine.get(symbol, granularity)
    if (limit && limit > 0) {
      breakouts = breakouts.slice(-limit)
    }

    return sendSuccess(res, {
      symbol,
      granularity,
      count: breakouts.length,
      breakouts,
    })

  } catch (err) {
    console.error('[BreakoutRoute] Error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /breakouts/:symbol/:granularity/sustained
router.get('/:symbol/:granularity/sustained', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureBreakoutsLoaded(symbol, granularity)
    const breakouts = breakoutEngine.getSustained(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, type: 'BOS_SUSTAINED', count: breakouts.length, breakouts })
  } catch (err) {
    console.error('[BreakoutRoute] Sustained error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /breakouts/:symbol/:granularity/summary
router.get('/:symbol/:granularity/summary', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureBreakoutsLoaded(symbol, granularity)
    const summary = breakoutEngine.getSummary(symbol, granularity)

    return sendSuccess(res, { summary })
  } catch (err) {
    console.error('[BreakoutRoute] Summary error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /breakouts/:symbol/:granularity/latest
router.get('/:symbol/:granularity/latest', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureBreakoutsLoaded(symbol, granularity)
    const latest = breakoutEngine.getLatest(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, latest })
  } catch (err) {
    console.error('[BreakoutRoute] Latest error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /breakouts/:symbol/:granularity/bullish
router.get('/:symbol/:granularity/bullish', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureBreakoutsLoaded(symbol, granularity)
    const breakouts = breakoutEngine.getBullish(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, direction: 'bullish', count: breakouts.length, breakouts })
  } catch (err) {
    console.error('[BreakoutRoute] Bullish error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /breakouts/:symbol/:granularity/bearish
router.get('/:symbol/:granularity/bearish', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureBreakoutsLoaded(symbol, granularity)
    const breakouts = breakoutEngine.getBearish(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, direction: 'bearish', count: breakouts.length, breakouts })
  } catch (err) {
    console.error('[BreakoutRoute] Bearish error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /breakouts/:symbol/:granularity/choch
router.get('/:symbol/:granularity/choch', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureBreakoutsLoaded(symbol, granularity)
    const breakouts = breakoutEngine.getCHoCH(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, type: 'CHoCH', count: breakouts.length, breakouts })
  } catch (err) {
    console.error('[BreakoutRoute] CHoCH error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /breakouts/:symbol/:granularity/strong
router.get('/:symbol/:granularity/strong', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureBreakoutsLoaded(symbol, granularity)

    const minStrength = parseInt(req.query.minStrength) || 2
    const breakouts = breakoutEngine.getByStrength(symbol, granularity, minStrength)

    return sendSuccess(res, { symbol, granularity, minStrength, count: breakouts.length, breakouts })
  } catch (err) {
    console.error('[BreakoutRoute] Strong error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

module.exports = router