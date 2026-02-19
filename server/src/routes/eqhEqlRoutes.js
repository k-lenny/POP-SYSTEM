// server/src/routes/eqhEqlRoutes.js
const express = require('express')
const router  = express.Router()

const eqhEqlEngine = require('../signals/dataProcessor/eqhEql')
const signalEngine = require('../signals/signalEngine') // added to get candles for redetect

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
async function ensureLevelsLoaded(symbol, granularity) {
  const existing = eqhEqlEngine.get(symbol, granularity)
  if (existing.length === 0) {
    await eqhEqlEngine._loadFromDisk(symbol, granularity)
  }
}

// GET /eqheql/:symbol/:granularity – returns ALL levels (with optional limit)
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)

    const limit = req.query.limit ? parseInt(req.query.limit) : undefined
    let levels = eqhEqlEngine.get(symbol, granularity)
    if (limit && limit > 0) {
      levels = levels.slice(-limit)
    }

    return sendSuccess(res, {
      symbol,
      granularity,
      count: levels.length,
      levels,
    })

  } catch (err) {
    console.error('[EqhEqlRoute] Error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/eqh
router.get('/:symbol/:granularity/eqh', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const levels = eqhEqlEngine.getEQH(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, type: 'EQH', count: levels.length, levels })
  } catch (err) {
    console.error('[EqhEqlRoute] EQH error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/eql
router.get('/:symbol/:granularity/eql', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const levels = eqhEqlEngine.getEQL(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, type: 'EQL', count: levels.length, levels })
  } catch (err) {
    console.error('[EqhEqlRoute] EQL error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/active
router.get('/:symbol/:granularity/active', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const levels = eqhEqlEngine.getActive(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, status: 'active', count: levels.length, levels })
  } catch (err) {
    console.error('[EqhEqlRoute] Active error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/active/eqh
router.get('/:symbol/:granularity/active/eqh', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const levels = eqhEqlEngine.getActiveEQH(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, type: 'EQH', status: 'active', count: levels.length, levels })
  } catch (err) {
    console.error('[EqhEqlRoute] Active EQH error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/active/eql
router.get('/:symbol/:granularity/active/eql', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const levels = eqhEqlEngine.getActiveEQL(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, type: 'EQL', status: 'active', count: levels.length, levels })
  } catch (err) {
    console.error('[EqhEqlRoute] Active EQL error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/broken
router.get('/:symbol/:granularity/broken', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const levels = eqhEqlEngine.getBroken(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, status: 'broken', count: levels.length, levels })
  } catch (err) {
    console.error('[EqhEqlRoute] Broken error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/swept
router.get('/:symbol/:granularity/swept', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const levels = eqhEqlEngine.getSwept(symbol, granularity)

    return sendSuccess(res, { symbol, granularity, status: 'swept', count: levels.length, levels })
  } catch (err) {
    console.error('[EqhEqlRoute] Swept error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/summary
router.get('/:symbol/:granularity/summary', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const summary = eqhEqlEngine.getSummary(symbol, granularity)

    return sendSuccess(res, { summary })
  } catch (err) {
    console.error('[EqhEqlRoute] Summary error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/:symbol/:granularity/latest
router.get('/:symbol/:granularity/latest', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    await ensureLevelsLoaded(symbol, granularity)
    const latest = eqhEqlEngine.getLatest(symbol, granularity)
    const latestActive = eqhEqlEngine.getLatestActive(symbol, granularity)

    return sendSuccess(res, {
      symbol,
      granularity,
      latest,
      latestActive,
    })
  } catch (err) {
    console.error('[EqhEqlRoute] Latest error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// ── POST /eqheql/redetect/:symbol/:granularity – force a full detection (regenerates levels) ──
router.post('/redetect/:symbol/:granularity', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    // Get all candles (with indices) for this symbol/granularity
    const candles = signalEngine.getCandles(symbol, granularity, true)
    if (!candles.length) {
      return sendError(res, 400, 'No candles available for this symbol/granularity')
    }

    // Run detectAll (this will replace the stored levels)
    const levels = await eqhEqlEngine.detectAll(symbol, granularity, candles)

    return sendSuccess(res, {
      message: 'Full re‑detection completed',
      symbol,
      granularity,
      count: levels.length,
    })
  } catch (err) {
    console.error('[EqhEqlRoute] Redetect error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

module.exports = router