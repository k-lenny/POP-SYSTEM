// server/src/routes/eqhEqlRoutes.js
const express = require('express')
const router  = express.Router()

const eqhEqlEngine   = require('../signals/dataProcessor/eqhEql')
const swingEngine    = require('../signals/dataProcessor/swings')
const breakoutEngine = require('../signals/dataProcessor/breakouts')
const signalEngine   = require('../signals/signalEngine')

const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../utils/resolvers')

// ── Helper to run full detection chain ──
const runDetection = (symbol, granularity, candles, strength = 1) => {
  const confirmed = candles.slice(0, -1)
  swingEngine.detectAll(symbol, granularity, confirmed, strength)
  breakoutEngine.detectAll(symbol, granularity, confirmed)
  eqhEqlEngine.detectAll(symbol, granularity, confirmed)
  return confirmed
}


// ── GET /eqheql/:symbol/:granularity ──
router.get('/:symbol/:granularity', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const strength = parseInt(req.query.strength) || 1
    const candles  = signalEngine.getCandles(symbol, granularity, true)

    if (!candles.length) {
      return sendSuccess(res, {
        message: 'No candles loaded yet — wait a few seconds and refresh',
        symbol,
        granularity,
        levels: [],
      })
    }

    const confirmed = runDetection(symbol, granularity, candles, strength)
    const levels    = eqhEqlEngine.get(symbol, granularity)
    const summary   = eqhEqlEngine.getSummary(symbol, granularity)

    return sendSuccess(res, {
      symbol,
      granularity,
      strength,
      candleCount: confirmed.length,
      levelCount:  levels.length,
      summary,
      levels,
    })

  } catch (err) {
    console.error('[EqhEqlRoute] Error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/eqh ──
router.get('/:symbol/:granularity/eqh', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const levels = eqhEqlEngine.getEQH(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, type: 'EQH', count: levels.length, levels })

  } catch (err) {
    console.error('[EqhEqlRoute] EQH error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/eql ──
router.get('/:symbol/:granularity/eql', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const levels = eqhEqlEngine.getEQL(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, type: 'EQL', count: levels.length, levels })

  } catch (err) {
    console.error('[EqhEqlRoute] EQL error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/active ──
router.get('/:symbol/:granularity/active', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const levels = eqhEqlEngine.getActive(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, status: 'active', count: levels.length, levels })

  } catch (err) {
    console.error('[EqhEqlRoute] Active error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/active/eqh ──
router.get('/:symbol/:granularity/active/eqh', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const levels = eqhEqlEngine.getActiveEQH(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, type: 'EQH', status: 'active', count: levels.length, levels })

  } catch (err) {
    console.error('[EqhEqlRoute] Active EQH error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/active/eql ──
router.get('/:symbol/:granularity/active/eql', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const levels = eqhEqlEngine.getActiveEQL(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, type: 'EQL', status: 'active', count: levels.length, levels })

  } catch (err) {
    console.error('[EqhEqlRoute] Active EQL error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/broken ──
router.get('/:symbol/:granularity/broken', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const levels = eqhEqlEngine.getBroken(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, status: 'broken', count: levels.length, levels })

  } catch (err) {
    console.error('[EqhEqlRoute] Broken error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/swept ──
router.get('/:symbol/:granularity/swept', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const levels = eqhEqlEngine.getSwept(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, status: 'swept', count: levels.length, levels })

  } catch (err) {
    console.error('[EqhEqlRoute] Swept error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/summary ──
router.get('/:symbol/:granularity/summary', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    return sendSuccess(res, { summary: eqhEqlEngine.getSummary(symbol, granularity) })

  } catch (err) {
    console.error('[EqhEqlRoute] Summary error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /eqheql/:symbol/:granularity/latest ──
router.get('/:symbol/:granularity/latest', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    return sendSuccess(res, {
      symbol,
      granularity,
      latest:       eqhEqlEngine.getLatest(symbol, granularity),
      latestActive: eqhEqlEngine.getLatestActive(symbol, granularity),
    })

  } catch (err) {
    console.error('[EqhEqlRoute] Latest error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

module.exports = router