// server/src/routes/breakoutRoutes.js
const express = require('express')
const router  = express.Router()

const breakoutEngine = require('../signals/dataProcessor/breakouts')
const swingEngine    = require('../signals/dataProcessor/swings')
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


// ── GET /breakouts/:symbol/:granularity ──
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
        message:    'No candles loaded yet — wait a few seconds and refresh',
        symbol,
        granularity,
        breakouts:  [],
      })
    }

    const confirmed = candles.slice(0, -1)

    swingEngine.detectAll(symbol, granularity, confirmed, strength)
    const breakouts = breakoutEngine.detectAll(symbol, granularity, confirmed)
    const summary   = breakoutEngine.getSummary(symbol, granularity)

    return sendSuccess(res, {
      symbol,
      granularity,
      strength,
      candleCount:   confirmed.length,
      breakoutCount: breakouts.length,
      summary,
      breakouts,
    })

  } catch (err) {
    console.error('[BreakoutRoute] Error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /breakouts/:symbol/:granularity/sustained ──
router.get('/:symbol/:granularity/sustained', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const breakouts = breakoutEngine.getSustained(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, type: 'BOS_SUSTAINED', count: breakouts.length, breakouts })

  } catch (err) {
    console.error('[BreakoutRoute] Sustained error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /breakouts/:symbol/:granularity/summary ──
router.get('/:symbol/:granularity/summary', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    return sendSuccess(res, { summary: breakoutEngine.getSummary(symbol, granularity) })

  } catch (err) {
    console.error('[BreakoutRoute] Summary error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /breakouts/:symbol/:granularity/latest ──
router.get('/:symbol/:granularity/latest', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    return sendSuccess(res, { symbol, granularity, latest: breakoutEngine.getLatest(symbol, granularity) })

  } catch (err) {
    console.error('[BreakoutRoute] Latest error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /breakouts/:symbol/:granularity/bullish ──
router.get('/:symbol/:granularity/bullish', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const breakouts = breakoutEngine.getBullish(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, direction: 'bullish', count: breakouts.length, breakouts })

  } catch (err) {
    console.error('[BreakoutRoute] Bullish error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /breakouts/:symbol/:granularity/bearish ──
router.get('/:symbol/:granularity/bearish', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const breakouts = breakoutEngine.getBearish(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, direction: 'bearish', count: breakouts.length, breakouts })

  } catch (err) {
    console.error('[BreakoutRoute] Bearish error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /breakouts/:symbol/:granularity/choch ──
router.get('/:symbol/:granularity/choch', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const breakouts = breakoutEngine.getCHoCH(symbol, granularity)
    return sendSuccess(res, { symbol, granularity, type: 'CHoCH', count: breakouts.length, breakouts })

  } catch (err) {
    console.error('[BreakoutRoute] CHoCH error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /breakouts/:symbol/:granularity/strong ──
router.get('/:symbol/:granularity/strong', (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const minStrength = parseInt(req.query.minStrength) || 2
    const breakouts   = breakoutEngine.getByStrength(symbol, granularity, minStrength)
    return sendSuccess(res, { symbol, granularity, minStrength, count: breakouts.length, breakouts })

  } catch (err) {
    console.error('[BreakoutRoute] Strong error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

module.exports = router