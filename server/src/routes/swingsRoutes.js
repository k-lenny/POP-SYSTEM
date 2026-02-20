// server/src/routes/swingsRoutes.js
const express = require('express')
const router  = express.Router()

const swingEngine  = require('../signals/dataProcessor/swings')
const signalEngine = require('../signals/signalEngine')

const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../utils/resolvers')


// ── GET /api/swings/:symbol/:granularity ──
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req)

  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) {
      return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, {
        validSymbols: getValidSymbols(),
      })
    }

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) {
      return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, {
        validGranularities: getValidGranularities(),
      })
    }

    const strength = parseInt(req.query.strength) || 1

    const candles = signalEngine.getCandles(symbol, granularity, true)

    if (!candles.length) {
      return sendSuccess(res, {
        message:     'No candles loaded yet — wait a few seconds and refresh',
        symbol,
        granularity,
        swingCount:  0,
        swings:      [],
      })
    }

    // Exclude forming candle — only run on confirmed closed candles
    const confirmed = candles.slice(0, -1)

    const swings  = await swingEngine.detectAll(symbol, granularity, confirmed, strength)
    const summary = swingEngine.getSummary(symbol, granularity)

    return sendSuccess(res, {
      symbol,
      granularity,
      strength,
      candleCount: confirmed.length,
      swingCount:  swings.length,
      summary,
      swings,
    })

  } catch (err) {
    console.error('[SwingsRoute] Error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /api/swings/:symbol/:granularity/summary ──
router.get('/:symbol/:granularity/summary', (req, res) => {
  logRequest(req)

  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) {
      return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, {
        validSymbols: getValidSymbols(),
      })
    }

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) {
      return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, {
        validGranularities: getValidGranularities(),
      })
    }

    const summary = swingEngine.getSummary(symbol, granularity)

    return sendSuccess(res, { summary })

  } catch (err) {
    console.error('[SwingsRoute] Summary error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})


// ── GET /api/swings/:symbol/:granularity/latest ──
// Returns only the latest swing high and swing low
router.get('/:symbol/:granularity/latest', (req, res) => {
  logRequest(req)

  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) {
      return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, {
        validSymbols: getValidSymbols(),
      })
    }

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) {
      return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, {
        validGranularities: getValidGranularities(),
      })
    }

    return sendSuccess(res, {
      symbol,
      granularity,
      latestHigh: swingEngine.getLatestHigh(symbol, granularity),
      latestLow:  swingEngine.getLatestLow(symbol, granularity),
    })

  } catch (err) {
    console.error('[SwingsRoute] Latest error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// ── GET /api/swings/:symbol/:granularity/debug ──
// Returns swings with their source candle attached for verification
router.get('/:symbol/:granularity/debug', async (req, res) => {
  logRequest(req)

  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const candles = signalEngine.getCandles(symbol, granularity, true)
    if (!candles.length) return sendSuccess(res, { message: 'No candles loaded yet', symbol, granularity, swings: [] })

    const confirmed = candles.slice(0, -1)
    const swings = await swingEngine.detectAll(symbol, granularity, confirmed)

    const swingsWithSource = swings.map(s => ({
      ...s,
      sourceCandle: confirmed[s.candleIndex] || null,
    }))

    return sendSuccess(res, { symbol, granularity, swingCount: swings.length, swings: swingsWithSource })
  } catch (err) {
    console.error('[SwingsRoute debug] Error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

module.exports = router
