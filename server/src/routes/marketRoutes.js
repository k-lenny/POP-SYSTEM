// server/src/routes/marketRoutes.js
const express = require('express')
const router  = express.Router()

const {
  getCandles,
  getLatestCandle,
  getAvailableSymbols,
  subscribeSymbol,
  isReady,
  isFullyLoaded,
  getStoreHealth,
  detectGaps,
  getStructure,
  getFullStructureStore,
  volatilitySymbols,
  timeframes,
} = require('../signals/signalEngine')

const {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
} = require('../utils/resolvers')


// ── All Symbols and Timeframes ──
router.get('/symbols', (req, res) => {
  logRequest(req)
  return sendSuccess(res, {
    symbols:         volatilitySymbols,
    timeframes:      timeframes,
    totalSymbols:    Object.keys(volatilitySymbols).length,
    totalTimeframes: Object.keys(timeframes).length,
  })
})


// ── Lightweight Health Check ──
router.get('/health', (req, res) => {
  logRequest(req)
  return sendSuccess(res, {
    fullyLoaded:       isFullyLoaded(),
    symbolsFetched:    getAvailableSymbols().length,
    totalSymbols:      Object.keys(volatilitySymbols).length,
    totalTimeframes:   Object.keys(timeframes).length,
    totalCombinations: Object.keys(volatilitySymbols).length * Object.keys(timeframes).length,
    timestamp:         new Date().toISOString(),
  })
})


// ── Full Store Health ──
router.get('/health/store', (req, res) => {
  logRequest(req)
  return sendSuccess(res, {
    fullyLoaded: isFullyLoaded(),
    store:       getStoreHealth(),
  })
})


// ── Overview Route ──
router.get('/overview', (req, res) => {
  logRequest(req)

  const granularity = resolveGranularity(req.query.granularity || '3600')
  if (!granularity) {
    return sendError(res, 400, `Invalid granularity "${req.query.granularity}"`, {
      validGranularities: getValidGranularities(),
    })
  }

  const available = getAvailableSymbols()

  if (available.length === 0) {
    return sendSuccess(res, {
      message:     'No data yet — server just started, wait 10 seconds and refresh',
      granularity,
      fullyLoaded: false,
      overview:    {},
    })
  }

  const overview = {}

  Object.entries(volatilitySymbols).forEach(([name, symbol]) => {
    const latest  = getLatestCandle(symbol, granularity)
    const candles = getCandles(symbol, granularity)
    const gaps    = detectGaps(symbol, granularity)
    const ready   = isReady(symbol, granularity)

    overview[name] = {
      symbol,
      status:       latest ? '✅ Live' : '⏳ Waiting',
      ready,
      totalCandles: candles.length,
      gaps:         gaps.length,
      oldest:       candles[0]?.formattedTime || null,
      latest:       latest
        ? {
            open:  latest.open,
            high:  latest.high,
            low:   latest.low,
            close: latest.close,
            time:  latest.formattedTime,
          }
        : null,
    }
  })

  return sendSuccess(res, {
    granularity,
    fullyLoaded:    isFullyLoaded(),
    symbolsFetched: available.length,
    totalSymbols:   Object.keys(volatilitySymbols).length,
    overview,
  })
})


// ── Get Candles for One Symbol ──
router.get('/candles/:symbol', (req, res) => {
  logRequest(req)

  const symbol = resolveSymbol(req.params.symbol)
  if (!symbol) {
    return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, {
      validSymbols: getValidSymbols(),
    })
  }

  const granularity = resolveGranularity(req.query.granularity || '3600')
  if (!granularity) {
    return sendError(res, 400, `Invalid granularity "${req.query.granularity}"`, {
      validGranularities: getValidGranularities(),
    })
  }

  const limit  = parseInt(req.query.limit)  || null
  const offset = parseInt(req.query.offset) || 0

  const allCandles = getCandles(symbol, granularity)

  if (allCandles.length === 0) {
    return sendSuccess(res, {
      message:      `No data yet for ${symbol} @ ${granularity}s — wait a few seconds and refresh`,
      symbol,
      granularity,
      ready:        false,
      totalCandles: 0,
      candles:      [],
    })
  }

  const end     = allCandles.length - offset
  const start   = limit ? Math.max(0, end - limit) : 0
  const candles = allCandles.slice(start, end)

  const latest = getLatestCandle(symbol, granularity)
  const gaps   = detectGaps(symbol, granularity)
  const ready  = isReady(symbol, granularity)

  return sendSuccess(res, {
    symbol,
    granularity,
    ready,
    totalCandles: allCandles.length,
    returning:    candles.length,
    offset,
    gaps:         gaps.length,
    gapDetails:   gaps,
    oldest:       allCandles[0]?.formattedTime || null,
    latest: {
      open:  latest.open,
      high:  latest.high,
      low:   latest.low,
      close: latest.close,
      time:  latest.formattedTime,
    },
    candles,
  })
})


// ── Get Structure for One Symbol ──
router.get('/structure/:symbol', (req, res) => {
  logRequest(req)

  const symbol = resolveSymbol(req.params.symbol)
  if (!symbol) {
    return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, {
      validSymbols: getValidSymbols(),
    })
  }

  const granularity = resolveGranularity(req.query.granularity || '3600')
  if (!granularity) {
    return sendError(res, 400, `Invalid granularity "${req.query.granularity}"`, {
      validGranularities: getValidGranularities(),
    })
  }

  const type = req.query.type || null

  if (!isReady(symbol, granularity)) {
    return sendSuccess(res, {
      message:   `${symbol} @ ${granularity}s not ready yet — not enough candles loaded`,
      symbol,
      granularity,
      ready:     false,
      structure: {},
    })
  }

  if (type) {
    const data = getStructure(symbol, granularity, type)
    return sendSuccess(res, {
      symbol,
      granularity,
      type,
      count: data.length,
      data,
    })
  }

  const allStructure    = getFullStructureStore()
  const symbolStructure = allStructure[symbol]?.[granularity] || {}

  return sendSuccess(res, {
    symbol,
    granularity,
    ready:     true,
    types:     Object.keys(symbolStructure),
    structure: symbolStructure,
  })
})


// ── Get Full Structure Store ──
router.get('/structure', (req, res) => {
  logRequest(req)
  return sendSuccess(res, {
    fullyLoaded: isFullyLoaded(),
    structure:   getFullStructureStore(),
  })
})


// ── Subscribe Route ──
router.post('/subscribe', (req, res) => {
  logRequest(req)

  const { symbol: rawSymbol, granularity: rawGranularity } = req.body

  if (!rawSymbol || !rawGranularity) {
    return sendError(res, 400, 'symbol and granularity are required')
  }

  const symbol = resolveSymbol(rawSymbol)
  if (!symbol) {
    return sendError(res, 400, `Invalid symbol "${rawSymbol}"`, {
      validSymbols: getValidSymbols(),
    })
  }

  const granularity = resolveGranularity(rawGranularity)
  if (!granularity) {
    return sendError(res, 400, `Invalid granularity "${rawGranularity}"`, {
      validGranularities: getValidGranularities(),
    })
  }

  subscribeSymbol(symbol, granularity)

  return sendSuccess(res, {
    message:     `Subscribed to ${symbol} @ ${granularity}s candles`,
    symbol,
    granularity,
  })
})


// ── 404 catch for unknown routes ──
router.use((req, res) => {
  logRequest(req)
  return sendError(res, 404, `Route not found: ${req.method} ${req.originalUrl}`, {
    availableRoutes: [
      'GET  /api/symbols',
      'GET  /api/health',
      'GET  /api/health/store',
      'GET  /api/overview?granularity=3600',
      'GET  /api/candles/:symbol?granularity=3600&limit=200&offset=0',
      'GET  /api/structure/:symbol?granularity=3600&type=swingHighs',
      'GET  /api/structure',
      'POST /api/subscribe { symbol, granularity }',
    ],
  })
})

module.exports = router