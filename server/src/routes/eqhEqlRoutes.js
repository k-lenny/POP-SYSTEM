// server/src/routes/eqhEqlRoutes.js
const express = require('express')
const router  = express.Router()

const eqhEqlEngine = require('../signals/dataProcessor/eqhEql')
const swingEngine  = require('../signals/dataProcessor/swings')
const breakoutEngine = require('../signals/dataProcessor/breakouts')
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

// Helper: ensure levels exist in memory for this symbol/granularity.
// If none present, run a detection using candles from the signal engine.
async function ensureLevelsLoaded(symbol, granularity) {
  const existing = eqhEqlEngine.get(symbol, granularity)
  if (existing.length > 0) return existing

  // Attempt to get candles and run a full detection to populate memory.
  try {
    let candles = signalEngine.getCandles(symbol, granularity, true)
    
    // If no candles yet, subscribe and WAIT for them
    if (!candles || candles.length === 0) {
      console.log(`[EqhEqlRoute] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`)
      
      // Ask signalEngine to subscribe
      try { 
        signalEngine.subscribeSymbol(symbol, granularity) 
      } catch (e) {}

      const minCandles = 10
      const timeoutMs = 60000 // Wait up to 60 seconds
      const intervalMs = 1000
      const start = Date.now()
      
      // Wait for candles to arrive
      while (Date.now() - start < timeoutMs) {
        await new Promise(r => setTimeout(r, intervalMs))
        candles = signalEngine.getCandles(symbol, granularity, true)
        
        if (candles && candles.length >= minCandles) {
          console.log(`[EqhEqlRoute] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`)
          break
        }
        
        // Log progress every 5 seconds
        if ((Date.now() - start) % 5000 < 1000) {
          console.log(`[EqhEqlRoute] Still waiting for ${symbol} @ ${granularity}s... (${Date.now() - start}ms elapsed)`)
        }
      }

      // If still no candles after timeout, try more aggressive approach
      if (!candles || candles.length === 0) {
        console.log(`[EqhEqlRoute] Timeout waiting for ${symbol} @ ${granularity}s, trying to force subscription...`)
        
        // Try subscribing to all symbols as a fallback
        if (typeof signalEngine.subscribeToAllSymbols === 'function') {
          try {
            signalEngine.subscribeToAllSymbols()
          } catch (e) {}
        }
        
        // Wait a bit longer
        const extraTimeout = 30000
        const extraStart = Date.now()
        while (Date.now() - extraStart < extraTimeout) {
          await new Promise(r => setTimeout(r, intervalMs))
          candles = signalEngine.getCandles(symbol, granularity, true)
          if (candles && candles.length >= minCandles) break
        }
      }
    }

    // If we have candles now, run detections
    if (candles && candles.length) {
      console.log(`[EqhEqlRoute] Running detections for ${symbol} @ ${granularity}s with ${candles.length} candles`)
      
      // Ensure swings exist
      try {
        const swings = swingEngine.get(symbol, granularity)
        if (!swings.length) {
          await swingEngine.detectAll(symbol, granularity, candles)
        }
      } catch (e) {
        console.error('[EqhEqlRoute] swing detection error:', e)
      }

      // Ensure breakouts exist
      try {
        const breakouts = breakoutEngine.get(symbol, granularity)
        if (!breakouts.length) {
          await breakoutEngine.detectAll(symbol, granularity, candles)
        }
      } catch (e) {
        console.error('[EqhEqlRoute] breakout detection error:', e)
      }

      // Run EQH/EQL detection
      await eqhEqlEngine.detectAll(symbol, granularity, candles)
      
      console.log(`[EqhEqlRoute] Detection complete for ${symbol} @ ${granularity}s`)
    } else {
      console.log(`[EqhEqlRoute] WARNING: No candles available for ${symbol} @ ${granularity}s after all attempts`)
    }
  } catch (err) {
    console.error('[EqhEqlRoute] ensureLevelsLoaded error:', err)
  }
  
  // Return whatever we have now
  return eqhEqlEngine.get(symbol, granularity)
}

// ── STATUS ROUTES (must come BEFORE parameterized routes) ──

// GET /eqheql/loading-status/:granularity – check loading progress
router.get('/loading-status/:granularity', async (req, res) => {
  logRequest(req)
  try {
    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const symbols = Object.values(signalEngine.volatilitySymbols || {})
    
    const status = {}
    let totalCandles = 0
    let symbolsWithData = 0
    
    for (const symbol of symbols) {
      const candles = signalEngine.getCandles(symbol, granularity, true)
      const count = candles.length
      totalCandles += count
      if (count > 0) symbolsWithData++
      
      status[symbol] = {
        candles: count,
        hasData: count > 0,
        latestTime: candles.length > 0 ? candles[candles.length-1]?.formattedTime : null
      }
    }
    
    const totalSymbols = symbols.length
    const percentComplete = ((symbolsWithData / totalSymbols) * 100).toFixed(1)
    
    // Get subscription progress from signalEngine if available
    let subscriptionProgress = null
    if (typeof signalEngine.getSubscriptionProgress === 'function') {
      subscriptionProgress = signalEngine.getSubscriptionProgress()
    }
    
    return sendSuccess(res, {
      granularity,
      totalSymbols,
      symbolsWithData,
      totalCandles,
      percentComplete,
      isFullyLoaded: symbolsWithData === totalSymbols,
      subscriptionProgress,
      status
    })
  } catch (err) {
    console.error('[EqhEqlRoute] loading-status error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/status/:granularity – returns candle counts and readiness per symbol
router.get('/status/:granularity', async (req, res) => {
  logRequest(req)
  try {
    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const symbols = getValidSymbols().map(s => s.code)
    const report = []
    for (const symbol of symbols) {
      try {
        const candles = signalEngine.getCandles(symbol, granularity, true)
        const count = candles.length
        const ready = typeof signalEngine.isReady === 'function' ? signalEngine.isReady(symbol, granularity) : (count >= 100)
        report.push({ symbol, granularity, count, ready })
      } catch (e) {
        report.push({ symbol, granularity, count: 0, ready: false, error: e.message })
      }
    }
    return sendSuccess(res, { granularity, report })
  } catch (err) {
    console.error('[EqhEqlRoute] status error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// ── DEBUG ROUTES ──

// GET /eqheql/debug/candles/:symbol/:granularity – debug candle data
router.get('/debug/candles/:symbol/:granularity', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    // Support multiple ways to specify the two boundary candles:
    let firstIdx = req.query.firstIdx ? parseInt(req.query.firstIdx) : undefined
    let secondIdx = req.query.secondIdx ? parseInt(req.query.secondIdx) : undefined
    const firstSwingIdx = req.query.firstSwingIdx ? parseInt(req.query.firstSwingIdx) : undefined
    const secondSwingIdx = req.query.secondSwingIdx ? parseInt(req.query.secondSwingIdx) : undefined

    if ((firstIdx == null || secondIdx == null) && (firstSwingIdx == null || secondSwingIdx == null)) {
      return sendError(res, 400, 'Provide either firstIdx & secondIdx OR firstSwingIdx & secondSwingIdx query params')
    }

    const candles = signalEngine.getCandles(symbol, granularity, true)
    if (!candles.length) return sendSuccess(res, { symbol, granularity, firstPos: -1, secondPos: -1, slice: [] })

    const indexMap = new Map()
    candles.forEach((c, i) => indexMap.set(c.index, i))

    // If swing indices were provided, resolve them to candle indices
    if (firstSwingIdx != null || secondSwingIdx != null) {
      const swings = swingEngine.get(symbol, granularity) || []
      if (firstSwingIdx != null) {
        const s = swings.find(sw => sw.index === firstSwingIdx || sw.candleIndex === firstSwingIdx)
        if (s) firstIdx = s.candleIndex ?? s.index
      }
      if (secondSwingIdx != null) {
        const s2 = swings.find(sw => sw.index === secondSwingIdx || sw.candleIndex === secondSwingIdx)
        if (s2) secondIdx = s2.candleIndex ?? s2.index
      }
    }

    let firstPos = indexMap.get(firstIdx)
    let secondPos = indexMap.get(secondIdx)
    if (firstPos === undefined) firstPos = candles.findIndex(c => c.time === firstIdx || c.formattedTime === String(firstIdx))
    if (secondPos === undefined) secondPos = candles.findIndex(c => c.time === secondIdx || c.formattedTime === String(secondIdx))

    const start = Math.min(firstPos, secondPos)
    const end = Math.max(firstPos, secondPos)
    const slice = (start >= 0 && end >= 0) ? candles.slice(start, end + 1) : []

    return sendSuccess(res, { symbol, granularity, firstIdx, secondIdx, firstPos, secondPos, slice })
  } catch (err) {
    console.error('[EqhEqlRoute] debug/candles error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// GET /eqheql/debug/progress – quick progress overview
router.get('/debug/progress', async (req, res) => {
  logRequest(req)
  try {
    const symbols = Object.values(signalEngine.volatilitySymbols || {})
    const timeframes = Object.values(signalEngine.timeframes || {})
    const totalSubscriptions = symbols.length * timeframes.length
    
    const progress = {}
    let totalCandles = 0
    let activeSubscriptions = 0
    
    for (const symbol of symbols) {
      progress[symbol] = {}
      for (const gran of timeframes) {
        const candles = signalEngine.getCandles(symbol, gran, true)
        const count = candles.length
        totalCandles += count
        if (count > 0) activeSubscriptions++
        progress[symbol][gran] = count
      }
    }
    
    return sendSuccess(res, {
      totalSubscriptions,
      activeSubscriptions,
      totalCandles,
      percentComplete: ((activeSubscriptions / totalSubscriptions) * 100).toFixed(1),
      progress
    })
  } catch (err) {
    console.error('[EqhEqlRoute] debug/progress error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// ── MAIN DATA ROUTES ──

// GET /eqheql/:symbol/:granularity – returns ALL levels (with optional limit)
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req)
  try {
    let symbol = resolveSymbol(req.params.symbol)
    if (!symbol) {
      // Accept raw code values as well
      const raw = req.params.symbol
      const allCodes = Object.values(signalEngine.volatilitySymbols || {})
      if (allCodes.includes(raw)) symbol = raw
      else {
        // try case-insensitive name match
        const nameKey = Object.keys(signalEngine.volatilitySymbols || {}).find(k => k.toLowerCase() === raw.toLowerCase())
        if (nameKey) symbol = signalEngine.volatilitySymbols[nameKey]
      }
    }
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    // This will now WAIT for data to load
    const levels = await ensureLevelsLoaded(symbol, granularity)

    const limit = req.query.limit ? parseInt(req.query.limit) : undefined
    let resultLevels = levels
    if (limit && limit > 0) {
      resultLevels = levels.slice(-limit)
    }

    return sendSuccess(res, {
      symbol,
      granularity,
      count: resultLevels.length,
      levels: resultLevels,
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

// ── POST /eqheql/redetect/:symbol/:granularity – force a full detection ──
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

// GET /eqheql/all/:granularity – run detection for all valid symbols and return summaries
router.get('/all/:granularity', async (req, res) => {
  logRequest(req)
  try {
    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const expand = req.query.expand === 'true'
    const limit = req.query.limit ? parseInt(req.query.limit) : undefined

    const symbols = getValidSymbols().map(s => s.code)
    const results = []

    for (const symbol of symbols) {
      await ensureLevelsLoaded(symbol, granularity)
      let levels = eqhEqlEngine.get(symbol, granularity)
      const count = levels.length
      if (limit && limit > 0) levels = levels.slice(-limit)
      results.push({ symbol, granularity, count, levels: expand ? levels : undefined })
    }

    return sendSuccess(res, { granularity, symbols: results })
  } catch (err) {
    console.error('[EqhEqlRoute] All-symbols error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

// POST /eqheql/subscribe-all – trigger subscribing to all symbols/timeframes
router.post('/subscribe-all', async (req, res) => {
  logRequest(req)
  try {
    if (typeof signalEngine.subscribeToAllSymbols === 'function') {
      signalEngine.subscribeToAllSymbols()
      return sendSuccess(res, { message: 'Subscription to all symbols started' })
    }
    return sendError(res, 500, 'subscribeToAllSymbols not available on signalEngine')
  } catch (err) {
    console.error('[EqhEqlRoute] subscribe-all error:', err)
    return sendError(res, 500, 'Internal server error', { message: err.message })
  }
})

module.exports = router