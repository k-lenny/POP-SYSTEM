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

// Helper: ensure breakouts exist in memory for this symbol/granularity.
// If none present, run detection using candles from the signal engine.
async function ensureBreakoutsLoaded(symbol, granularity) {
  let breakouts = breakoutEngine.get(symbol, granularity)
  if (breakouts.length > 0) return breakouts

  // No data in memory – run detection
  try {
    let candles = signalEngine.getCandles(symbol, granularity, true)
    
    // If no candles yet, subscribe and WAIT for them
    if (!candles || candles.length === 0) {
      console.log(`[BreakoutRoute] No candles for ${symbol} @ ${granularity}s, subscribing and waiting...`)
      
      try { 
        signalEngine.subscribeSymbol(symbol, granularity) 
      } catch (e) {}

      const minCandles = 10
      const timeoutMs = 60000
      const intervalMs = 1000
      const start = Date.now()
      
      while (Date.now() - start < timeoutMs) {
        await new Promise(r => setTimeout(r, intervalMs))
        candles = signalEngine.getCandles(symbol, granularity, true)
        
        if (candles && candles.length >= minCandles) {
          console.log(`[BreakoutRoute] Got ${candles.length} candles for ${symbol} @ ${granularity}s after ${Date.now() - start}ms`)
          break
        }
        
        if ((Date.now() - start) % 5000 < 1000) {
          console.log(`[BreakoutRoute] Still waiting for ${symbol} @ ${granularity}s... (${Date.now() - start}ms elapsed)`)
        }
      }

      // If still no candles after timeout, try more aggressive approach
      if (!candles || candles.length === 0) {
        console.log(`[BreakoutRoute] Timeout waiting for ${symbol} @ ${granularity}s, trying to force subscription...`)
        
        if (typeof signalEngine.subscribeToAllSymbols === 'function') {
          try {
            signalEngine.subscribeToAllSymbols()
          } catch (e) {}
        }
        
        const extraTimeout = 30000
        const extraStart = Date.now()
        while (Date.now() - extraStart < extraTimeout) {
          await new Promise(r => setTimeout(r, intervalMs))
          candles = signalEngine.getCandles(symbol, granularity, true)
          if (candles && candles.length >= minCandles) break
        }
      }
    }

    if (candles && candles.length) {
      console.log(`[BreakoutRoute] Running detection for ${symbol} @ ${granularity}s with ${candles.length} candles`)
      
      // Ensure swings exist
      try {
        const swings = swingEngine.get(symbol, granularity)
        if (!swings.length) {
          console.log(`[BreakoutRoute] No swings found, detecting swings first...`)
          await swingEngine.detectAll(symbol, granularity, candles)
        }
      } catch (e) {
        console.error('[BreakoutRoute] swing detection error:', e)
      }

      // Run breakout detection
      await breakoutEngine.detectAll(symbol, granularity, candles)
      
      console.log(`[BreakoutRoute] Detection complete for ${symbol} @ ${granularity}s`)
    } else {
      console.log(`[BreakoutRoute] WARNING: No candles available for ${symbol} @ ${granularity}s after all attempts`)
    }
  } catch (err) {
    console.error('[BreakoutRoute] ensureBreakoutsLoaded error:', err)
  }
  
  return breakoutEngine.get(symbol, granularity)
}

// GET /breakouts/:symbol/:granularity – returns ALL breakouts (with optional limit)
router.get('/:symbol/:granularity', async (req, res) => {
  logRequest(req)
  try {
    const symbol = resolveSymbol(req.params.symbol)
    if (!symbol) return sendError(res, 400, `Invalid symbol "${req.params.symbol}"`, { validSymbols: getValidSymbols() })

    const granularity = resolveGranularity(req.params.granularity)
    if (!granularity) return sendError(res, 400, `Invalid granularity "${req.params.granularity}"`, { validGranularities: getValidGranularities() })

    const breakouts = await ensureBreakoutsLoaded(symbol, granularity)

    const limit = req.query.limit ? parseInt(req.query.limit) : undefined
    let resultBreakouts = breakouts
    if (limit && limit > 0) {
      resultBreakouts = breakouts.slice(-limit)
    }

    return sendSuccess(res, {
      symbol,
      granularity,
      count: resultBreakouts.length,
      breakouts: resultBreakouts,
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