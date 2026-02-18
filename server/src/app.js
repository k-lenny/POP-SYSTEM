// server/src/app.js
require('dotenv').config()
const express = require('express')
const cors    = require('cors')
const {
  startSignalEngine,
  getCandles,
  getLatestCandle,
  getAvailableSymbols,
  subscribeSymbol,
  volatilitySymbols,
  timeframes,
} = require('./signals/signalEngine')

const app  = express()
const PORT = process.env.PORT 

app.use(cors({ origin: '*' }))
app.use(express.json())

// ── Health ──
app.get('/api/health', (req, res) => {
  res.json({
    status:    'ok',
    message:   'POP System server is running',
    timestamp: new Date().toISOString(),
  })
})

// ── All Symbols and Timeframes ──
app.get('/api/symbols', (req, res) => {
  res.json({
    symbols:    volatilitySymbols,
    timeframes: timeframes,
  })
})

// ────────────────────────────────────────────
// ── MAIN ROUTE — See ALL symbols OHLC ──
// Open this in Chrome to see proof of all data
// http://localhost:4000/api/overview
// ────────────────────────────────────────────
app.get('/api/overview', (req, res) => {
  const granularity = parseInt(req.query.granularity) || 3600
  const available   = getAvailableSymbols()

  if (available.length === 0) {
    return res.json({
      message:    'No data yet — server just started, wait 10 seconds and refresh',
      tip:        'Each symbol takes ~500ms to subscribe so all 13 take about 7 seconds',
      granularity,
      symbols:    {},
    })
  }

  // Build overview object with latest OHLC for every symbol
  const overview = {}

  Object.entries(volatilitySymbols).forEach(([name, symbol]) => {
    const latest = getLatestCandle(symbol, granularity)
    const candles = getCandles(symbol, granularity)

    overview[name] = {
      symbol,
      status:        latest ? '✅ Fetching' : '⏳ Waiting',
      totalCandles:  candles.length,
      latest:        latest ? {
        open:  latest.open,
        high:  latest.high,
        low:   latest.low,
        close: latest.close,
        time:  latest.formattedTime,
      } : null,
    }
  })

  res.json({
    granularity,
    symbolsFetched: available.length,
    totalSymbols:   Object.keys(volatilitySymbols).length,
    overview,
  })
})

// ── Get Candles for One Symbol ──
// http://localhost:4000/api/candles/R_100?granularity=3600
app.get('/api/candles/:symbol', (req, res) => {
  const symbol      = req.params.symbol
  const granularity = parseInt(req.query.granularity) || 3600
  const candles     = getCandles(symbol, granularity)
  const latest      = getLatestCandle(symbol, granularity)

  if (candles.length === 0) {
    return res.json({
      message:     `No data yet for ${symbol} — wait a few seconds and refresh`,
      symbol,
      granularity,
      candles:     [],
    })
  }

  res.json({
    symbol,
    granularity,
    totalCandles: candles.length,
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

// ── Subscribe to extra symbol on demand ──
app.post('/api/subscribe', (req, res) => {
  const { symbol, granularity } = req.body

  if (!symbol || !granularity) {
    return res.status(400).json({ error: 'symbol and granularity are required' })
  }

  subscribeSymbol(symbol, granularity)

  res.json({
    message:     `Subscribed to ${symbol} ${granularity}s candles`,
    symbol,
    granularity,
  })
})

// Start everything
startSignalEngine()

// Print all test URLs when server starts
app.listen(PORT, () => {
  console.log(`\n✅ Server running on http://localhost:${PORT}`)
  console.log('\n📌 Open these in Chrome to verify data:\n')

  // Overview with any granularity
  console.log(`   All symbols overview (change granularity in URL):`)
  Object.entries(timeframes).forEach(([name, seconds]) => {
    console.log(`   http://localhost:${PORT}/api/overview?granularity=${seconds}  ← ${name}`)
  })

  console.log('\n   Individual symbols (pick any symbol + any granularity):')
  Object.entries(volatilitySymbols).forEach(([symbolName, symbol]) => {
    console.log(`\n   ${symbolName} (${symbol}):`)
    Object.entries(timeframes).forEach(([tfName, seconds]) => {
      console.log(`     http://localhost:${PORT}/api/candles/${symbol}?granularity=${seconds}  ← ${tfName}`)
    })
  })

  console.log()
})