// server/src/app.js
require('dotenv').config()
const express = require('express')
const cors    = require('cors')

const marketRoutes   = require('./routes/marketRoutes')
const swingsRoutes   = require('./routes/swingsRoutes')
const breakoutRoutes = require('./routes/breakoutRoutes')
const eqhEqlRoutes   = require('./routes/eqhEqlRoutes')
const setupRoutes    = require('./routes/setupRoutes')
const confirmedSetupRoutes = require('./routes/confirmedSetupRoute')
const retestRoutes   = require('./routes/retestRoutes')
const majorSwingsRoutes = require('./routes/majorSwingsRoutes')
const mitigationBlockRoutes = require('./routes/mitigationBlockRoutes');
const OBLVRoutes = require('./routes/OBLVRoutes');
const LogicRoutes = require('./routes/LogicRoutes');

const { startSignalEngine, isFullyLoaded, getCandles, volatilitySymbols, timeframes, subscribeToAllSymbols } = require('./signals/signalEngine')

// Import engines for loading
const swingEngine    = require('./signals/dataProcessor/swings')
const breakoutEngine = require('./signals/dataProcessor/breakouts')
const eqhEqlEngine   = require('./signals/dataProcessor/eqhEql')

const app  = express()
const PORT = process.env.PORT || 4000

// ── Middleware ──
app.use(cors({ origin: '*' }))
app.use(express.json())

// ── Health Check ──
app.get('/api/health', (req, res) => {
  res.json({
    status:    'ok',
    message:   'POP System server is running',
    timestamp: new Date().toISOString(),
  })
})

// ── Register Routes ──
app.use('/api',        marketRoutes)
app.use('/swings',     swingsRoutes)
app.use('/breakouts',  breakoutRoutes)
app.use('/eqheql',     eqhEqlRoutes)
app.use('/setups',     setupRoutes)
app.use('/confirmed-setups', confirmedSetupRoutes)
app.use('/retests',    retestRoutes)
app.use('/major-swings', majorSwingsRoutes)
app.use('/mitigation-blocks', mitigationBlockRoutes);
app.use('/oblv', OBLVRoutes);
app.use('/logic', LogicRoutes);

// ── List all symbols and granularities from signalEngine ──
const symbols = Object.values(volatilitySymbols)
const granularities = Object.values(timeframes)

// ── Load all persisted data from disk ──
async function loadAllData() {
  console.log('🔄 Loading persisted data from disk...')
  let loadedCount = 0
  for (const symbol of symbols) {
    for (const gran of granularities) {
      try {
        await swingEngine._loadFromDisk(symbol, gran)
        await breakoutEngine._loadFromDisk(symbol, gran)
        await eqhEqlEngine._loadFromDisk(symbol, gran)
        loadedCount++
      } catch (err) {
        // Ignore if file doesn't exist – it will be created later
        if (err.code !== 'ENOENT') {
          console.error(`❌ Error loading ${symbol} @ ${gran}:`, err.message)
        }
      }
    }
  }
  console.log(`✅ Data loading complete. Processed ${loadedCount} symbol/granularity combinations.`)
}

// ── Optional: run a full detection for any symbol/granularity that has zero breakouts ──
async function runInitialFullDetections() {
  console.log('⏳ Waiting for historical data to load...')
  while (!isFullyLoaded()) {
    await new Promise(resolve => setTimeout(resolve, 1000))
  }
  console.log('📊 Running initial full detections for empty stores...')
  for (const symbol of symbols) {
    for (const gran of granularities) {
      const breakouts = breakoutEngine.get(symbol, gran)
      if (breakouts.length === 0) {
        const candles = getCandles(symbol, gran, true) // with index
        if (candles.length) {
          console.log(`   Running detectAll for ${symbol} @ ${gran} (${candles.length} candles)`)
          await swingEngine.detectAll(symbol, gran, candles)
          await breakoutEngine.detectAll(symbol, gran, candles)
          await eqhEqlEngine.detectAll(symbol, gran, candles)
        }
      }
    }
  }
  console.log('✅ Initial detections complete.')
}

// ── Start server after loading data ──
loadAllData().then(() => {
  // Start the live signal engine (receives real‑time candles)
  startSignalEngine()

  // Automatically subscribe to all symbols/timeframes so historical candles are loaded
  try {
    if (typeof subscribeToAllSymbols === 'function') subscribeToAllSymbols()
  } catch (err) {
    console.error('Failed to auto-subscribe to all symbols:', err)
  }

  // Optionally run full detections after history is loaded
  runInitialFullDetections().catch(err => console.error('Initial detection error:', err))

  app.listen(PORT, () => {
    console.log(`✅ Server running on http://localhost:${PORT}`)
  })
})