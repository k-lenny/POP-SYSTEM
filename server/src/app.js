// server/src/app.js

require('dotenv').config()
const express = require('express')
const cors    = require('cors')

const marketRoutes   = require('./routes/marketRoutes')
const swingsRoutes   = require('./routes/swingsRoutes')
const breakoutRoutes = require('./routes/breakoutRoutes')
const eqhEqlRoutes   = require('./routes/eqhEqlRoutes')

const { startSignalEngine } = require('./signals/signalEngine')

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

// ── Start Signal Engine ──
startSignalEngine()

// ── Start Server ──
app.listen(PORT, () => {
  console.log(`✅ Server running on http://localhost:${PORT}`)
})
