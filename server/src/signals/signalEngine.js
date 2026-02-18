// server/src/signals/signalEngine.js
const derivWS = require('../broker/derivWebsocket')

// ── All symbols exactly from your client code ──
const volatilitySymbols = {
  'Volatility 10':   'R_10',
  'Volatility 10s':  '1HZ10V',
  'Volatility 15s':  '1HZ15V',
  'Volatility 25':   'R_25',
  'Volatility 25s':  '1HZ25V',
  'Volatility 30s':  '1HZ30V',
  'Volatility 50':   'R_50',
  'Volatility 50s':  '1HZ50V',
  'Volatility 75':   'R_75',
  'Volatility 75s':  '1HZ75V',
  'Volatility 90s':  '1HZ90V',
  'Volatility 100':  'R_100',
  'Volatility 100s': '1HZ100V',
}

const timeframes = {
  '1 min':    60,
  '2 min':    120,
  '3 min':    180,
  '5 min':    300,
  '10 min':   600,
  '15 min':   900,
  '30 min':   1800,
  '1 hour':   3600,
  '2 hours':  7200,
  '4 hours':  14400,
  '8 hours':  28800,
  '24 hours': 86400,
}

// ── Default timeframe to subscribe all symbols with ──
const DEFAULT_GRANULARITY = 3600

// ── One year ago ──
const oneYearAgo = () => Math.floor(Date.now() / 1000) - 365 * 24 * 60 * 60

// ── Candle storage ──
// Structure: candleStore['R_10'][3600] = [candles]
const candleStore = {}

// ── Format time ──
const formatTime = (epochTime) => {
  const date = new Date(epochTime * 1000)
  return `${date.toISOString().split('T')[0]} ${date.toISOString().split('T')[1].split('.')[0]}`
}

// ── Subscribe to one symbol ──
const subscribeToCandles = (symbol, granularity) => {
  console.log(`[SignalEngine] Subscribing → ${symbol} ${granularity}s`)

  derivWS.send({
    ticks_history:     symbol,
    granularity:       granularity,
    start:             oneYearAgo(),
    style:             'candles',
    end:               'latest',
    subscribe:         1,
    adjust_start_time: 1,
  })
}

// ── Subscribe to ALL symbols ──
const subscribeToAllSymbols = () => {
  const allSymbols    = Object.values(volatilitySymbols)
  const allTimeframes = Object.values(timeframes)

  // Build every combination
  // 13 symbols × 12 timeframes = 156 total subscriptions
  const allCombinations = []

  allSymbols.forEach((symbol) => {
    allTimeframes.forEach((granularity) => {
      allCombinations.push({ symbol, granularity })
    })
  })

  console.log(`\n[SignalEngine] Subscribing to all combinations...`)
  console.log(`   Symbols:      ${allSymbols.length}`)
  console.log(`   Timeframes:   ${allTimeframes.length}`)
  console.log(`   Total:        ${allCombinations.length} subscriptions`)
  console.log(`   Estimated time: ${((allCombinations.length * 300) / 1000).toFixed(0)} seconds\n`)

  // Send each subscription 300ms apart
  // 156 × 300ms = ~47 seconds total
  // Slow enough to not get rate limited by Deriv
  allCombinations.forEach(({ symbol, granularity }, index) => {
    setTimeout(() => {
      subscribeToCandles(symbol, granularity)

      // Log progress every 10 subscriptions
      if ((index + 1) % 10 === 0) {
        console.log(`[SignalEngine] Progress: ${index + 1}/${allCombinations.length} subscriptions sent`)
      }

      // Log when all done
      if (index + 1 === allCombinations.length) {
        console.log(`\n[SignalEngine] ✅ All ${allCombinations.length} subscriptions sent`)
        console.log(`[SignalEngine] Check Chrome now:\n`)
        console.log(`   http://localhost:4000/api/overview?granularity=60`)
        console.log(`   http://localhost:4000/api/overview?granularity=3600\n`)
      }

    }, index * 300)
  })
}

// ── Store historical candles ──
const storeCandles = (symbol, granularity, candles) => {
  if (!candleStore[symbol]) candleStore[symbol] = {}

  candleStore[symbol][granularity] = candles.map(c => ({
    open:          parseFloat(c.open),
    high:          parseFloat(c.high),
    low:           parseFloat(c.low),
    close:         parseFloat(c.close),
    time:          c.epoch,
    formattedTime: formatTime(c.epoch),
    date:          new Date(c.epoch * 1000).toISOString().split('T')[0],
  }))

  const latest = candleStore[symbol][granularity].slice(-1)[0]

  console.log(`[SignalEngine] ✅ ${symbol} — ${candles.length} candles received`)
  console.log(`   Latest → O:${latest.open} H:${latest.high} L:${latest.low} C:${latest.close} | ${latest.formattedTime}`)
}

// ── Update live candle ──
const updateLiveCandle = (symbol, granularity, ohlc) => {
  if (!candleStore[symbol] || !candleStore[symbol][granularity]) return

  const newCandle = {
    open:          parseFloat(ohlc.open),
    high:          parseFloat(ohlc.high),
    low:           parseFloat(ohlc.low),
    close:         parseFloat(ohlc.close),
    time:          ohlc.open_time,
    formattedTime: formatTime(ohlc.open_time),
    date:          new Date(ohlc.open_time * 1000).toISOString().split('T')[0],
  }

  const store       = candleStore[symbol][granularity]
  const lastCandle  = store[store.length - 1]

  if (lastCandle && lastCandle.time === newCandle.time) {
    // Same candle still forming — update it
    store[store.length - 1] = newCandle
  } else {
    // New candle — add it
    store.push(newCandle)
  }
}

// ── Start Signal Engine ──
const startSignalEngine = () => {
  console.log('[SignalEngine] Starting...')

  derivWS.connect()

  derivWS.onMessage((data) => {

    // No token — connection open
    if (data.msg_type === 'open') {
      subscribeToAllSymbols()
      return
    }

    // Token authorized
    if (data.msg_type === 'authorize') {
      subscribeToAllSymbols()
      return
    }

    // Historical candles received
    if (data.msg_type === 'candles' && data.candles) {
      const symbol      = data.echo_req.ticks_history
      const granularity = parseInt(data.echo_req.granularity)
      storeCandles(symbol, granularity, data.candles)
      return
    }

    // Live candle update
    if (data.msg_type === 'ohlc' && data.ohlc) {
      const ohlc        = data.ohlc
      const symbol      = ohlc.symbol
      const granularity = parseInt(ohlc.granularity)
      updateLiveCandle(symbol, granularity, ohlc)
      return
    }

  })
}

// ── Getters used by routes ──
const getCandles         = (symbol, granularity) => candleStore[symbol]?.[granularity] || []
const getLatestCandle    = (symbol, granularity) => getCandles(symbol, granularity).slice(-1)[0] || null
const getAvailableSymbols = () => Object.keys(candleStore)
const subscribeSymbol    = (symbol, granularity) => subscribeToCandles(symbol, granularity)

module.exports = {
  startSignalEngine,
  getCandles,
  getLatestCandle,
  getAvailableSymbols,
  subscribeSymbol,
  volatilitySymbols,
  timeframes,
}