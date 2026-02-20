// server/src/signals/signalEngine.js
const derivWS = require('../broker/derivWebsocket')

// ── All symbols ──
const volatilitySymbols = {
  'Volatility 10': 'R_10',
  'Volatility 10s': '1HZ10V',      // 1s tick
  'Volatility 15s': '1HZ15V',      // 1s tick
  'Volatility 25': 'R_25',
  'Volatility 25s': '1HZ25V',
  'Volatility 30s': '1HZ30V',
  'Volatility 50': 'R_50',
  'Volatility 50s': '1HZ50V',
  'Volatility 75': 'R_75',
  'Volatility 75s': '1HZ75V',
  'Volatility 90s': '1HZ90V',
  'Volatility 100': 'R_100',
  'Volatility 100s': '1HZ100V',
  'Volatility 150s': '1HZ150V',    // 1s tick for 150 level
  'Volatility 250s': '1HZ250V',    // 1s tick for 250 level
}


const timeframes = {
  '1 min': 60,
  '2 min': 120,
  '3 min': 180,
  '5 min': 300,
  '10 min': 600,
  '15 min': 900,
  '30 min': 1800,
  '1 hour': 3600,
  '2 hours': 7200,
  '4 hours': 14400,
  '8 hours': 28800,
  '24 hours': 86400,
}

const DEFAULT_GRANULARITY = 3600

// ── Total subscriptions — always computed fresh ──
const getTotalSubscriptions = () =>
  Object.values(volatilitySymbols).length * Object.values(timeframes).length

// ── One year ago ──
const oneYearAgo = () => Math.floor(Date.now() / 1000) - 365 * 24 * 60 * 60

// ── Candle storage ──
const candleStore = {}

// ── Structure storage ──
const structureStore = {}

// ── Indexed candle cache ──
const indexedCandleCache = {}

// ── Subscription tracking ──
const activeSubscriptions = new Set()

// ── Historical data counter ──
let historicalDataReceived = 0

// ── Guard against duplicate full subscriptions ──
let fullySubscribed = false

// ── External candle-closed handler ──
let _onCandleClosedHandler = null
const setCandleClosedHandler = (fn) => {
  _onCandleClosedHandler = fn
}

// ── Format time ──
const formatTime = (epochTime) => {
  const date = new Date(epochTime * 1000)
  return `${date.toISOString().split('T')[0]} ${date.toISOString().split('T')[1].split('.')[0]}`
}

// ── Candle validation — no NaN or Infinity ──
const isValidCandle = (c) => {
  const fields = ['open', 'high', 'low', 'close']
  return fields.every((f) => {
    const val = parseFloat(c[f])
    return !isNaN(val) && isFinite(val)
  })
}

// ── Candle sanity — high must be highest, low must be lowest ──
const isSaneCandle = (c) => {
  const open  = parseFloat(c.open)
  const high  = parseFloat(c.high)
  const low   = parseFloat(c.low)
  const close = parseFloat(c.close)
  return (
    high >= low   &&
    high >= open  &&
    high >= close &&
    low  <= open  &&
    low  <= close
  )
}

// ── Gap detection ──
const detectGaps = (symbol, granularity) => {
  const candles = candleStore[symbol]?.[granularity] || []
  const gaps = []
  for (let i = 1; i < candles.length; i++) {
    const expected = candles[i - 1].time + granularity
    if (candles[i].time > expected) {
      gaps.push({
        from: candles[i - 1].formattedTime,
        to: candles[i].formattedTime,
        missedCandles: (candles[i].time - expected) / granularity,
      })
    }
  }
  return gaps
}

// ── Subscribe to one symbol ──
const subscribeToCandles = (symbol, granularity) => {
  const key = `${symbol}_${granularity}`
  if (activeSubscriptions.has(key)) return
  activeSubscriptions.add(key)

  console.log(`[SignalEngine] Subscribing → ${symbol} ${granularity}s`)

  derivWS.send({
    ticks_history: symbol,
    granularity:   granularity,
    start:         oneYearAgo(),
    style:         'candles',
    end:           'latest',
    subscribe:     1,
    adjust_start_time: 1,
  })
}

// ── Lazy subscription ──
const lazySubscribe = (symbol, granularity) => {
  const candles = candleStore[symbol]?.[granularity]
  if (!candles || candles.length === 0) {
    subscribeToCandles(symbol, granularity)
  }
}

// ── Subscribe to ALL symbols (full subscription) ──
const subscribeToAllSymbols = () => {
  if (fullySubscribed) return
  fullySubscribed = true

  const allSymbols   = Object.values(volatilitySymbols)
  const allTimeframes = Object.values(timeframes)

  const allCombinations = []
  allSymbols.forEach((symbol) => {
    allTimeframes.forEach((granularity) => {
      allCombinations.push({ symbol, granularity })
    })
  })

  const total = getTotalSubscriptions()
  console.log(`\n[SignalEngine] Subscribing to all combinations → total ${total} subscriptions\n`)

  allCombinations.forEach(({ symbol, granularity }, index) => {
    setTimeout(() => {
      subscribeToCandles(symbol, granularity)
      if ((index + 1) % 10 === 0) console.log(`[SignalEngine] Progress: ${index + 1}/${total}`)
    }, index * 300)
  })
}

// ── Resubscribe all on reconnect ──
const resubscribeAll = () => {
  console.log(`[SignalEngine] Resubscribing all — clearing tracking state...`)
  activeSubscriptions.clear()
  historicalDataReceived = 0
  fullySubscribed = false
  subscribeToAllSymbols()
}

// ── Store historical candles ──
const storeCandles = (symbol, granularity, candles) => {
  if (!candleStore[symbol]) candleStore[symbol] = {}

  const incoming = candles
    .filter((c) => isValidCandle(c) && isSaneCandle(c))
    .map((c) => ({
      open: parseFloat(c.open),
      high: parseFloat(c.high),
      low: parseFloat(c.low),
      close: parseFloat(c.close),
      time: parseInt(c.epoch),
      formattedTime: formatTime(parseInt(c.epoch)),
      date: new Date(parseInt(c.epoch) * 1000).toISOString().split('T')[0],
    }))

  const existing      = candleStore[symbol][granularity] || []
  const existingTimes = new Set(existing.map((c) => c.time))
  const merged        = [...existing, ...incoming.filter((c) => !existingTimes.has(c.time))]
  merged.sort((a, b) => a.time - b.time)

  candleStore[symbol][granularity] = merged
  delete indexedCandleCache[`${symbol}_${granularity}`]

  historicalDataReceived++
  const total   = getTotalSubscriptions()
  const percent = ((historicalDataReceived / total) * 100).toFixed(1)

  const latest = merged[merged.length - 1]
  console.log(`[SignalEngine] ✅ ${symbol} @ ${granularity}s — ${merged.length} candles | Progress: ${historicalDataReceived}/${total} (${percent}%)`)
  console.log(`  Latest → O:${latest.open} H:${latest.high} L:${latest.low} C:${latest.close} | ${latest.formattedTime}`)

  if (historicalDataReceived === total) {
    console.log(`\n[SignalEngine] 🚀 ALL HISTORICAL DATA LOADED — Detection logic is fully armed\n`)
  }
}

// ── Candle closed ──
const onCandleClosed = (symbol, granularity, closedCandle) => {
  if (_onCandleClosedHandler) _onCandleClosedHandler(symbol, granularity, closedCandle)
}

// ── Update live candle ──
const updateLiveCandle = (symbol, granularity, ohlc) => {
  if (!candleStore[symbol]?.[granularity]) return
  if (!isValidCandle(ohlc) || !isSaneCandle(ohlc)) return

  const newCandle = {
    open: parseFloat(ohlc.open),
    high: parseFloat(ohlc.high),
    low: parseFloat(ohlc.low),
    close: parseFloat(ohlc.close),
    time: parseInt(ohlc.open_time),
    formattedTime: formatTime(parseInt(ohlc.open_time)),
    date: new Date(parseInt(ohlc.open_time) * 1000).toISOString().split('T')[0],
  }

  const store = candleStore[symbol][granularity]
  const lastCandle = store[store.length - 1]

  if (lastCandle && lastCandle.time === newCandle.time) {
    store[store.length - 1] = newCandle
    delete indexedCandleCache[`${symbol}_${granularity}`]
  } else {
    if (lastCandle) onCandleClosed(symbol, granularity, lastCandle)
    store.push(newCandle)
    delete indexedCandleCache[`${symbol}_${granularity}`]
  }
}

// ── Start Signal Engine ──
const startSignalEngine = () => {
  derivWS.connect()

  derivWS.onMessage((data) => {
    try {
      if (['open','authorize'].includes(data.msg_type)) {
        subscribeToAllSymbols()
        return
      }
      if (data.msg_type === 'error') {
        console.error(`[SignalEngine] Deriv error → ${data.error?.message}`)
        return
      }
      if (data.msg_type === 'candles' && data.candles) {
        const symbol      = data.echo_req.ticks_history
        const granularity = parseInt(data.echo_req.granularity)
        storeCandles(symbol, granularity, data.candles)
        return
      }
      if (data.msg_type === 'ohlc' && data.ohlc) {
        const ohlc        = data.ohlc
        const symbol      = ohlc.symbol
        const granularity = parseInt(ohlc.granularity)
        updateLiveCandle(symbol, granularity, ohlc)
        return
      }
    } catch (err) {
      console.error(`[SignalEngine] Uncaught error:`, err)
    }
  })

  derivWS.onReconnect(() => {
    console.warn('[SignalEngine] WebSocket reconnecting — resubscribing all...')
    resubscribeAll()
  })
}

// ── Getters with hybrid cache + lazy loading ──
const getCandles = (symbol, granularity, withIndex = false) => {
  lazySubscribe(symbol, granularity) // lazy subscription on first access
  const candles = candleStore[symbol]?.[granularity] || []
  if (!withIndex) return candles

  const key = `${symbol}_${granularity}`
  const cached = indexedCandleCache[key]
  if (cached && cached.length === candles.length) return cached

  const indexed = candles.map((c, i) => ({ ...c, index: i }))
  indexedCandleCache[key] = indexed
  return indexed
}

const getLatestCandle = (symbol, granularity) =>
  getCandles(symbol, granularity).slice(-1)[0] || null

const getAvailableSymbols = () => {
  const loadedSymbols = Object.keys(candleStore)
  const allSymbols    = Object.values(volatilitySymbols)
  const combined      = Array.from(new Set([...allSymbols, ...loadedSymbols]))
  return combined
}


const subscribeSymbol = (symbol, granularity) => subscribeToCandles(symbol, granularity)

const isReady = (symbol, granularity, minCandles = 100) =>
  getCandles(symbol, granularity).length >= minCandles

const isFullyLoaded = () => historicalDataReceived >= getTotalSubscriptions()

const getStoreHealth = () => {
  const health = {}
  for (const symbol of Object.keys(candleStore)) {
    health[symbol] = {}
    for (const granularity of Object.keys(candleStore[symbol])) {
      const candles = candleStore[symbol][granularity]
      const gaps    = detectGaps(symbol, parseInt(granularity))
      health[symbol][granularity] = {
        candles: candles.length,
        gaps: gaps.length,
        gapDetails: gaps,
        oldest: candles[0]?.formattedTime || null,
        latest: candles[candles.length - 1]?.formattedTime || null,
      }
    }
  }
  return health
}

// ── Structure store ──
const updateStructure = (symbol, granularity, type, data) => {
  if (!structureStore[symbol]) structureStore[symbol] = {}
  if (!structureStore[symbol][granularity]) structureStore[symbol][granularity] = {}
  if (!structureStore[symbol][granularity][type]) structureStore[symbol][granularity][type] = []
  structureStore[symbol][granularity][type].push(data)
}

const getStructure = (symbol, granularity, type) =>
  structureStore[symbol]?.[granularity]?.[type] || []

const getFullStructureStore = () => structureStore

module.exports = {
  startSignalEngine,
  getCandles,
  getLatestCandle,
  getAvailableSymbols,
  subscribeSymbol,
  subscribeToAllSymbols,
  setCandleClosedHandler,
  isReady,
  isFullyLoaded,
  getStoreHealth,
  detectGaps,
  updateStructure,
  getStructure,
  getFullStructureStore,
  volatilitySymbols,
  timeframes,
}
