// server/src/signals/dataProcessor/breakouts.js
const swingEngine = require('./swings')

class BreakoutEngine {
  constructor() {
    this.store          = {}
    this.indexSets      = {}
    this.lastBullishBOS = {}
    this.lastBearishBOS = {}
    this.counts         = {}
  }

  // ── Initialize store ──
  _initStore(symbol, granularity) {
    if (!this.store[symbol])          this.store[symbol]          = {}
    if (!this.indexSets[symbol])      this.indexSets[symbol]      = {}
    if (!this.lastBullishBOS[symbol]) this.lastBullishBOS[symbol] = {}
    if (!this.lastBearishBOS[symbol]) this.lastBearishBOS[symbol] = {}
    if (!this.counts[symbol])         this.counts[symbol]         = {}

    if (!this.store[symbol][granularity])     this.store[symbol][granularity]     = []
    if (!this.indexSets[symbol][granularity]) this.indexSets[symbol][granularity] = new Set()
    if (!this.counts[symbol][granularity])    this._resetCounts(symbol, granularity)
  }

  // ── Reset counts ──
  _resetCounts(symbol, granularity) {
    if (!this.counts[symbol]) this.counts[symbol] = {}
    this.counts[symbol][granularity] = {
      sustained: 0,
      close:     0,
      wick:      0,
      bullish:   0,
      bearish:   0,
      choch:     0,
      bos:       0,
    }
  }

  // ── O(1) duplicate check ──
  _isDuplicate(symbol, granularity, swingIndex, swingType) {
    return this.indexSets[symbol]?.[granularity]?.has(`${swingIndex}_${swingType}`) || false
  }

  // ── Register breakout in Set ──
  _registerBreakout(symbol, granularity, breakout) {
    if (!this.indexSets[symbol])              this.indexSets[symbol]              = {}
    if (!this.indexSets[symbol][granularity]) this.indexSets[symbol][granularity] = new Set()
    this.indexSets[symbol][granularity].add(`${breakout.swingIndex}_${breakout.swingType}`)
  }

  // ── Update counts cache ──
  _updateCounts(symbol, granularity, breakout) {
    if (!this.counts[symbol])              this.counts[symbol]              = {}
    if (!this.counts[symbol][granularity]) this._resetCounts(symbol, granularity)

    const c = this.counts[symbol][granularity]
    if (breakout.bosType === 'BOS_SUSTAINED')  c.sustained++
    if (breakout.bosType === 'BOS_CLOSE')       c.close++
    if (breakout.bosType === 'BOS_WICK')        c.wick++
    if (breakout.breakDirection === 'bullish')  c.bullish++
    if (breakout.breakDirection === 'bearish')  c.bearish++
    if (breakout.isCHoCH)                       c.choch++
    if (!breakout.isCHoCH)                      c.bos++
  }

  // ── Update last BOS cache ──
  _updateLastBOS(symbol, granularity, breakout) {
    if (!this.lastBullishBOS[symbol]) this.lastBullishBOS[symbol] = {}
    if (!this.lastBearishBOS[symbol]) this.lastBearishBOS[symbol] = {}

    if (breakout.breakDirection === 'bullish') {
      this.lastBullishBOS[symbol][granularity] = breakout
    }
    if (breakout.breakDirection === 'bearish') {
      this.lastBearishBOS[symbol][granularity] = breakout
    }
  }

  // ── CHoCH check ──
  _isCHoCH(swing) {
    return (
      (swing.type === 'high' && swing.direction === 'LH') ||
      (swing.type === 'low'  && swing.direction === 'HL')
    )
  }

  // ── Build a breakout object ──
  _buildBreakout(type, swing, breakingCandle, confirmingCandles = []) {
    const isHigh = swing.type === 'high'
    return {
      // ── Classification ──
      bosType:   type,
      isCHoCH:   this._isCHoCH(swing),
      strength:  type === 'BOS_SUSTAINED' ? 3
               : type === 'BOS_CLOSE'     ? 2
               : 1,

      // ── The swing that was broken ──
      swingType:          swing.type,
      swingIndex:         swing.index,
      swingPrice:         swing.price,
      swingKeyPrice:      swing.keyPrice,
      swingDirection:     swing.direction,
      swingTime:          swing.time,
      swingFormattedTime: swing.formattedTime,

      // ── The candle that caused the break ──
      // BOS_SUSTAINED → confirming candle (first close beyond swing level)
      // BOS_CLOSE     → first candle that closed beyond swing level
      // BOS_WICK      → first candle whose wick pierced swing level
      breakingCandleIndex:         breakingCandle.index,
      breakingCandleTime:          breakingCandle.time,
      breakingCandleHigh:          breakingCandle.high,
      breakingCandleLow:           breakingCandle.low,
      breakingCandleClose:         breakingCandle.close,
      breakingCandleFormattedTime: breakingCandle.formattedTime,

      // ── Confirming candles ──
      // BOS_WICK      → []
      // BOS_CLOSE     → []
      // BOS_SUSTAINED → [confirmingCandle, sustainedCandle]
      confirmingCandles: confirmingCandles.map((c) => ({
        index:         c.index,
        time:          c.time,
        close:         c.close,
        formattedTime: c.formattedTime,
      })),

      // ── Market context ──
      breakDirection: isHigh ? 'bullish' : 'bearish',

      // ── Metadata ──
      formattedTime: breakingCandle.formattedTime,
      time:          breakingCandle.time,
      date:          breakingCandle.date,
    }
  }

  // ── Build candle index map for O(1) start index lookup ──
  _buildCandleIndexMap(candles) {
    const map = new Map()
    candles.forEach((c, i) => map.set(c.index, i))
    return map
  }

  // ── Core break detection — single pass O(n) ──
  // BOS_CLOSE     = first candle that closes beyond the swing level
  // BOS_SUSTAINED = subsequent candle that closes beyond the CONFIRMING
  //                 candle's close — not just beyond the swing level
  _checkBreak(swing, candles, candleIndexMap) {
    const isHigh = swing.type === 'high'
    const level  = swing.price

    // O(1) start index lookup
    let startIdx = candleIndexMap.get(swing.index + 1)
    if (startIdx === undefined) {
      startIdx = candles.findIndex((c) => c.index > swing.index)
    }
    if (startIdx === -1 || startIdx === undefined) return null

    let firstWickBOS  = null
    let firstCloseBOS = null  // the confirming candle
    let sustainedBOS  = null  // candle that closed beyond confirming candle's close

    for (let i = startIdx; i < candles.length; i++) {
      const candle      = candles[i]
      const wickBeyond  = isHigh ? candle.high  > level : candle.low   < level
      const closeBeyond = isHigh ? candle.close > level : candle.close < level

      // Track first wick pierce
      if (wickBeyond && !firstWickBOS) firstWickBOS = candle

      // Track first close beyond swing level — this is the confirming candle
      if (closeBeyond && !firstCloseBOS) {
        firstCloseBOS = candle
        continue // move to next candle to check for sustained
      }

      // Once confirming candle found — check subsequent candles
      if (firstCloseBOS) {
        const closedBeyondConfirming = isHigh
          ? candle.close > firstCloseBOS.close
          : candle.close < firstCloseBOS.close

        if (closedBeyondConfirming) {
          sustainedBOS = candle
          break // found sustained — stop immediately
        }

        // Invalidation — price closed back below swing level entirely
        // sustained is no longer possible — stop searching
        const invalidated = isHigh
          ? candle.close < level
          : candle.close > level

        if (invalidated) break
      }
    }

    // Priority — sustained > close > wick
    if (sustainedBOS) {
      return this._buildBreakout(
        'BOS_SUSTAINED',
        swing,
        firstCloseBOS,
        [firstCloseBOS, sustainedBOS]
      )
    }
    if (firstCloseBOS) return this._buildBreakout('BOS_CLOSE', swing, firstCloseBOS)
    if (firstWickBOS)  return this._buildBreakout('BOS_WICK',  swing, firstWickBOS)
    return null
  }

  // ── Upgrade existing breakout if stronger confirmation found ──
  // Never downgrades — only upgrades BOS_WICK → BOS_CLOSE → BOS_SUSTAINED
  _upgradeBreakout(symbol, granularity, swing, candles, candleIndexMap) {
    const existing = this.store[symbol][granularity]
      .find((b) => b.swingIndex === swing.index && b.swingType === swing.type)

    if (!existing) return

    // Already at highest level — nothing to upgrade
    if (existing.bosType === 'BOS_SUSTAINED') return

    const result = this._checkBreak(swing, candles, candleIndexMap)
    if (!result) return

    // Only upgrade — never downgrade
    const currentStrength = existing.strength
    const newStrength     = result.strength
    if (newStrength <= currentStrength) return

    // Update counts — decrement old type increment new type
    const c = this.counts[symbol][granularity]
    if (currentStrength === 1) c.wick--
    if (currentStrength === 2) c.close--
    if (newStrength     === 2) c.close++
    if (newStrength     === 3) c.sustained++

    // Capture old type before overwrite for accurate logging
    const oldBosType = existing.bosType

    // Upgrade in place — preserve swing fields, update break fields only
    Object.assign(existing, {
      bosType:                     result.bosType,
      strength:                    result.strength,
      breakingCandleIndex:         result.breakingCandleIndex,
      breakingCandleTime:          result.breakingCandleTime,
      breakingCandleHigh:          result.breakingCandleHigh,
      breakingCandleLow:           result.breakingCandleLow,
      breakingCandleClose:         result.breakingCandleClose,
      breakingCandleFormattedTime: result.breakingCandleFormattedTime,
      confirmingCandles:           result.confirmingCandles,
      formattedTime:               result.formattedTime,
      time:                        result.time,
      date:                        result.date,
    })

    // Update last BOS cache with upgraded breakout
    this._updateLastBOS(symbol, granularity, existing)

    console.log(`[BreakoutEngine] ⬆️  Upgraded → ${symbol} @ ${granularity}s | ${existing.swingType} at ${existing.swingPrice} | ${oldBosType} → ${existing.bosType} | ${existing.formattedTime}`)
  }

  // ── Full detection run ──
  detectAll(symbol, granularity, candles) {
    this._initStore(symbol, granularity)

    // Full reset before rebuild
    this.store[symbol][granularity]          = []
    this.indexSets[symbol][granularity]      = new Set()
    this.lastBullishBOS[symbol][granularity] = null
    this.lastBearishBOS[symbol][granularity] = null
    this._resetCounts(symbol, granularity)

    const swings = swingEngine.get(symbol, granularity)
    if (!swings.length) {
      console.warn(`[BreakoutEngine] No swings for ${symbol} @ ${granularity}s — run swingEngine.detectAll first`)
      return []
    }

    // Build index map once for entire detection run
    const candleIndexMap = this._buildCandleIndexMap(candles)
    const breakouts      = []

    for (const swing of swings) {
      const result = this._checkBreak(swing, candles, candleIndexMap)
      if (result) {
        breakouts.push(result)
        this.store[symbol][granularity].push(result)
        this._registerBreakout(symbol, granularity, result)
        this._updateCounts(symbol, granularity, result)
        this._updateLastBOS(symbol, granularity, result)
      }
    }

    console.log(`[BreakoutEngine] ${symbol} @ ${granularity}s — ${breakouts.length} breakouts detected`)
    return breakouts
  }

  // ── Incremental detection ──
  detectLatest(symbol, granularity, candles) {
    this._initStore(symbol, granularity)

    const swings = swingEngine.get(symbol, granularity)
    if (!swings.length) return []

    // Build index map once for this run
    const candleIndexMap = this._buildCandleIndexMap(candles)
    const newBreakouts   = []

    for (const swing of swings) {
      if (swing.index >= candles.length - 1) continue

      if (this._isDuplicate(symbol, granularity, swing.index, swing.type)) {
        // Already registered — attempt upgrade if stronger confirmation exists
        this._upgradeBreakout(symbol, granularity, swing, candles, candleIndexMap)
        continue
      }

      const result = this._checkBreak(swing, candles, candleIndexMap)
      if (result) {
        this.store[symbol][granularity].push(result)
        this._registerBreakout(symbol, granularity, result)
        this._updateCounts(symbol, granularity, result)
        this._updateLastBOS(symbol, granularity, result)
        newBreakouts.push(result)
        console.log(`[BreakoutEngine] 🚨 ${result.bosType}${result.isCHoCH ? ' (CHoCH)' : ''} → ${symbol} @ ${granularity}s | ${result.swingType} broken at ${result.swingPrice} | Strength: ${result.strength} | ${result.formattedTime}`)
      }
    }

    return newBreakouts
  }

  // ── Check if a swing is broken ──
  isBroken(symbol, granularity, swingIndex, swingType) {
    return this._isDuplicate(symbol, granularity, swingIndex, swingType)
  }

  // ── Current market bias ──
  getCurrentBias(symbol, granularity) {
    const latest = this.getLatest(symbol, granularity)
    if (!latest) return null
    return {
      bias:          latest.breakDirection,
      isCHoCH:       latest.isCHoCH,
      bosType:       latest.bosType,
      strength:      latest.strength,
      formattedTime: latest.formattedTime,
    }
  }

  // ── Getters ──
  get(symbol, granularity) {
    return this.store[symbol]?.[granularity] || []
  }

  getSustained(symbol, granularity) {
    return this.get(symbol, granularity).filter((b) => b.bosType === 'BOS_SUSTAINED')
  }

  getClose(symbol, granularity) {
    return this.get(symbol, granularity).filter((b) => b.bosType === 'BOS_CLOSE')
  }

  getWick(symbol, granularity) {
    return this.get(symbol, granularity).filter((b) => b.bosType === 'BOS_WICK')
  }

  getBullish(symbol, granularity) {
    return this.get(symbol, granularity).filter((b) => b.breakDirection === 'bullish')
  }

  getBearish(symbol, granularity) {
    return this.get(symbol, granularity).filter((b) => b.breakDirection === 'bearish')
  }

  getCHoCH(symbol, granularity) {
    return this.get(symbol, granularity).filter((b) => b.isCHoCH)
  }

  getBOS(symbol, granularity) {
    return this.get(symbol, granularity).filter((b) => !b.isCHoCH)
  }

  getByStrength(symbol, granularity, minStrength = 2) {
    return this.get(symbol, granularity).filter((b) => b.strength >= minStrength)
  }

  getLastBullishBOS(symbol, granularity) {
    return this.lastBullishBOS[symbol]?.[granularity] || null
  }

  getLastBearishBOS(symbol, granularity) {
    return this.lastBearishBOS[symbol]?.[granularity] || null
  }

  getLatest(symbol, granularity) {
    const breakouts = this.get(symbol, granularity)
    return breakouts[breakouts.length - 1] || null
  }

  getLastN(symbol, granularity, n) {
    return this.get(symbol, granularity).slice(-n)
  }

  getAll() {
    return this.store
  }

  // ── Clear store ──
  clearStore(symbol, granularity) {
    if (this.store[symbol])          this.store[symbol][granularity]          = []
    if (this.indexSets[symbol])      this.indexSets[symbol][granularity]      = new Set()
    if (this.lastBullishBOS[symbol]) this.lastBullishBOS[symbol][granularity] = null
    if (this.lastBearishBOS[symbol]) this.lastBearishBOS[symbol][granularity] = null
    if (this.counts[symbol])         this._resetCounts(symbol, granularity)
    console.log(`[BreakoutEngine] Store cleared → ${symbol} @ ${granularity}s`)
  }

  clearAll() {
    Object.keys(this.store).forEach((symbol) => {
      Object.keys(this.store[symbol]).forEach((g) => {
        this.store[symbol][g] = []
      })
    })
    Object.keys(this.indexSets).forEach((symbol) => {
      Object.keys(this.indexSets[symbol]).forEach((g) => {
        this.indexSets[symbol][g] = new Set()
      })
    })
    Object.keys(this.lastBullishBOS).forEach((symbol) => {
      Object.keys(this.lastBullishBOS[symbol]).forEach((g) => {
        this.lastBullishBOS[symbol][g] = null
      })
    })
    Object.keys(this.lastBearishBOS).forEach((symbol) => {
      Object.keys(this.lastBearishBOS[symbol]).forEach((g) => {
        this.lastBearishBOS[symbol][g] = null
      })
    })
    Object.keys(this.counts).forEach((symbol) => {
      Object.keys(this.counts[symbol]).forEach((g) => {
        this._resetCounts(symbol, g)
      })
    })
    console.log(`[BreakoutEngine] Full store cleared`)
  }

  // ── Summary — O(1) using cached counts ──
  getSummary(symbol, granularity) {
    const c = this.counts[symbol]?.[granularity] || {
      sustained: 0, close: 0, wick: 0,
      bullish: 0, bearish: 0, choch: 0, bos: 0,
    }
    return {
      symbol,
      granularity,
      total:     this.get(symbol, granularity).length,
      sustained: c.sustained,
      close:     c.close,
      wick:      c.wick,
      bullish:   c.bullish,
      bearish:   c.bearish,
      choch:     c.choch,
      bos:       c.bos,
      latest:    this.getLatest(symbol, granularity),
      bias:      this.getCurrentBias(symbol, granularity),
    }
  }
}

module.exports = new BreakoutEngine()