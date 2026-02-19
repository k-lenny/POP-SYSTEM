// server/src/signals/dataProcessor/eqhEql.js
const swingEngine    = require('./swings')
const breakoutEngine = require('./breakouts')

class EqhEqlEngine {
  constructor() {
    this.store           = {}
    this.indexSets       = {}
    this.counts          = {}
    this.lastLevel       = {}
    this.lastActiveLevel = {}
    this.lastSwingCount  = {}
  }

  // ─────────────────────────────────────────────
  // STORE INIT / RESET
  // ─────────────────────────────────────────────

  _initStore(symbol, granularity) {
    if (!this.store[symbol])           this.store[symbol]           = {}
    if (!this.indexSets[symbol])       this.indexSets[symbol]       = {}
    if (!this.counts[symbol])          this.counts[symbol]          = {}
    if (!this.lastLevel[symbol])       this.lastLevel[symbol]       = {}
    if (!this.lastActiveLevel[symbol]) this.lastActiveLevel[symbol] = {}
    if (!this.lastSwingCount[symbol])  this.lastSwingCount[symbol]  = {}

    if (!this.store[symbol][granularity])               this.store[symbol][granularity]               = []
    if (!this.indexSets[symbol][granularity])           this.indexSets[symbol][granularity]           = new Set()
    if (!this.counts[symbol][granularity])              this._resetCounts(symbol, granularity)
    if (this.lastLevel[symbol][granularity]      == null) this.lastLevel[symbol][granularity]         = null
    if (this.lastActiveLevel[symbol][granularity] == null) this.lastActiveLevel[symbol][granularity]  = null
    if (!this.lastSwingCount[symbol][granularity])      this.lastSwingCount[symbol][granularity]      = { highs: 0, lows: 0 }
  }

  _resetCounts(symbol, granularity) {
    if (!this.counts[symbol]) this.counts[symbol] = {}
    this.counts[symbol][granularity] = { eqh: 0, eql: 0, active: 0, broken: 0, swept: 0 }
  }

  // ─────────────────────────────────────────────
  // INDEX / MAP HELPERS
  // ─────────────────────────────────────────────

  // Maps candle.index value → array position.
  // Used for O(1) lookup by candle index value.
  _buildCandleIndexMap(candles) {
    const map = new Map()
    candles.forEach((c, i) => map.set(c.index, i))
    return map
  }

  // Returns the array position of the first candle whose .index value is
  // strictly greater than afterCandleIndex.
  //
  // PRIMARY FIX — previous code used candleIndexMap.get(someIndex + 1) which
  // silently returns undefined whenever candle indices are not perfectly
  // sequential (weekends, missing bars, broker gaps). This helper uses the
  // map to find the known position of afterCandleIndex, then returns pos+1 —
  // which is always the very next candle in the array regardless of how the
  // .index values are spaced.
  _nextArrayIdx(candleIndexMap, candles, afterCandleIndex) {
    const pos = candleIndexMap.get(afterCandleIndex)
    if (pos === undefined) return undefined
    const next = pos + 1
    return next < candles.length ? next : undefined
  }

  _buildSwingIndexMap(allSwings) {
    const map     = new Map()
    const indices = []
    for (const swing of allSwings) {
      map.set(swing.index, swing)
      indices.push(swing.index)
    }
    // swingEngine returns swings in chronological (index-ascending) order already
    return { map, indices }
  }

  // ─────────────────────────────────────────────
  // DUPLICATE / REGISTRATION
  // ─────────────────────────────────────────────

  _isDuplicate(symbol, granularity, key) {
    return this.indexSets[symbol]?.[granularity]?.has(key) || false
  }

  _registerLevel(symbol, granularity, level) {
    if (!this.indexSets[symbol])              this.indexSets[symbol]              = {}
    if (!this.indexSets[symbol][granularity]) this.indexSets[symbol][granularity] = new Set()
    this.indexSets[symbol][granularity].add(level.key)
  }

  _registerLevelCounts(symbol, granularity, level) {
    const c = this.counts[symbol][granularity]
    if (level.type === 'EQH') c.eqh++
    else                      c.eql++
    if      (level.status === 'active') c.active++
    else if (level.status === 'broken') c.broken++
    else if (level.status === 'swept')  c.swept++
  }

  // ─────────────────────────────────────────────
  // CACHE HELPERS
  // ─────────────────────────────────────────────

  _updateLastLevel(symbol, granularity, level) {
    if (!this.lastLevel[symbol]) this.lastLevel[symbol] = {}
    this.lastLevel[symbol][granularity] = level
    if (level.status === 'active') {
      if (!this.lastActiveLevel[symbol]) this.lastActiveLevel[symbol] = {}
      this.lastActiveLevel[symbol][granularity] = level
    }
  }

  _repairCachesIfNeeded(symbol, granularity) {
    const sorted = this.store[symbol]?.[granularity]
    if (!sorted?.length) return
    if (!this.lastLevel[symbol][granularity]) {
      this.lastLevel[symbol][granularity] = sorted[sorted.length - 1]
    }
    if (!this.lastActiveLevel[symbol][granularity]) {
      this.lastActiveLevel[symbol][granularity] = this._findLastActive(symbol, granularity)
    }
  }

  _findLastActive(symbol, granularity) {
    const levels = this.store[symbol]?.[granularity] || []
    for (let i = levels.length - 1; i >= 0; i--) {
      if (levels[i].status === 'active') return levels[i]
    }
    return null
  }

  // ─────────────────────────────────────────────
  // VALIDATE + BUILD (single pass — replaces 3 separate passes)
  // ─────────────────────────────────────────────

  // Walks the candles between firstSwing and secondSwing exactly once:
  //   • checks zone is undisturbed
  //   • confirms a V shape swing exists in the range
  //   • captures vShape extreme metadata
  // Returns null if any validation fails, otherwise the fully built level object.
  _validateAndBuild(type, firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap) {
    const zoneTop    = firstSwing.price
    const zoneBottom = firstSwing.keyPrice

    const startIdx = this._nextArrayIdx(candleIndexMap, candles, firstSwing.index)
    if (startIdx === undefined) return null

    const { map: swingMap, indices: swingIndices } = swingIndexMap
    const vTargetType = type === 'EQH' ? 'low' : 'high'

    let hasCandles     = false
    let hasVShape      = false
    let vExtreme       = type === 'EQH' ? Infinity : -Infinity
    let vExtremeCandle = null
    let candleCount    = 0

    for (let i = startIdx; i < candles.length; i++) {
      const c = candles[i]
      if (c.index >= secondSwing.index) break

      hasCandles = true
      candleCount++

      if (type === 'EQH' && c.high >= zoneBottom) return null
      if (type === 'EQL' && c.low  <= zoneTop)    return null

      if (type === 'EQH' && c.low  < vExtreme) { vExtreme = c.low;  vExtremeCandle = c }
      if (type === 'EQL' && c.high > vExtreme) { vExtreme = c.high; vExtremeCandle = c }
    }

    if (!hasCandles) return null

    for (const idx of swingIndices) {
      if (idx <= firstSwing.index)  continue
      if (idx >= secondSwing.index) break
      if (swingMap.get(idx).type === vTargetType) { hasVShape = true; break }
    }

    if (!hasVShape) return null

    return {
      type,
      key: `${firstSwing.index}_${secondSwing.index}_${type}`,

      zoneTop,
      zoneBottom,
      zoneMid: (zoneTop + zoneBottom) / 2,

      firstSwingIndex:         firstSwing.index,
      firstSwingPrice:         firstSwing.price,
      firstSwingKeyPrice:      firstSwing.keyPrice,
      firstSwingDirection:     firstSwing.direction,
      firstSwingTime:          firstSwing.time,
      firstSwingFormattedTime: firstSwing.formattedTime,

      secondSwingIndex:         secondSwing.index,
      secondSwingPrice:         secondSwing.price,
      secondSwingKeyPrice:      secondSwing.keyPrice,
      secondSwingDirection:     secondSwing.direction,
      secondSwingTime:          secondSwing.time,
      secondSwingFormattedTime: secondSwing.formattedTime,

      vShapeDepth:         vExtreme === Infinity || vExtreme === -Infinity ? null : vExtreme,
      vShapeIndex:         vExtremeCandle?.index         ?? null,
      vShapeTime:          vExtremeCandle?.time          ?? null,
      vShapeFormattedTime: vExtremeCandle?.formattedTime ?? null,
      candlesBetween:      candleCount,

      status:              'active',
      brokenTime:          null,
      brokenFormattedTime: null,
      brokenIndex:         null,
      brokenBy:            null,
      brokenBosType:       null,
      sweptTime:           null,
      sweptFormattedTime:  null,
      sweptIndex:          null,
      sweptBy:             null,

      lastCheckedIndex: null,

      bias:          null,
      formattedTime: secondSwing.formattedTime,
      time:          secondSwing.time,
      date:          secondSwing.date,
    }
  }

  // ─────────────────────────────────────────────
  // BOS TYPE CLASSIFICATION
  // ─────────────────────────────────────────────

  // Scans candles forward from array position startK to determine BOS type.
  //
  // Rules (identical for both _checkLevelStatus and _upgradePendingBosTypes):
  //
  //   BOS_CLOSE     = no candle has closed beyond brokenBy yet, OR a candle
  //                   closed back inside the zone before any confirmation.
  //
  //   BOS_SUSTAINED = any candle (C2, C3, C4 … doesn't matter which) closes
  //                   beyond brokenBy, provided no candle has closed back inside
  //                   the zone beforehand. Gap candles — those that stay above
  //                   the zone (EQH) or below it (EQL) but haven't closed past
  //                   brokenBy — keep the chain alive and are skipped.
  //
  // Example (EQH):
  //   C1 closes above zoneTop  → broken, brokenBy = C1.close
  //   C2 above zone, < brokenBy → gap — chain alive
  //   C3 above zone, < brokenBy → gap — chain alive
  //   C4 closes > brokenBy      → BOS_SUSTAINED ✓
  //
  //   C1 closes above zoneTop  → broken, brokenBy = C1.close
  //   C2 closes ≤ zoneTop       → zone-retreat — chain dead → BOS_CLOSE ✓
  //
  // Returns 'BOS_SUSTAINED' or 'BOS_CLOSE'.
  // If candles run out before a decision, returns 'BOS_CLOSE' (live edge —
  // _upgradePendingBosTypes will re-evaluate on the next tick).
  _classifyBosType(level, candles, startK) {
    if (level.type === 'EQH') {
      for (let k = startK; k < candles.length; k++) {
        const c = candles[k]
        if (c.close <= level.zoneTop)  return 'BOS_CLOSE'     // zone-retreat — chain dead
        if (c.close > level.brokenBy)  return 'BOS_SUSTAINED' // confirmed
        // gap candle (above zone, not past brokenBy) — chain alive, keep scanning
      }
    } else {
      for (let k = startK; k < candles.length; k++) {
        const c = candles[k]
        if (c.close >= level.zoneBottom) return 'BOS_CLOSE'     // zone-retreat — chain dead
        if (c.close < level.brokenBy)    return 'BOS_SUSTAINED' // confirmed
        // gap candle — chain alive, keep scanning
      }
    }
    return 'BOS_CLOSE' // live edge — no candles left, check again next tick
  }

  // ─────────────────────────────────────────────
  // CHECK LEVEL STATUS
  // ─────────────────────────────────────────────

  // Resumes from lastCheckedIndex so each call only processes new candles — O(1)
  // on live updates. Handles active→swept, active→broken, swept→broken transitions.
  //
  // When a break is detected, _classifyBosType is called immediately with all
  // candles already available in the array. If the confirming candle hasn't
  // arrived yet (live edge), BOS_CLOSE is stored temporarily and
  // _upgradePendingBosTypes will promote it on subsequent ticks.
  _checkLevelStatus(level, candles, candleIndexMap) {
    // Resume from the candle after the last one we already checked.
    // Use _nextArrayIdx to safely handle non-sequential candle index values.
    const resumeFrom = level.lastCheckedIndex ?? level.secondSwingIndex
    const startIdx   = this._nextArrayIdx(candleIndexMap, candles, resumeFrom)
    if (startIdx === undefined) return

    for (let i = startIdx; i < candles.length; i++) {
      const candle = candles[i]
      level.lastCheckedIndex = candle.index

      if (level.type === 'EQH') {
        const wickBreaches  = candle.high  > level.zoneTop
        const closeBreaches = candle.close > level.zoneTop

        if (closeBreaches) {
          level.status              = 'broken'
          level.brokenTime          = candle.time
          level.brokenFormattedTime = candle.formattedTime
          level.brokenIndex         = candle.index
          level.brokenBy            = candle.close
          // Classify BOS type using all candles already in the array after the break candle.
          // i+1 is safe here — we are iterating by array position, not candle index value.
          level.brokenBosType = this._classifyBosType(level, candles, i + 1)
          break
        }

        if (level.status === 'active' && wickBreaches) {
          level.status             = 'swept'
          level.sweptTime          = candle.time
          level.sweptFormattedTime = candle.formattedTime
          level.sweptIndex         = candle.index
          level.sweptBy            = candle.high
        }

      } else {
        const wickBreaches  = candle.low   < level.zoneBottom
        const closeBreaches = candle.close < level.zoneBottom

        if (closeBreaches) {
          level.status              = 'broken'
          level.brokenTime          = candle.time
          level.brokenFormattedTime = candle.formattedTime
          level.brokenIndex         = candle.index
          level.brokenBy            = candle.close
          level.brokenBosType = this._classifyBosType(level, candles, i + 1)
          break
        }

        if (level.status === 'active' && wickBreaches) {
          level.status             = 'swept'
          level.sweptTime          = candle.time
          level.sweptFormattedTime = candle.formattedTime
          level.sweptIndex         = candle.index
          level.sweptBy            = candle.low
        }
      }
    }
  }

  // ─────────────────────────────────────────────
  // UPGRADE PENDING BOS_CLOSE → BOS_SUSTAINED
  // ─────────────────────────────────────────────

  // Called every tick from _updateActiveStatuses (outside the active/swept guard
  // so it always runs even when there are zero active or swept levels).
  //
  // For every broken level still carrying BOS_CLOSE, resumes the forward scan
  // from the candle immediately after the break candle using _nextArrayIdx —
  // which correctly handles non-sequential candle index values (weekends, gaps).
  //
  // Applies the same _classifyBosType logic: gap candles keep the chain alive,
  // only a zone-retreat permanently kills it.
  //
  // BOS_SUSTAINED is terminal — once set it never reverts.
  // newLevelKeys excluded — their BOS type was classified at build time.
  _upgradePendingBosTypes(symbol, granularity, candles, candleIndexMap, newLevelKeys) {
    const levels = this.store[symbol]?.[granularity] || []

    for (const level of levels) {
      if (level.status !== 'broken')           continue
      if (level.brokenBosType !== 'BOS_CLOSE') continue
      if (newLevelKeys.has(level.key))         continue

      // _nextArrayIdx finds the array position of the candle immediately after
      // brokenIndex — correctly handles any gap in .index values.
      const startK = this._nextArrayIdx(candleIndexMap, candles, level.brokenIndex)
      if (startK === undefined) continue

      const result = this._classifyBosType(level, candles, startK)
      if (result === 'BOS_SUSTAINED') {
        level.brokenBosType = 'BOS_SUSTAINED'
        console.log(`[EqhEqlEngine] ⬆️  ${level.type} BOS_CLOSE → BOS_SUSTAINED → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop}`)
      }
      // BOS_CLOSE return means either live edge (no candles yet) or zone-retreat
      // already happened — either way leave as BOS_CLOSE
    }
  }

  // ─────────────────────────────────────────────
  // SORTED INSERT
  // ─────────────────────────────────────────────

  _insertSorted(symbol, granularity, level) {
    const arr = this.store[symbol][granularity]
    let lo = 0, hi = arr.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (arr[mid].time <= level.time) lo = mid + 1
      else hi = mid
    }
    arr.splice(lo, 0, level)
  }

  // ─────────────────────────────────────────────
  // DETECT ALL (full scan)
  // ─────────────────────────────────────────────

  detectAll(symbol, granularity, candles) {
    this._initStore(symbol, granularity)

    if (!candles?.length) {
      console.warn(`[EqhEqlEngine] No candles for ${symbol} @ ${granularity}s — detectAll skipped`)
      return []
    }

    this.store[symbol][granularity]           = []
    this.indexSets[symbol][granularity]       = new Set()
    this.lastLevel[symbol][granularity]       = null
    this.lastActiveLevel[symbol][granularity] = null
    this.lastSwingCount[symbol][granularity]  = { highs: 0, lows: 0 }
    this._resetCounts(symbol, granularity)

    const allSwings = swingEngine.get(symbol, granularity)
    const highs     = swingEngine.getHighs(symbol, granularity)
    const lows      = swingEngine.getLows(symbol, granularity)

    if (!allSwings.length) {
      console.warn(`[EqhEqlEngine] No swings for ${symbol} @ ${granularity}s`)
      return []
    }

    const candleIndexMap = this._buildCandleIndexMap(candles)
    const swingIndexMap  = this._buildSwingIndexMap(allSwings)

    // ── EQH ──
    for (let i = 0; i < highs.length - 1; i++) {
      const firstSwing = highs[i]
      for (let j = i + 1; j < highs.length; j++) {
        const secondSwing = highs[j]
        if (secondSwing.price > firstSwing.price)    continue
        if (secondSwing.price < firstSwing.keyPrice) continue

        const key = `${firstSwing.index}_${secondSwing.index}_EQH`
        if (this._isDuplicate(symbol, granularity, key)) continue

        const level = this._validateAndBuild('EQH', firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap)
        if (!level) continue

        level.bias = breakoutEngine.getCurrentBias(symbol, granularity)
        this._checkLevelStatus(level, candles, candleIndexMap)
        this._registerLevelCounts(symbol, granularity, level)
        this.store[symbol][granularity].push(level)
        this._registerLevel(symbol, granularity, level)
      }
    }

    // ── EQL ──
    for (let i = 0; i < lows.length - 1; i++) {
      const firstSwing = lows[i]
      for (let j = i + 1; j < lows.length; j++) {
        const secondSwing = lows[j]
        if (secondSwing.price < firstSwing.price)    continue
        if (secondSwing.price > firstSwing.keyPrice) continue

        const key = `${firstSwing.index}_${secondSwing.index}_EQL`
        if (this._isDuplicate(symbol, granularity, key)) continue

        const level = this._validateAndBuild('EQL', firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap)
        if (!level) continue

        level.bias = breakoutEngine.getCurrentBias(symbol, granularity)
        this._checkLevelStatus(level, candles, candleIndexMap)
        this._registerLevelCounts(symbol, granularity, level)
        this.store[symbol][granularity].push(level)
        this._registerLevel(symbol, granularity, level)
      }
    }

    this.store[symbol][granularity].sort((a, b) => a.time - b.time)

    const sorted = this.store[symbol][granularity]
    if (sorted.length) {
      this.lastLevel[symbol][granularity]       = sorted[sorted.length - 1]
      this.lastActiveLevel[symbol][granularity] = this._findLastActive(symbol, granularity)
    }

    this.lastSwingCount[symbol][granularity] = { highs: highs.length, lows: lows.length }

    const c = this.counts[symbol][granularity]
    console.log(`[EqhEqlEngine] ${symbol} @ ${granularity}s — ${c.eqh + c.eql} levels (EQH: ${c.eqh}, EQL: ${c.eql} | Active: ${c.active}, Broken: ${c.broken}, Swept: ${c.swept})`)
    return this.store[symbol][granularity]
  }

  // ─────────────────────────────────────────────
  // DETECT LATEST (incremental)
  // ─────────────────────────────────────────────

  detectLatest(symbol, granularity, candles) {
    this._initStore(symbol, granularity)

    if (!candles?.length) {
      console.warn(`[EqhEqlEngine] No candles for ${symbol} @ ${granularity}s — detectLatest skipped`)
      return []
    }

    const allSwings = swingEngine.get(symbol, granularity)
    const highs     = swingEngine.getHighs(symbol, granularity)
    const lows      = swingEngine.getLows(symbol, granularity)

    if (!allSwings.length) return []

    const lastCounts    = this.lastSwingCount[symbol][granularity]
    const newHighsStart = lastCounts.highs
    const newLowsStart  = lastCounts.lows

    const candleIndexMap = this._buildCandleIndexMap(candles)
    const swingIndexMap  = this._buildSwingIndexMap(allSwings)
    const newLevels      = []

    // ── New EQH ──
    for (let j = newHighsStart; j < highs.length; j++) {
      const secondSwing = highs[j]
      for (let i = 0; i < j; i++) {
        const firstSwing = highs[i]
        if (secondSwing.price > firstSwing.price)    continue
        if (secondSwing.price < firstSwing.keyPrice) continue

        const key = `${firstSwing.index}_${secondSwing.index}_EQH`
        if (this._isDuplicate(symbol, granularity, key)) continue

        const level = this._validateAndBuild('EQH', firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap)
        if (!level) continue

        level.bias = breakoutEngine.getCurrentBias(symbol, granularity)
        this._checkLevelStatus(level, candles, candleIndexMap)
        this._registerLevelCounts(symbol, granularity, level)
        this._updateLastLevel(symbol, granularity, level)
        this._insertSorted(symbol, granularity, level)
        this._registerLevel(symbol, granularity, level)
        newLevels.push(level)
        console.log(`[EqhEqlEngine] 🔴 New EQH → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop} | V Depth: ${level.vShapeDepth} @ ${level.vShapeFormattedTime} | Status: ${level.status} | ${level.formattedTime}`)
      }
    }

    // ── New EQL ──
    for (let j = newLowsStart; j < lows.length; j++) {
      const secondSwing = lows[j]
      for (let i = 0; i < j; i++) {
        const firstSwing = lows[i]
        if (secondSwing.price < firstSwing.price)    continue
        if (secondSwing.price > firstSwing.keyPrice) continue

        const key = `${firstSwing.index}_${secondSwing.index}_EQL`
        if (this._isDuplicate(symbol, granularity, key)) continue

        const level = this._validateAndBuild('EQL', firstSwing, secondSwing, candles, candleIndexMap, swingIndexMap)
        if (!level) continue

        level.bias = breakoutEngine.getCurrentBias(symbol, granularity)
        this._checkLevelStatus(level, candles, candleIndexMap)
        this._registerLevelCounts(symbol, granularity, level)
        this._updateLastLevel(symbol, granularity, level)
        this._insertSorted(symbol, granularity, level)
        this._registerLevel(symbol, granularity, level)
        newLevels.push(level)
        console.log(`[EqhEqlEngine] 🟢 New EQL → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop} | V Depth: ${level.vShapeDepth} @ ${level.vShapeFormattedTime} | Status: ${level.status} | ${level.formattedTime}`)
      }
    }

    lastCounts.highs = highs.length
    lastCounts.lows  = lows.length

    const newLevelKeys = new Set(newLevels.map((l) => l.key))
    this._updateActiveStatuses(symbol, granularity, candles, candleIndexMap, newLevelKeys)
    this._repairCachesIfNeeded(symbol, granularity)

    return newLevels
  }

  // ─────────────────────────────────────────────
  // UPDATE ACTIVE / SWEPT STATUSES
  // ─────────────────────────────────────────────

  _updateActiveStatuses(symbol, granularity, candles, candleIndexMap, newLevelKeys = new Set()) {
    const levels = this.store[symbol]?.[granularity] || []
    const c      = this.counts[symbol][granularity]

    let activeCacheInvalidated = false

    // Only run the active/swept loop when there is something to check.
    // _upgradePendingBosTypes always runs beneath regardless.
    if (c.active > 0 || c.swept > 0) {
      for (const level of levels) {
        if (level.status === 'broken')   continue
        if (newLevelKeys.has(level.key)) continue

        const prevStatus = level.status
        this._checkLevelStatus(level, candles, candleIndexMap)
        if (level.status === prevStatus) continue

        if (prevStatus === 'active' && level.status === 'swept') {
          c.active--
          c.swept++
          console.log(`[EqhEqlEngine] 🧹 ${level.type} Swept → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop} | ${level.sweptFormattedTime}`)
          if (this.lastActiveLevel[symbol]?.[granularity]?.key === level.key) activeCacheInvalidated = true
        }
        else if (prevStatus === 'active' && level.status === 'broken') {
          c.active--
          c.broken++
          console.log(`[EqhEqlEngine] ❌ ${level.type} Broken → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop} | ${level.brokenFormattedTime}`)
          if (this.lastActiveLevel[symbol]?.[granularity]?.key === level.key) activeCacheInvalidated = true
        }
        else if (prevStatus === 'swept' && level.status === 'broken') {
          c.swept--
          c.broken++
          console.log(`[EqhEqlEngine] ❌ ${level.type} Broken (was Swept) → ${symbol} @ ${granularity}s | Zone: ${level.zoneBottom} - ${level.zoneTop} | ${level.brokenFormattedTime}`)
        }
      }

      if (activeCacheInvalidated) {
        this.lastActiveLevel[symbol][granularity] = this._findLastActive(symbol, granularity)
      }
    }

    // Always runs — upgrades BOS_CLOSE → BOS_SUSTAINED for broken levels
    // whose confirming candle has now arrived, regardless of active/swept count.
    this._upgradePendingBosTypes(symbol, granularity, candles, candleIndexMap, newLevelKeys)
  }

  // ─────────────────────────────────────────────
  // GETTERS
  // ─────────────────────────────────────────────

  get(symbol, granularity)          { return this.store[symbol]?.[granularity] || [] }
  getAll()                           { return this.store }
  getLastN(symbol, granularity, n)  { return this.get(symbol, granularity).slice(-n) }

  getEQH(symbol, granularity)       { return this.get(symbol, granularity).filter((l) => l.type === 'EQH') }
  getEQL(symbol, granularity)       { return this.get(symbol, granularity).filter((l) => l.type === 'EQL') }

  getActive(symbol, granularity)    { return this.get(symbol, granularity).filter((l) => l.status === 'active') }
  getActiveEQH(symbol, granularity) { return this.get(symbol, granularity).filter((l) => l.type === 'EQH' && l.status === 'active') }
  getActiveEQL(symbol, granularity) { return this.get(symbol, granularity).filter((l) => l.type === 'EQL' && l.status === 'active') }

  getBroken(symbol, granularity)    { return this.get(symbol, granularity).filter((l) => l.status === 'broken') }
  getBrokenEQH(symbol, granularity) { return this.get(symbol, granularity).filter((l) => l.type === 'EQH' && l.status === 'broken') }
  getBrokenEQL(symbol, granularity) { return this.get(symbol, granularity).filter((l) => l.type === 'EQL' && l.status === 'broken') }

  getSwept(symbol, granularity)     { return this.get(symbol, granularity).filter((l) => l.status === 'swept') }
  getSweptEQH(symbol, granularity)  { return this.get(symbol, granularity).filter((l) => l.type === 'EQH' && l.status === 'swept') }
  getSweptEQL(symbol, granularity)  { return this.get(symbol, granularity).filter((l) => l.type === 'EQL' && l.status === 'swept') }

  // O(1) via caches
  getLatest(symbol, granularity)       { return this.lastLevel[symbol]?.[granularity]       || null }
  getLatestActive(symbol, granularity) { return this.lastActiveLevel[symbol]?.[granularity] || null }

  getSummary(symbol, granularity) {
    const c = this.counts[symbol]?.[granularity] || { eqh: 0, eql: 0, active: 0, broken: 0, swept: 0 }
    return {
      symbol,
      granularity,
      total:   this.get(symbol, granularity).length,
      eqh:     c.eqh,
      eql:     c.eql,
      active:  c.active,
      broken:  c.broken,
      swept:   c.swept,
      latest:  this.getLatest(symbol, granularity),
      bias:    breakoutEngine.getCurrentBias(symbol, granularity),
    }
  }

  // ─────────────────────────────────────────────
  // CLEAR
  // ─────────────────────────────────────────────

  clearStore(symbol, granularity) {
    if (this.store[symbol])           this.store[symbol][granularity]           = []
    if (this.indexSets[symbol])       this.indexSets[symbol][granularity]       = new Set()
    if (this.counts[symbol])          this._resetCounts(symbol, granularity)
    if (this.lastLevel[symbol])       this.lastLevel[symbol][granularity]       = null
    if (this.lastActiveLevel[symbol]) this.lastActiveLevel[symbol][granularity] = null
    if (this.lastSwingCount[symbol])  this.lastSwingCount[symbol][granularity]  = { highs: 0, lows: 0 }
    console.log(`[EqhEqlEngine] Store cleared → ${symbol} @ ${granularity}s`)
  }

  clearAll() {
    Object.keys(this.store).forEach((symbol) => {
      Object.keys(this.store[symbol]).forEach((g) => {
        this.store[symbol][g]           = []
        this.indexSets[symbol][g]       = new Set()
        this._resetCounts(symbol, g)
        this.lastLevel[symbol][g]       = null
        this.lastActiveLevel[symbol][g] = null
        this.lastSwingCount[symbol][g]  = { highs: 0, lows: 0 }
      })
    })
    console.log(`[EqhEqlEngine] Full store cleared`)
  }
}

module.exports = new EqhEqlEngine()