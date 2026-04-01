// server/src/signals/dataProcessor/retestEngine.js

const confirmedSetupEngine = require('./confirmedSetup');
const majorSwingsEngine = require('./majorSwings');
const swingEngine = require('./swings');
const signalEngine = require('../signalEngine');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');
const { processOBLV } = require('./OBLV');

class LRUCache {
  constructor(max = 100) {
    this.max = max;
    this.cache = new Map();
  }
  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return undefined;
    entry.lastUsed = Date.now();
    this.cache.set(key, entry); // refresh order
    return entry.value;
  }
  set(key, value) {
    const now = Date.now();
    this.cache.set(key, { value, lastUsed: now });
    if (this.cache.size > this.max) {
      let oldestKey = null, oldestTime = Infinity;
      for (const [k, e] of this.cache) {
        if (e.lastUsed < oldestTime) {
          oldestTime = e.lastUsed;
          oldestKey = k;
        }
      }
      if (oldestKey) this.cache.delete(oldestKey);
    }
  }
  has(key) { return this.cache.has(key); }
  delete(key) { this.cache.delete(key); }
  clear() { this.cache.clear(); }
}

class RetestEngine {
  constructor() {
    // Caches
    this.retestCache = new LRUCache(100);
    this.oblvCache = new LRUCache(100);
    this.candleIndexMaps = new LRUCache(100);
    this.swingExtremaCache = new LRUCache(100);
    this.majorSwingCountsCache = new LRUCache(100);
    this.oteCache = new LRUCache(500); // small OTE cache

    this.oblvStore = {};

    // Constants
    this.PRICE_EPSILON = 1e-8;
    this.OTE_LOWER_RATIO = 0.625;
    this.OTE_UPPER_RATIO = 0.79;
    this.MAJOR_SWING_CLOSE_THRESHOLD = 3;
    this.FRESH_ACTIVATION_MS = 60000;
    this.CONSECUTIVE_CLOSE_THRESHOLD = 2;

    // Tracking
    this.lastProcessedCandle = {};
    this.setupsHash = {};

    // Optional: skip confidence scores to save time
    this.computeConfidenceScores = true; // set to false for maximum speed
  }

  _logDataWarning(method, message, data) { /* optional */ }
  _getCacheKey(symbol, granularity) { return `${symbol}_${granularity}`; }
  _getSetupsHash(setups) {
    if (!setups || !setups.length) return 'empty';
    let hash = '';
    for (let i = 0; i < setups.length; i++) {
      const s = setups[i];
      hash += `${s.setupStatusIndex}_${s.ConfirmedSetupBreakoutStatusIndex}|`;
    }
    return hash;
  }

  // ----------------------------------------------------------------------
  // Incremental candle index map with numeric timestamps
  // ----------------------------------------------------------------------
  _updateCandleIndexMap(symbol, granularity, candles) {
    const key = this._getCacheKey(symbol, granularity);
    let map = this.candleIndexMaps.get(key);
    const lastIdx = candles.length ? candles[candles.length - 1].index : -1;
    const lastProc = this.lastProcessedCandle[key] || -1;

    if (!map) {
      map = new Map();
      for (let i = 0; i < candles.length; i++) {
        const c = candles[i];
        if (c._numericTs === undefined) c._numericTs = new Date(c.formattedTime).getTime();
        map.set(c._numericTs, i);
      }
      this.candleIndexMaps.set(key, map);
      this.lastProcessedCandle[key] = lastIdx;
      return map;
    }

    if (lastProc >= lastIdx) return map;

    // find first new candle
    let start = -1;
    for (let i = 0; i < candles.length; i++) {
      if (candles[i].index > lastProc) { start = i; break; }
    }
    if (start === -1) return map;

    for (let i = start; i < candles.length; i++) {
      const c = candles[i];
      if (c._numericTs === undefined) c._numericTs = new Date(c.formattedTime).getTime();
      map.set(c._numericTs, i);
    }
    this.candleIndexMaps.set(key, map);
    this.lastProcessedCandle[key] = lastIdx;
    return map;
  }

  // ----------------------------------------------------------------------
  // Precomputed swing extrema with position map
  // ----------------------------------------------------------------------
  _precomputeSwingExtrema(swings) {
    const highs = [], lows = [], posMap = new Map();
    for (let i = 0; i < swings.length; i++) {
      const s = swings[i];
      if (s.type === 'high') {
        posMap.set(s.index, { type: 'high', pos: highs.length });
        highs.push(s);
      } else {
        posMap.set(s.index, { type: 'low', pos: lows.length });
        lows.push(s);
      }
    }
    return { highs, lows, posMap };
  }

  _getSwingExtrema(symbol, granularity, swings) {
    const key = this._getCacheKey(symbol, granularity);
    let ext = this.swingExtremaCache.get(key);
    if (!ext) {
      ext = this._precomputeSwingExtrema(swings);
      this.swingExtremaCache.set(key, ext);
    }
    return ext;
  }

  // ----------------------------------------------------------------------
  // Cumulative major swing counts
  // ----------------------------------------------------------------------
  _precomputeMajorCounts(majorSwings) {
    const cumHigh = new Array(majorSwings.length);
    const cumLow = new Array(majorSwings.length);
    let h = 0, l = 0;
    for (let i = 0; i < majorSwings.length; i++) {
      if (majorSwings[i].type === 'high') h++;
      else l++;
      cumHigh[i] = h;
      cumLow[i] = l;
    }
    return { cumHigh, cumLow, totalHigh: h, totalLow: l };
  }

  _getMajorCounts(symbol, granularity, majorSwings) {
    const key = this._getCacheKey(symbol, granularity);
    let counts = this.majorSwingCountsCache.get(key);
    if (!counts) {
      counts = this._precomputeMajorCounts(majorSwings);
      this.majorSwingCountsCache.set(key, counts);
    }
    return counts;
  }

  // ----------------------------------------------------------------------
  // OTE cache
  // ----------------------------------------------------------------------
  _getOTEStatus(retestPrice, mssPrice, nextPrice, isBearish) {
    const key = `${retestPrice}|${mssPrice}|${isBearish}`;
    let range = this.oteCache.get(key);
    if (range === undefined) {
      const high = isBearish ? retestPrice : mssPrice;
      const low = isBearish ? mssPrice : retestPrice;
      if (high <= low) {
        range = null;
      } else {
        const diff = high - low;
        if (isBearish) {
          range = {
            low: low + diff * this.OTE_LOWER_RATIO,
            high: low + diff * this.OTE_UPPER_RATIO
          };
        } else {
          range = {
            low: high - diff * this.OTE_UPPER_RATIO,
            high: high - diff * this.OTE_LOWER_RATIO
          };
        }
      }
      this.oteCache.set(key, range);
    }
    if (range && nextPrice >= range.low && nextPrice <= range.high) return 'OTE';
    return null;
  }

  // ----------------------------------------------------------------------
  // Main entry point
  // ----------------------------------------------------------------------
  getRetests(symbol, granularity) {
    const confirmedSetups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);
    if (!confirmedSetups.length) return [];

    const swings = swingEngine.get(symbol, granularity);
    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length || !swings.length) return [];

    const cacheKey = this._getCacheKey(symbol, granularity);
    const candleIndexMap = this._updateCandleIndexMap(symbol, granularity, candles);

    // Fast cache hit: only update dynamic fields
    if (!this._shouldInvalidateCache(symbol, granularity, candles, confirmedSetups)) {
      const cached = this.retestCache.get(cacheKey);
      if (cached) return this._updateDynamicFields(cached, candles, candleIndexMap);
    }

    // Full recomputation
    if (!this.oblvStore[symbol]) this.oblvStore[symbol] = {};
    const lastCandleTs = candles.length ? candles[candles.length - 1].formattedTime : null;
    const oblvEntry = this.oblvCache.get(cacheKey);
    if (!oblvEntry || oblvEntry.lastUpdate !== lastCandleTs) {
      this.oblvStore[symbol][granularity] = processOBLV(symbol, granularity, candles);
      this.oblvCache.set(cacheKey, { lastUpdate: lastCandleTs });
    }

    const majorSwings = majorSwingsEngine.getMajorSwings(symbol, granularity);
    const swingExtrema = this._getSwingExtrema(symbol, granularity, swings);
    const majorCounts = this._getMajorCounts(symbol, granularity, majorSwings);

    const lastCandle = candles[candles.length - 1];
    const lastCandleTimeMs = lastCandle._numericTs;
    const currentPrice = lastCandle.close;

    const retests = [];

    // Preallocate some reusable objects to avoid allocation in loop
    const prevMSS = { exP: null, exI: null, exT: null, brP: null, brI: null, brT: null };
    const nextMSS = { exP: null, exI: null, exT: null, brP: null, brI: null, brT: null };
    const prevFR = { status: 'WAITING FOR FINAL RETEST', idx: null, time: null };
    const nextFR = { status: 'WAITING FOR FINAL RETEST', idx: null, time: null };

    const oteDoubleSet = new Set(['OTE', 'DOUBLE EQ']);

    for (let idx = 0; idx < confirmedSetups.length; idx++) {
      const setup = confirmedSetups[idx];
      if (setup.ConfirmedSetupBreakoutStatus !== 'YES') continue;

      const isBearish = setup.type === 'EQL';
      const isBuy = !isBearish;

      const retestState = this._findRetestState(setup, candles, swingExtrema, candleIndexMap);
      if (retestState.status === 'FORMED') {
        this._calcMSS(retestState.prevSwing, retestState.retestSwing, isBearish, candles, candleIndexMap, prevMSS);
        this._calcMSS(retestState.retestSwing, retestState.nextSwing, isBearish, candles, candleIndexMap, nextMSS);
        this._finalRetest(prevMSS.exP, prevMSS.brI, isBearish, candles, candleIndexMap, lastCandleTimeMs, prevFR);
        this._finalRetest(nextMSS.exP, nextMSS.brI, isBearish, candles, candleIndexMap, lastCandleTimeMs, nextFR);
      } else {
        // clear objects to avoid stale data
        prevMSS.exP = prevMSS.exI = prevMSS.exT = prevMSS.brP = prevMSS.brI = prevMSS.brT = null;
        nextMSS.exP = nextMSS.exI = nextMSS.exT = nextMSS.brP = nextMSS.brI = nextMSS.brT = null;
        prevFR.status = 'WAITING FOR FINAL RETEST'; prevFR.idx = null; prevFR.time = null;
        nextFR.status = 'WAITING FOR FINAL RETEST'; nextFR.idx = null; nextFR.time = null;
      }

      const nextStatus = this._nextStatus(retestState.retestSwing, retestState.nextSwing, nextMSS, isBearish, candles, candleIndexMap);
      const prevStatus = this._prevStatus(retestState.retestSwing, retestState.prevSwing, isBearish, candles, candleIndexMap);

      // --- NextRetestStatus ---
      let nextRetestStatus = 'PENDING';
      if (nextMSS.exP !== null) {
        const expired = isBearish ? currentPrice < nextMSS.exP : currentPrice > nextMSS.exP;
        if (expired) nextRetestStatus = 'EXPIRED';
        else if (oteDoubleSet.has(nextStatus) && retestState.nextSwing && nextMSS.exP !== null && nextMSS.brI !== null) {
          const minP = Math.min(retestState.nextSwing.price, nextMSS.exP);
          const maxP = Math.max(retestState.nextSwing.price, nextMSS.exP);
          if (currentPrice >= minP && currentPrice <= maxP) nextRetestStatus = 'ACTIVE';
          else if (nextStatus === 'WAITING FOR SETUP' || nextFR.status === 'WAITING FOR FINAL RETEST') nextRetestStatus = 'PENDING';
        } else if (nextStatus === 'WAITING FOR SETUP' || nextFR.status === 'WAITING FOR FINAL RETEST') nextRetestStatus = 'PENDING';
      }

      // --- PreviousRetestStatus ---
      let prevRetestStatus = 'PENDING';
      if (prevMSS.exP !== null) {
        const expired = isBearish ? currentPrice < prevMSS.exP : currentPrice > prevMSS.exP;
        if (expired) prevRetestStatus = 'EXPIRED';
        else if (prevStatus === 'RIGHT S SETUP' && retestState.prevSwing && prevMSS.exP !== null && prevMSS.brI !== null) {
          const minP = Math.min(retestState.prevSwing.price, prevMSS.exP);
          const maxP = Math.max(retestState.prevSwing.price, prevMSS.exP);
          if (currentPrice >= minP && currentPrice <= maxP) prevRetestStatus = 'ACTIVE';
          else if (prevStatus === 'WRONG S SETUP' || prevFR.status === 'WAITING FOR FINAL RETEST') prevRetestStatus = 'PENDING';
        } else if (prevStatus === 'WRONG S SETUP' || prevFR.status === 'WAITING FOR FINAL RETEST') prevRetestStatus = 'PENDING';
      }

      // --- Trade status using precomputed major swing counts ---
      const targetType = isBearish ? 'low' : 'high';
      let majorSwingCount = 0;
      // binary search in majorSwings for first index after impulseExtremeIndex
      let left = 0, right = majorSwings.length - 1, start = majorSwings.length;
      while (left <= right) {
        const mid = (left + right) >> 1;
        if (majorSwings[mid].index > setup.impulseExtremeIndex) {
          start = mid;
          right = mid - 1;
        } else {
          left = mid + 1;
        }
      }
      if (start < majorSwings.length) {
        const counts = majorCounts;
        if (targetType === 'high') {
          majorSwingCount = counts.totalHigh - (start > 0 ? counts.cumHigh[start - 1] : 0);
        } else {
          majorSwingCount = counts.totalLow - (start > 0 ? counts.cumLow[start - 1] : 0);
        }
      }

      const isClosed = majorSwingCount >= this.MAJOR_SWING_CLOSE_THRESHOLD;

      // --- NextTradeStatus & PrevTradeStatus with inline time formatting ---
      let nextTradeStatus, prevTradeStatus;
      if (isClosed) {
        nextTradeStatus = 'CLOSED';
        prevTradeStatus = 'CLOSED';
      } else {
        if (nextStatus === 'OTE' || nextStatus === 'DOUBLE EQ') {
          let activation = null;
          if (retestState.nextSwing && nextMSS.exP !== null && nextMSS.brI !== null) {
            const minP = Math.min(retestState.nextSwing.price, nextMSS.exP);
            const maxP = Math.max(retestState.nextSwing.price, nextMSS.exP);
            for (let i = candles.length - 1; i > candleIndexMap.get(nextMSS.brI); i--) {
              const c = candles[i];
              if (c.high >= minP && c.low <= maxP) {
                activation = c._numericTs;
                break;
              }
            }
          }
          if (activation) {
            const msAgo = lastCandleTimeMs - activation;
            let ago = '';
            if (msAgo < 60000) ago = `${Math.round(msAgo / 1000)} seconds ago`;
            else if (msAgo < 3600000) ago = `${Math.round(msAgo / 60000)} minutes ago`;
            else if (msAgo < 86400000) ago = `${Math.round(msAgo / 3600000)} hours ago`;
            else ago = `${Math.round(msAgo / 86400000)} days ago`;
            nextTradeStatus = `RUNNING (${ago} AGO)`;
          } else {
            nextTradeStatus = 'RUNNING';
          }
        } else {
          nextTradeStatus = 'WAITING';
        }

        if (prevStatus === 'RIGHT S SETUP') {
          let activation = null;
          if (retestState.prevSwing && prevMSS.exP !== null && prevMSS.brI !== null) {
            const minP = Math.min(retestState.prevSwing.price, prevMSS.exP);
            const maxP = Math.max(retestState.prevSwing.price, prevMSS.exP);
            for (let i = candles.length - 1; i > candleIndexMap.get(prevMSS.brI); i--) {
              const c = candles[i];
              if (c.high >= minP && c.low <= maxP) {
                activation = c._numericTs;
                break;
              }
            }
          }
          if (activation) {
            const msAgo = lastCandleTimeMs - activation;
            let ago = '';
            if (msAgo < 60000) ago = `${Math.round(msAgo / 1000)} seconds ago`;
            else if (msAgo < 3600000) ago = `${Math.round(msAgo / 60000)} minutes ago`;
            else if (msAgo < 86400000) ago = `${Math.round(msAgo / 3600000)} hours ago`;
            else ago = `${Math.round(msAgo / 86400000)} days ago`;
            prevTradeStatus = `RUNNING (${ago} AGO)`;
          } else {
            prevTradeStatus = 'RUNNING';
          }
        } else {
          prevTradeStatus = 'WAITING';
        }
      }

      // Confidence scores (optional, skip if not needed)
      let nextConf = 0, prevConf = 0;
      let nextReasons = [], prevReasons = [];
      if (this.computeConfidenceScores && retestState.status === 'FORMED') {
        // Lazy compute OB/mitigation blocks
        let miti = null, ob = null, retMiti = false, retOB = false;
        const ensure = () => {
          if (miti === null) {
            miti = this._findMitigationBlock(setup, isBuy, candles, candleIndexMap);
            ob = this._findOB(setup, isBuy, candles, candleIndexMap);
            retMiti = this._checkBlockRetest(retestState.retestSwing, miti, isBearish, candles, candleIndexMap);
            retOB = this._checkBlockRetest(retestState.retestSwing, ob, isBearish, candles, candleIndexMap);
          }
        };
        if (retMiti || retOB) {
          nextConf++; prevConf++;
          ensure();
          if (retOB) { nextReasons.push('Retest respected Order Block'); prevReasons.push('Retest respected Order Block'); }
          if (retMiti) { nextReasons.push('Retest respected Mitigation Block'); prevReasons.push('Retest respected Mitigation Block'); }
        }
        if ((nextStatus === 'OTE' || nextStatus === 'DOUBLE EQ') && (nextRetestStatus === 'ACTIVE' || nextRetestStatus === 'EXPIRED')) {
          nextConf++; nextReasons.push(`Pattern confirmed: ${nextStatus}`);
        }
        if (prevStatus === 'RIGHT S SETUP' && (prevRetestStatus === 'ACTIVE' || prevRetestStatus === 'EXPIRED')) {
          prevConf++; prevReasons.push('Valid Sweep (Right S Setup)');
        }
        // Fresh activation
        if (retestState.nextSwing && nextMSS.exP !== null && nextMSS.brI !== null) {
          const minP = Math.min(retestState.nextSwing.price, nextMSS.exP);
          const maxP = Math.max(retestState.nextSwing.price, nextMSS.exP);
          for (let i = candles.length - 1; i > candleIndexMap.get(nextMSS.brI); i--) {
            const c = candles[i];
            if (c.high >= minP && c.low <= maxP) {
              if (lastCandleTimeMs - c._numericTs < this.FRESH_ACTIVATION_MS) {
                nextConf++; nextReasons.push('Fresh activation');
              }
              break;
            }
          }
        }
        if (retestState.prevSwing && prevMSS.exP !== null && prevMSS.brI !== null) {
          const minP = Math.min(retestState.prevSwing.price, prevMSS.exP);
          const maxP = Math.max(retestState.prevSwing.price, prevMSS.exP);
          for (let i = candles.length - 1; i > candleIndexMap.get(prevMSS.brI); i--) {
            const c = candles[i];
            if (c.high >= minP && c.low <= maxP) {
              if (lastCandleTimeMs - c._numericTs < this.FRESH_ACTIVATION_MS) {
                prevConf++; prevReasons.push('Fresh activation');
              }
              break;
            }
          }
        }
        if (nextConf > 3) nextConf = 3;
        if (prevConf > 3) prevConf = 3;
      }

      // Build result object without spread to avoid overhead
      const result = {
        ...setup, // unavoidable if we need all setup fields
        signalType: isBearish ? 'SELL' : 'BUY',
        RetestStatus: retestState.status,
        RetestViolationReason: retestState.violationReason || null,
        RetestExtremeSwing: retestState.retestSwing?.price || null,
        RetestExtremeSwingIndex: retestState.retestSwing?.index || null,
        RetestExtremeSwingFormattedTime: retestState.retestSwing?.formattedTime || null,
        PreviousExtremeSwing: retestState.prevSwing?.price || null,
        PreviousExtremeSwingIndex: retestState.prevSwing?.index || null,
        PreviousExtremeSwingFormattedTime: retestState.prevSwing?.formattedTime || null,
        NextExtremeSwing: retestState.nextSwing?.price || null,
        NextExtremeSwingIndex: retestState.nextSwing?.index || null,
        NextExtremeSwingFormattedTime: retestState.nextSwing?.formattedTime || null,
        PreviousMSSExtreme: prevMSS.exP,
        PreviousMSSExtremeIndex: prevMSS.exI,
        PreviousMSSExtremeFormattedTime: prevMSS.exT,
        PreviousMSSbreakout: prevMSS.brP,
        PreviousMSSbreakoutIndex: prevMSS.brI,
        PreviousMSSbreakoutFormattedTime: prevMSS.brT,
        NextMSSExtreme: nextMSS.exP,
        NextMSSExtremeIndex: nextMSS.exI,
        NextMSSExtremeFormattedTime: nextMSS.exT,
        NextMSSbreakout: nextMSS.brP,
        NextMSSbreakoutIndex: nextMSS.brI,
        NextMSSbreakoutFormattedTime: nextMSS.brT,
        NextFinalRetest: nextFR.status,
        NextFinalRetestIndex: nextFR.idx,
        NextFinalRetestFormattedTime: nextFR.time,
        PreviousFinalRetest: prevFR.status,
        PreviousFinalRetestIndex: prevFR.idx,
        PreviousFinalRetestFormattedTime: prevFR.time,
        NextStatus: nextStatus,
        PreviousStatus: prevStatus,
        NextRetestStatus: nextRetestStatus,
        PreviousRetestStatus: prevRetestStatus,
        NextTradeStatus: nextTradeStatus,
        PrevTradeStatus: prevTradeStatus,
        NextConfidenceScore: nextConf,
        PrevConfidenceScore: prevConf,
        NextConfidenceReasons: nextReasons,
        PrevConfidenceReasons: prevReasons,
      };
      retests.push(result);
    }

    this.retestCache.set(cacheKey, retests);
    this.setupsHash[cacheKey] = this._getSetupsHash(confirmedSetups);
    return retests;
  }

  // ----------------------------------------------------------------------
  // Internal helpers (inlined where possible)
  // ----------------------------------------------------------------------
  _shouldInvalidateCache(symbol, granularity, candles, confirmedSetups) {
    const key = this._getCacheKey(symbol, granularity);
    if (!this.retestCache.has(key)) return true;
    const lastIdx = candles.length ? candles[candles.length - 1].index : null;
    if (this.lastProcessedCandle[key] !== lastIdx) return true;
    return this.setupsHash[key] !== this._getSetupsHash(confirmedSetups);
  }

  _updateDynamicFields(cached, candles, map) {
    const lastCandle = candles[candles.length - 1];
    const price = lastCandle.close;
    const lastTime = lastCandle._numericTs;
    for (let i = 0; i < cached.length; i++) {
      const r = cached[i];
      const isBearish = r.type === 'EQL';
      // Next status
      if (r.NextMSSExtreme !== null) {
        let ns = 'PENDING';
        if ((isBearish && price < r.NextMSSExtreme) || (!isBearish && price > r.NextMSSExtreme)) ns = 'EXPIRED';
        else if ((r.NextStatus === 'OTE' || r.NextStatus === 'DOUBLE EQ') && r.NextExtremeSwing && r.NextMSSExtreme !== null && r.NextMSSbreakoutIndex !== null) {
          const minP = Math.min(r.NextExtremeSwing, r.NextMSSExtreme);
          const maxP = Math.max(r.NextExtremeSwing, r.NextMSSExtreme);
          if (price >= minP && price <= maxP) ns = 'ACTIVE';
          else if (r.NextStatus === 'WAITING FOR SETUP' || r.NextFinalRetest === 'WAITING FOR FINAL RETEST') ns = 'PENDING';
        } else if (r.NextStatus === 'WAITING FOR SETUP' || r.NextFinalRetest === 'WAITING FOR FINAL RETEST') ns = 'PENDING';
        r.NextRetestStatus = ns;
      }
      // Previous status
      if (r.PreviousMSSExtreme !== null) {
        let ps = 'PENDING';
        if ((isBearish && price < r.PreviousMSSExtreme) || (!isBearish && price > r.PreviousMSSExtreme)) ps = 'EXPIRED';
        else if (r.PreviousStatus === 'RIGHT S SETUP' && r.PreviousExtremeSwing && r.PreviousMSSExtreme !== null && r.PreviousMSSbreakoutIndex !== null) {
          const minP = Math.min(r.PreviousExtremeSwing, r.PreviousMSSExtreme);
          const maxP = Math.max(r.PreviousExtremeSwing, r.PreviousMSSExtreme);
          if (price >= minP && price <= maxP) ps = 'ACTIVE';
          else if (r.PreviousStatus === 'WRONG S SETUP' || r.PreviousFinalRetest === 'WAITING FOR FINAL RETEST') ps = 'PENDING';
        } else if (r.PreviousStatus === 'WRONG S SETUP' || r.PreviousFinalRetest === 'WAITING FOR FINAL RETEST') ps = 'PENDING';
        r.PreviousRetestStatus = ps;
      }
    }
    return cached;
  }

  _findRetestState(setup, candles, swingExtrema, map) {
    const breakoutIdx = setup.ConfirmedSetupBreakoutStatusIndex;
    const invLevel = setup.setupVshapeDepth;
    const breakLevel = setup.impulseExtremeDepth;
    const isBear = setup.type === 'EQL';
    const target = isBear ? 'high' : 'low';
    const swingsArr = target === 'high' ? swingExtrema.highs : swingExtrema.lows;
    let extreme = null;
    for (let i = 0; i < swingsArr.length; i++) {
      const s = swingsArr[i];
      if (s.index <= breakoutIdx) continue;
      if (!extreme) extreme = s;
      else if (isBear) { if (s.price > extreme.price) extreme = s; }
      else { if (s.price < extreme.price) extreme = s; }
    }
    if (!extreme) return { status: 'WAITING', retestSwing: null, prevSwing: null, nextSwing: null, violationReason: null };
    const retraced = isBear ? extreme.price > breakLevel : extreme.price < breakLevel;
    if (!retraced) return { status: 'WAITING', retestSwing: null, prevSwing: null, nextSwing: null, violationReason: null };
    const inv = this._checkInvalidation(extreme, invLevel, isBear, candles, map);
    if (!inv.isValid) return { status: 'VIOLATED', violationReason: inv.reason, retestSwing: extreme, prevSwing: null, nextSwing: null };
    const posInfo = swingExtrema.posMap.get(extreme.index);
    if (!posInfo) return { status: 'FORMED', retestSwing: extreme, prevSwing: null, nextSwing: null, violationReason: null };
    const arrIdx = posInfo.pos;
    let prevSwing = null;
    for (let i = arrIdx - 1; i >= 0; i--) {
      const cand = swingsArr[i];
      if (cand.index > breakoutIdx) { prevSwing = cand; break; }
    }
    let nextSwing = null;
    for (let i = arrIdx + 1; i < swingsArr.length; i++) {
      nextSwing = swingsArr[i]; break;
    }
    return { status: 'FORMED', retestSwing: extreme, prevSwing, nextSwing, violationReason: null };
  }

  _checkInvalidation(swing, invLevel, isBear, candles, map) {
    const pos = map.get(swing.index);
    if (pos === undefined) return { isValid: false, reason: 'SWING_CANDLE_NOT_FOUND' };
    const c = candles[pos];
    if (isBear) {
      if (c.high <= invLevel + this.PRICE_EPSILON) return { isValid: true };
      if (c.close <= invLevel + this.PRICE_EPSILON) return { isValid: true };
      const nxt = candles[pos + 1];
      if (nxt && nxt.close < invLevel - this.PRICE_EPSILON) return { isValid: true };
      return { isValid: false, reason: 'SUSTAINED_CLOSE_ABOVE_INVALIDATION' };
    } else {
      if (c.low >= invLevel - this.PRICE_EPSILON) return { isValid: true };
      if (c.close >= invLevel - this.PRICE_EPSILON) return { isValid: true };
      const nxt = candles[pos + 1];
      if (nxt && nxt.close > invLevel + this.PRICE_EPSILON) return { isValid: true };
      return { isValid: false, reason: 'SUSTAINED_CLOSE_BELOW_INVALIDATION' };
    }
  }

  _calcMSS(start, end, isBear, candles, map, out) {
    out.exP = out.exI = out.exT = out.brP = out.brI = out.brT = null;
    if (!start || !end) return;
    let extremeCandle = null;
    let extremeVal = isBear ? Infinity : -Infinity;
    const sPos = nextArrayIdx(map, candles, start.index);
    const ePos = map.get(end.index);
    if (sPos === null || ePos === null || sPos >= ePos) return;
    for (let i = sPos; i <= ePos; i++) {
      const c = candles[i];
      if (isBear) {
        if (c.low < extremeVal) { extremeVal = c.low; extremeCandle = c; }
      } else {
        if (c.high > extremeVal) { extremeVal = c.high; extremeCandle = c; }
      }
    }
    if (!extremeCandle) return;
    out.exP = extremeVal;
    out.exI = extremeCandle.index;
    out.exT = extremeCandle.formattedTime;
    const mssPos = map.get(extremeCandle.index);
    if (mssPos !== null) {
      for (let i = mssPos + 1; i < candles.length; i++) {
        const c = candles[i];
        if (isBear) {
          if (c.close < extremeVal - this.PRICE_EPSILON) {
            out.brP = c.close; out.brI = c.index; out.brT = c.formattedTime; break;
          }
        } else {
          if (c.close > extremeVal + this.PRICE_EPSILON) {
            out.brP = c.close; out.brI = c.index; out.brT = c.formattedTime; break;
          }
        }
      }
    }
  }

  _finalRetest(mssExtreme, mssBreakIdx, isBear, candles, map, lastTimeMs, out) {
    out.status = 'WAITING FOR FINAL RETEST';
    out.idx = null; out.time = null;
    if (mssBreakIdx === null || mssExtreme === null) return;
    const breakPos = map.get(mssBreakIdx);
    if (breakPos === null) return;
    let bestCandle = null;
    let bestVal = isBear ? -Infinity : Infinity;
    for (let i = breakPos + 1; i < candles.length; i++) {
      const c = candles[i];
      const retested = isBear ? c.high >= mssExtreme - this.PRICE_EPSILON : c.low <= mssExtreme + this.PRICE_EPSILON;
      if (retested) {
        if (isBear) {
          if (c.high > bestVal) { bestVal = c.high; bestCandle = c; }
        } else {
          if (c.low < bestVal) { bestVal = c.low; bestCandle = c; }
        }
      }
    }
    if (bestCandle) {
      const agoMs = lastTimeMs - bestCandle._numericTs;
      let ago = '';
      if (agoMs < 60000) ago = `${Math.round(agoMs / 1000)} seconds ago`;
      else if (agoMs < 3600000) ago = `${Math.round(agoMs / 60000)} minutes ago`;
      else if (agoMs < 86400000) ago = `${Math.round(agoMs / 3600000)} hours ago`;
      else ago = `${Math.round(agoMs / 86400000)} days ago`;
      out.status = `RETESTED ${ago}`;
      out.idx = bestCandle.index;
      out.time = bestCandle.formattedTime;
    }
  }

  _nextStatus(retestSwing, nextSwing, mss, isBear, candles, map) {
    if (!retestSwing || !nextSwing || !mss || mss.exP === null) return 'WAITING FOR SETUP';
    const ote = this._getOTEStatus(retestSwing.price, mss.exP, nextSwing.price, isBear);
    if (ote) return ote;
    const retestPos = map.get(retestSwing.index);
    if (retestPos !== null) {
      const rc = candles[retestPos];
      const bodyUpper = rc.open > rc.close ? rc.open : rc.close;
      const bodyLower = rc.open < rc.close ? rc.open : rc.close;
      if (isBear) {
        if (nextSwing.price <= rc.high && nextSwing.price >= bodyUpper) return 'DOUBLE EQ';
      } else {
        if (nextSwing.price >= rc.low && nextSwing.price <= bodyLower) return 'DOUBLE EQ';
      }
    }
    return 'WAITING FOR SETUP';
  }

  _prevStatus(retestSwing, prevSwing, isBear, candles, map) {
    if (!retestSwing || !prevSwing) return 'WRONG S SETUP';
    const level = prevSwing.price;
    const retestPos = map.get(retestSwing.index);
    if (retestPos === null) return 'WRONG S SETUP';
    const rc = candles[retestPos];
    if (isBear) { if (rc.high <= level) return 'WRONG S SETUP'; }
    else { if (rc.low >= level) return 'WRONG S SETUP'; }
    const startPos = map.get(prevSwing.index);
    if (startPos === null || startPos >= retestPos) return 'WRONG S SETUP';
    let firstPast = null;
    for (let i = startPos + 1; i <= retestPos; i++) {
      const c = candles[i];
      const closedPast = isBear ? c.close > level : c.close < level;
      if (closedPast && firstPast === null) firstPast = i;
      if (firstPast !== null && i > firstPast) {
        let consec = 0;
        for (let j = i; j <= retestPos; j++) {
          const c2 = candles[j];
          const closedBack = isBear ? c2.close < level : c2.close > level;
          if (closedBack) {
            consec++;
            if (consec >= this.CONSECUTIVE_CLOSE_THRESHOLD) return 'WRONG S SETUP';
          } else break;
        }
      }
    }
    return 'RIGHT S SETUP';
  }

  _findOB(setup, isBuy, candles, map) {
    if (!setup?.symbol || !setup?.granularity) return null;
    const obData = this.oblvStore[setup.symbol]?.[setup.granularity];
    if (!obData?.length) return null;
    const setupIdx = setup.setupStatusIndex;
    const breakoutIdx = setup.ConfirmedSetupBreakoutStatusIndex;
    if (setupIdx == null || breakoutIdx == null || setupIdx >= breakoutIdx) return null;
    for (let i = 0; i < obData.length; i++) {
      const obEntry = obData[i];
      if (!obEntry.OBFormattedTime) continue;
      const ts = new Date(obEntry.OBFormattedTime).getTime();
      const obIdx = map.get(ts);
      if (obIdx === undefined) continue;
      if (obIdx > setupIdx && obIdx < breakoutIdx) {
        const ob = obEntry.OB;
        if (!ob) return null;
        return { high: ob.high, low: ob.low, open: ob.open, close: ob.close };
      }
    }
    return null;
  }

  _findMitigationBlock(setup, isBuy, candles, map) {
    const { preBreakoutVIndex, impulseExtremeIndex } = setup;
    if (preBreakoutVIndex == null || impulseExtremeIndex == null) return null;
    const pos1 = map.get(preBreakoutVIndex);
    const pos2 = map.get(impulseExtremeIndex);
    if (pos1 == null || pos2 == null) return null;
    const start = Math.min(pos1, pos2);
    const end = Math.max(pos1, pos2);
    for (let i = end; i >= start; i--) {
      const c = candles[i];
      if (!c) continue;
      const isGreen = c.close > c.open;
      const isRed = c.close < c.open;
      if ((isBuy && isGreen) || (!isBuy && isRed)) {
        return { high: c.high, low: c.low, formattedTime: c.formattedTime };
      }
    }
    return null;
  }

  _checkBlockRetest(swing, block, isBear, candles, map) {
    if (!swing || !block) return false;
    const pos = map.get(swing.index);
    if (pos === undefined) return false;
    const c = candles[pos];
    if (isBear) {
      if (c.high < block.low) return false;
      if (c.close <= block.high) return true;
      const nxt = candles[pos + 1];
      return !(nxt && nxt.close > block.high);
    } else {
      if (c.low > block.high) return false;
      if (c.close >= block.low) return true;
      const nxt = candles[pos + 1];
      return !(nxt && nxt.close < block.low);
    }
  }

  // Public invalidation methods
  invalidateCache(symbol, granularity) {
    const key = this._getCacheKey(symbol, granularity);
    this.retestCache.delete(key);
    this.candleIndexMaps.delete(key);
    this.swingExtremaCache.delete(key);
    this.majorSwingCountsCache.delete(key);
    delete this.lastProcessedCandle[key];
    delete this.setupsHash[key];
    this.oblvCache.delete(key);
    if (this.oblvStore[symbol]) delete this.oblvStore[symbol][granularity];
  }

  clearAllCaches() {
    this.retestCache.clear();
    this.candleIndexMaps.clear();
    this.swingExtremaCache.clear();
    this.majorSwingCountsCache.clear();
    this.oblvCache.clear();
    this.oblvStore = {};
    this.lastProcessedCandle = {};
    this.setupsHash = {};
  }

  onCandlesUpdate(symbol, granularity) {
    const key = this._getCacheKey(symbol, granularity);
    this.candleIndexMaps.delete(key);
    delete this.lastProcessedCandle[key];
    this.retestCache.delete(key);
    this.oblvCache.delete(key);
    if (this.oblvStore[symbol]) delete this.oblvStore[symbol][granularity];
  }

  onSwingsUpdate(symbol, granularity) {
    const key = this._getCacheKey(symbol, granularity);
    this.swingExtremaCache.delete(key);
    this.retestCache.delete(key);
  }

  onMajorSwingsUpdate(symbol, granularity) {
    const key = this._getCacheKey(symbol, granularity);
    this.majorSwingCountsCache.delete(key);
    this.retestCache.delete(key);
  }

  onSetupsUpdate(symbol, granularity) {
    const key = this._getCacheKey(symbol, granularity);
    this.retestCache.delete(key);
    delete this.setupsHash[key];
  }
}

module.exports = new RetestEngine();