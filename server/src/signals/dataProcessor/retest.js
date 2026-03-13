// server/src/signals/dataProcessor/retestEngine.js

const confirmedSetupEngine = require('./confirmedSetup');
const majorSwingsEngine = require('./majorSwings');
const swingEngine = require('./swings');
const signalEngine = require('../signalEngine');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');
const { processOBLV } = require('./OBLV');

class RetestEngine {
  constructor() {
    this.oblvStore = {};
    this.oblvCache = {}; // Cache to track last calculated OBLV data
    this.PRICE_EPSILON = 1e-8; // Tolerance for floating-point price comparisons
    
    // Trading Constants
    this.OTE_LOWER_RATIO = 0.625;
    this.OTE_UPPER_RATIO = 0.79;
    this.MAJOR_SWING_CLOSE_THRESHOLD = 3;
    this.FRESH_ACTIVATION_MS = 60000;
    this.CONSECUTIVE_CLOSE_THRESHOLD = 2;
    
    // Event-driven cache
    this.retestCache = {}; // Cache computed retests by symbol_granularity
    this.lastProcessedCandle = {}; // Track last candle index processed
    this.setupsHash = {}; // Track changes in confirmed setups
  }
/**
   * Log data integrity warnings
   * @private
   */
  _logDataWarning(method, message, data = {}) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn(`[RetestEngine.${method}] ${message}`, data);
    }
  }

  /**
   * Generate cache key for symbol/granularity pair
   * @private
   */
  _getCacheKey(symbol, granularity) {
    return `${symbol}_${granularity}`;
  }

  /**
   * Generate hash of setup IDs to detect changes
   * @private
   */
  _getSetupsHash(setups) {
    if (!setups || setups.length === 0) return 'empty';
    return setups
      .map(s => `${s.setupStatusIndex}_${s.ConfirmedSetupBreakoutStatusIndex}`)
      .sort()
      .join('|');
  }
  /**
   * Finds the OB zone based on all detected OBs between setupVshapeIndex and breakout.
   * Returns { high, low } of OB zone.
   * @private
   */
_findOB(setup, isBuy, candles, candleIndexMap) {
  if (!setup?.symbol || !setup?.granularity) return null;

  const obData = this.oblvStore?.[setup.symbol]?.[setup.granularity];

  if (!obData?.length) return null;

  const setupIndex = setup.setupStatusIndex;
  const breakoutIndex = setup.ConfirmedSetupBreakoutStatusIndex;

  if (setupIndex == null || breakoutIndex == null || setupIndex >= breakoutIndex) {
    return null;
  }

  // 🔥 Find FIRST OB after setup (single candle only)
  for (const obEntry of obData) {
    if (!obEntry.OBFormattedTime) continue;

    const obIndex = candleIndexMap.get(
      new Date(obEntry.OBFormattedTime).getTime() / 1000
    );

    if (obIndex == null) continue;

    if (obIndex > setupIndex && obIndex < breakoutIndex) {
      const ob = obEntry.OB;

      if (!ob) return null;

      return {
        high: ob.high,
        low: ob.low,
        open: ob.open,
        close: ob.close,
      };
    }
  }

  return null;
}
_findMitigationBlock(setup, isBuy, candles, candleIndexMap) {
    const { preBreakoutVIndex, impulseExtremeIndex } = setup;
    if (preBreakoutVIndex == null || impulseExtremeIndex == null) return null;

    const pos1 = candleIndexMap.get(preBreakoutVIndex);
    const pos2 = candleIndexMap.get(impulseExtremeIndex);
    if (pos1 == null || pos2 == null) return null;

    const startPos = Math.min(pos1, pos2);
    const endPos = Math.max(pos1, pos2);

    for (let i = endPos; i >= startPos; i--) {
      const candle = candles[i];
      if (!candle) continue;
      const isGreen = candle.close > candle.open;
      const isRed = candle.close < candle.open;
      if ((isBuy && isGreen) || (!isBuy && isRed)) {
        return { high: candle.high, low: candle.low, formattedTime: candle.formattedTime };
      }
    }
    return null;
  }

_checkBlockRetest(swing, block, isBearish, candles, candleIndexMap) {
    if (!swing || !block) return false;

    const swingPos = candleIndexMap.get(swing.index);
    if (swingPos == null) {
      this._logDataWarning('_checkBlockRetest', 'Swing index not found in candle map', { 
        swingIndex: swing?.index 
      });
      return false;
    }
    const swingCandle = candles[swingPos];

    if (isBearish) { 
      if (swingCandle.high < block.low) return false;
      if (swingCandle.close <= block.high) return true;
      const nextC = candles[swingPos + 1];
      return !(nextC && nextC.close > block.high);
    } else { 
      if (swingCandle.low > block.high) return false;
      if (swingCandle.close >= block.low) return true;
      const nextC = candles[swingPos + 1];
      return !(nextC && nextC.close < block.low);
    }
  }
  /**
   * Identifies retest patterns on confirmed setups.
   * @param {string} symbol
   * @param {number} granularity
   */
  /**
   * Check if cache needs invalidation
   * @private
   */
  _shouldInvalidateCache(symbol, granularity, candles, confirmedSetups) {
    const cacheKey = this._getCacheKey(symbol, granularity);
    
    // No cache exists
    if (!this.retestCache[cacheKey]) return true;
    
    // Check if new candle arrived
    const lastCandleIndex = candles.length > 0 ? candles[candles.length - 1].index : null;
    if (this.lastProcessedCandle[cacheKey] !== lastCandleIndex) return true;
    
    // Check if setups changed
    const currentHash = this._getSetupsHash(confirmedSetups);
    if (this.setupsHash[cacheKey] !== currentHash) return true;
    
    return false;
  }

  /**
   * Update only the dynamic fields (prices, statuses, confidence)
   * without recalculating the entire retest structure
   * @private
   */
  _updateDynamicFields(cachedRetests, candles, candleIndexMap) {
    const lastCandleTime = candles.length > 0 ? new Date(candles[candles.length - 1].formattedTime) : new Date();
    const currentPrice = candles.length > 0 ? candles[candles.length - 1].close : 0;
    
    for (const retest of cachedRetests) {
      const isBearish = retest.type === 'EQL';
      
      // Update NextRetestStatus
      let isNextExpired = false;
      let isNextActive = false;
      let isNextPending = false;
      
      if (retest.NextMSSExtreme !== null) {
        if (isBearish) {
          if (currentPrice < retest.NextMSSExtreme) isNextExpired = true;
        } else {
          if (currentPrice > retest.NextMSSExtreme) isNextExpired = true;
        }
      }
      
      if (['OTE', 'DOUBLE EQ'].includes(retest.NextStatus)) {
        if (retest.NextExtremeSwing && retest.NextMSSExtreme !== null && retest.NextMSSbreakoutIndex !== null) {
          const minP = Math.min(retest.NextExtremeSwing, retest.NextMSSExtreme);
          const maxP = Math.max(retest.NextExtremeSwing, retest.NextMSSExtreme);
          if (currentPrice >= minP && currentPrice <= maxP) {
            isNextActive = true;
          }
        }
      }
      
      if (retest.NextStatus === 'WAITING FOR SETUP' || retest.NextFinalRetest === 'WAITING FOR FINAL RETEST') {
        isNextPending = true;
      }
      
      if (isNextExpired) retest.NextRetestStatus = 'EXPIRED';
      else if (isNextActive) retest.NextRetestStatus = 'ACTIVE';
      else if (isNextPending) retest.NextRetestStatus = 'PENDING';
      
      // Update PreviousRetestStatus
      let isPrevExpired = false;
      let isPrevActive = false;
      let isPrevPending = false;
      
      if (retest.PreviousMSSExtreme !== null) {
        if (isBearish) {
          if (currentPrice < retest.PreviousMSSExtreme) isPrevExpired = true;
        } else {
          if (currentPrice > retest.PreviousMSSExtreme) isPrevExpired = true;
        }
      }
      
      if (retest.PreviousStatus === 'RIGHT S SETUP') {
        if (retest.PreviousExtremeSwing && retest.PreviousMSSExtreme !== null && retest.PreviousMSSbreakoutIndex !== null) {
          const minP = Math.min(retest.PreviousExtremeSwing, retest.PreviousMSSExtreme);
          const maxP = Math.max(retest.PreviousExtremeSwing, retest.PreviousMSSExtreme);
          if (currentPrice >= minP && currentPrice <= maxP) {
            isPrevActive = true;
          }
        }
      }
      
      if (retest.PreviousStatus === 'WRONG S SETUP' || retest.PreviousFinalRetest === 'WAITING FOR FINAL RETEST') {
        isPrevPending = true;
      }
      
      if (isPrevExpired) retest.PreviousRetestStatus = 'EXPIRED';
      else if (isPrevActive) retest.PreviousRetestStatus = 'ACTIVE';
      else if (isPrevPending) retest.PreviousRetestStatus = 'PENDING';
    }
    
    return cachedRetests;
  }

  getRetests(symbol, granularity) {
    // 1. Get Confirmed Setups
    const confirmedSetups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);
    if (!confirmedSetups.length) return [];

    // 2. Get Swings and Candles
    const swings = swingEngine.get(symbol, granularity);
    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length) return [];
    

const cacheKey = this._getCacheKey(symbol, granularity);
    const candleIndexMap = buildCandleIndexMap(candles);
    
    // 3. Check if we can use cached results with quick update
    if (!this._shouldInvalidateCache(symbol, granularity, candles, confirmedSetups)) {
      // Cache is valid - just update dynamic fields (price-dependent statuses)
      return this._updateDynamicFields(this.retestCache[cacheKey], candles, candleIndexMap);
    }
    
    // Cache invalidated or doesn't exist - do full computation

if (!this.oblvStore) this.oblvStore = {};
if (!this.oblvStore[symbol]) this.oblvStore[symbol] = {};

// Cache key and last candle time check (reuse cacheKey from above)
const lastCandleTimestamp = candles.length > 0 ? candles[candles.length - 1].formattedTime : null;

// Only recalculate OBLV if data has changed (new candle arrived)
if (!this.oblvCache[cacheKey] || this.oblvCache[cacheKey].lastUpdate !== lastCandleTimestamp) {
  this.oblvStore[symbol][granularity] = processOBLV(
    symbol,
    granularity,
    candles
  );
  this.oblvCache[cacheKey] = { lastUpdate: lastCandleTimestamp };
}
    const majorSwings = majorSwingsEngine.getMajorSwings(symbol, granularity);
    if (!candles.length || !swings.length) return [];

    const lastCandleTime = candles.length > 0 ? new Date(candles[candles.length - 1].formattedTime) : new Date();
    const currentPrice = candles.length > 0 ? candles[candles.length - 1].close : 0;
    const retests = [];

    for (const setup of confirmedSetups) {
      // Only process setups that have actually broken out
      if (setup.ConfirmedSetupBreakoutStatus !== 'YES') continue;
      
      const isBearish = setup.type === 'EQL';
      const isBuy = !isBearish;
      const retestState = this._findRetestState(setup, candles, swings, candleIndexMap);

      let prevMSS = {
        extremePrice: null,
        extremeIndex: null,
        extremeFormattedTime: null,
        breakoutPrice: null,
        breakoutIndex: null,
        breakoutFormattedTime: null,
      };
      let nextMSS = {
        extremePrice: null,
        extremeIndex: null,
        extremeFormattedTime: null,
        breakoutPrice: null,
        breakoutIndex: null,
        breakoutFormattedTime: null,
      };

      let prevFinalRetest = { Status: 'WAITING FOR FINAL RETEST', Index: null, FormattedTime: null };
      let nextFinalRetest = { Status: 'WAITING FOR FINAL RETEST', Index: null, FormattedTime: null };

      if (retestState.status === 'FORMED') {
        prevMSS = this._calculateMSS(
          retestState.prevSwing,
          retestState.retestSwing,
          isBearish,
          candles,
          candleIndexMap
        );
        nextMSS = this._calculateMSS(
          retestState.retestSwing,
          retestState.nextSwing,
          isBearish,
          candles,
          candleIndexMap
        );

        prevFinalRetest = this._findFinalRetest(
          prevMSS.extremePrice,
          prevMSS.breakoutIndex,
          isBearish,
          candles,
          candleIndexMap,
          lastCandleTime
        );
        nextFinalRetest = this._findFinalRetest(
          nextMSS.extremePrice,
          nextMSS.breakoutIndex,
          isBearish,
          candles,
          candleIndexMap,
          lastCandleTime
        );
      }

      const nextStatus = this._calculateNextStatus(
        retestState.retestSwing,
        retestState.nextSwing,
        nextMSS,
        isBearish,
        candles,
        candleIndexMap
      );

      const previousStatus = this._calculatePreviousStatus(
        retestState.retestSwing,
        retestState.prevSwing,
        isBearish,
        candles,
        candleIndexMap
      );

      // ─── Calculate NextRetestStatus ───
      let nextRetestStatus = 'PENDING';
      let isNextExpired = false;
      let isNextActive = false;
      let isNextPending = false;

      // 1. Check EXPIRED
      if (nextMSS.extremePrice !== null) {
        if (setup.type === 'EQL') { // Bearish
          if (currentPrice < nextMSS.extremePrice) isNextExpired = true;
        } else { // Bullish
          if (currentPrice > nextMSS.extremePrice) isNextExpired = true;
        }
      }

      // 2. Check ACTIVE
      if (['OTE', 'DOUBLE EQ'].includes(nextStatus)) {
        if (retestState.nextSwing && nextMSS.extremePrice !== null && nextMSS.breakoutIndex !== null) {
          const minP = Math.min(retestState.nextSwing.price, nextMSS.extremePrice);
          const maxP = Math.max(retestState.nextSwing.price, nextMSS.extremePrice);
          if (currentPrice >= minP && currentPrice <= maxP) {
            isNextActive = true;
          }
        }
      }

      // 3. Check PENDING
      if (nextStatus === 'WAITING FOR SETUP' || nextFinalRetest.Status === 'WAITING FOR FINAL RETEST') {
        isNextPending = true;
      }

      if (isNextExpired) nextRetestStatus = 'EXPIRED';
      else if (isNextActive) nextRetestStatus = 'ACTIVE';
      else if (isNextPending) nextRetestStatus = 'PENDING';


      // ─── Calculate PreviousRetestStatus ───
      let prevRetestStatus = 'PENDING';
      let isPrevExpired = false;
      let isPrevActive = false;
      let isPrevPending = false;

      // 1. Check EXPIRED
      if (prevMSS.extremePrice !== null) {
        if (setup.type === 'EQL') { // Bearish
          if (currentPrice < prevMSS.extremePrice) isPrevExpired = true;
        } else { // Bullish
          if (currentPrice > prevMSS.extremePrice) isPrevExpired = true;
        }
      }

      // 2. Check ACTIVE
      if (previousStatus === 'RIGHT S SETUP') {
        if (retestState.prevSwing && prevMSS.extremePrice !== null && prevMSS.breakoutIndex !== null) {
          const minP = Math.min(retestState.prevSwing.price, prevMSS.extremePrice);
          const maxP = Math.max(retestState.prevSwing.price, prevMSS.extremePrice);
          if (currentPrice >= minP && currentPrice <= maxP) {
            isPrevActive = true;
          }
        }
      }

      // 3. Check PENDING
      if (previousStatus === 'WRONG S SETUP' || prevFinalRetest.Status === 'WAITING FOR FINAL RETEST') {
        isPrevPending = true;
      }

      if (isPrevExpired) prevRetestStatus = 'EXPIRED';
      else if (isPrevActive) prevRetestStatus = 'ACTIVE';
      else if (isPrevPending) prevRetestStatus = 'PENDING';
// ─── Calculate Trade Statuses (RUNNING, WAITING, CLOSED) ───
      
     // Count Major Swings after Impulse Extreme
// For EQL (Bearish), we count Major Lows. For EQH (Bullish), we count Major Highs.
const targetMajorType = setup.type === 'EQL' ? 'low' : 'high';

// Binary search to find first swing after impulseExtremeIndex
let left = 0;
let right = majorSwings.length - 1;
let startIdx = majorSwings.length; // Default to end if none found

while (left <= right) {
  const mid = Math.floor((left + right) / 2);
  if (majorSwings[mid].index > setup.impulseExtremeIndex) {
    startIdx = mid;
    right = mid - 1;
  } else {
    left = mid + 1;
  }
}

// Count matching swings from startIdx onwards
let majorSwingCount = 0;
for (let i = startIdx; i < majorSwings.length; i++) {
  if (majorSwings[i].type === targetMajorType) majorSwingCount++;
}

      // Helper to calculate "Time Ago" string
      const calculateRunningTime = (retestStatus, mssData, retestSwing, isBearish) => {
        let activationTime = null;
        
        // Find the last time the trade was in the active zone
        if (retestSwing && mssData.extremePrice !== null && mssData.breakoutIndex !== null) {
           activationTime = this._findActivationTime(retestSwing.price, mssData.extremePrice, mssData.breakoutIndex, candles, candleIndexMap);
        }

        // If an activation time was found, format the string
        if (activationTime) {
          return `(${this._formatTimeAgo(lastCandleTime, new Date(activationTime))} AGO)`;
        }
        // Otherwise, return an empty string
        return '';
      };

   // NextTradeStatus
      let nextTradeStatus;
      if (majorSwingCount >= this.MAJOR_SWING_CLOSE_THRESHOLD) {
        nextTradeStatus = 'CLOSED';
      } else if (nextStatus === 'OTE' || nextStatus === 'DOUBLE EQ') {
        nextTradeStatus = `RUNNING ${calculateRunningTime(nextRetestStatus, nextMSS, retestState.nextSwing, isBearish)}`;
      } else {
        nextTradeStatus = 'WAITING';
      }

      // PrevTradeStatus
      let prevTradeStatus;
      if (majorSwingCount >= this.MAJOR_SWING_CLOSE_THRESHOLD) {
        prevTradeStatus = 'CLOSED';
      } else if (previousStatus === 'RIGHT S SETUP') {
        prevTradeStatus = `RUNNING ${calculateRunningTime(prevRetestStatus, prevMSS, retestState.prevSwing, isBearish)}`;
      } else {
        prevTradeStatus = 'WAITING';
      }

      // ─── Calculate Confidence Scores (Max 3 points each) ───
      /**
       * Confidence scores are awarded based on a 3-point system to quantify the quality of a potential trade setup.
       * Both `PrevConfidenceScore` (for S-Setups) and `NextConfidenceScore` (for OTE/Double EQ) start at 0.
       *
       * ---
       *
       * ### Point 1: Quality of Retest (Applies to Both Scores)
       * **+1 point** is awarded if the primary `RetestExtremeSwing` successfully tests a significant price level.
       * This level can be one of:
       * - **An Order Block (OB):** The last opposing candle before the impulse that broke the structure.
       * - **A Mitigation Block:** The last candle in the *same direction* as the impulse.
       * - **The Impulse Extreme Depth:** The breakout level of the original setup.
       * If the retest swing's wick touches or has a non-sustained close into any of these zones, this point is awarded to both scores.
       *
       * ---
       *
       * ### `NextConfidenceScore`
       * **+1 point** is awarded if `NextStatus` is 'OTE' or 'DOUBLE EQ' **AND** `NextRetestStatus` is 'ACTIVE'. This confirms a high-probability pattern is currently actionable.
       *
       * ---
       *
       * ### `PrevConfidenceScore`
       * **+1 point** is awarded if `PreviousStatus` is 'RIGHT S SETUP' **AND** `PreviousRetestStatus` is 'ACTIVE'. This confirms a valid sweep setup is currently actionable.
       *
       */
      let NextConfidenceScore = 0;
      let PrevConfidenceScore = 0;

      let NextConfidenceReasons = [];
let PrevConfidenceReasons = [];

      // Point 1: Quality of the Retest Swing (Shared by both scores)
      const setupMitigationBlock = this._findMitigationBlock(setup, isBuy, candles, candleIndexMap);
      const ob = this._findOB(setup, isBuy, candles, candleIndexMap);

      const retestedMitigation = this._checkBlockRetest(retestState.retestSwing, setupMitigationBlock, isBearish, candles, candleIndexMap);
      const retestedOB = this._checkBlockRetest(retestState.retestSwing, ob, isBearish, candles, candleIndexMap);

    if (retestedMitigation || retestedOB) {
  NextConfidenceScore++;
  PrevConfidenceScore++;

  if (retestedOB) {
    NextConfidenceReasons.push('Retest respected Order Block');
    PrevConfidenceReasons.push('Retest respected Order Block');
  }

  if (retestedMitigation) {
    NextConfidenceReasons.push('Retest respected Mitigation Block');
    PrevConfidenceReasons.push('Retest respected Mitigation Block');
  }
}

      // Point 2 for NextConfidenceScore
     if ((nextStatus === 'OTE' || nextStatus === 'DOUBLE EQ') &&
    (nextRetestStatus === 'ACTIVE' || nextRetestStatus === 'EXPIRED')) {

  NextConfidenceScore++;
  NextConfidenceReasons.push(`Pattern confirmed: ${nextStatus}`);
}
      // Point 2 for PrevConfidenceScore
     if (previousStatus === 'RIGHT S SETUP' &&
    (prevRetestStatus === 'ACTIVE' || prevRetestStatus === 'EXPIRED')) {

  PrevConfidenceScore++;
  PrevConfidenceReasons.push('Valid Sweep (Right S Setup)');
}
// Point 3 for NextConfidenceScore - Check if activation is fresh
      if (retestState.nextSwing && nextMSS.extremePrice !== null && nextMSS.breakoutIndex !== null) {
        const nextActivationTime = this._findActivationTime(
          retestState.nextSwing.price, 
          nextMSS.extremePrice, 
          nextMSS.breakoutIndex, 
          candles, 
          candleIndexMap
        );
        if (nextActivationTime) {
          const msAgo = lastCandleTime.getTime() - new Date(nextActivationTime).getTime();
          if (msAgo < this.FRESH_ACTIVATION_MS) {
            NextConfidenceScore++;
            NextConfidenceReasons.push('Fresh activation');
          }
        }
      }

      // Point 3 for PrevConfidenceScore - Check if activation is fresh
      if (retestState.prevSwing && prevMSS.extremePrice !== null && prevMSS.breakoutIndex !== null) {
        const prevActivationTime = this._findActivationTime(
          retestState.prevSwing.price, 
          prevMSS.extremePrice, 
          prevMSS.breakoutIndex, 
          candles, 
          candleIndexMap
        );
        if (prevActivationTime) {
          const msAgo = lastCandleTime.getTime() - new Date(prevActivationTime).getTime();
          if (msAgo < this.FRESH_ACTIVATION_MS) {
            PrevConfidenceScore++;
            PrevConfidenceReasons.push('Fresh activation');
          }
        }
      }

      // Cap scores at 3
      if (NextConfidenceScore > 3) NextConfidenceScore = 3;
      if (PrevConfidenceScore > 3) PrevConfidenceScore = 3;
retests.push({
  ...setup,
  signalType: setup.type === 'EQL' ? 'SELL' : 'BUY',
  // Retest Status
  RetestStatus: retestState.status,
  RetestViolationReason: retestState.violationReason || null,

  // Retest Swing
  RetestExtremeSwing: retestState.retestSwing ? retestState.retestSwing.price : null,
  RetestExtremeSwingIndex: retestState.retestSwing ? retestState.retestSwing.index : null,
  RetestExtremeSwingFormattedTime: retestState.retestSwing ? retestState.retestSwing.formattedTime : null,
  
  // Previous Swing
  PreviousExtremeSwing: retestState.prevSwing ? retestState.prevSwing.price : null,
  PreviousExtremeSwingIndex: retestState.prevSwing ? retestState.prevSwing.index : null,
  PreviousExtremeSwingFormattedTime: retestState.prevSwing ? retestState.prevSwing.formattedTime : null,

  // Next Swing
  NextExtremeSwing: retestState.nextSwing ? retestState.nextSwing.price : null,
  NextExtremeSwingIndex: retestState.nextSwing ? retestState.nextSwing.index : null,
  NextExtremeSwingFormattedTime: retestState.nextSwing ? retestState.nextSwing.formattedTime : null,

  // Previous MSS
  PreviousMSSExtreme: prevMSS.extremePrice,
  PreviousMSSExtremeIndex: prevMSS.extremeIndex,
  PreviousMSSExtremeFormattedTime: prevMSS.extremeFormattedTime,
  PreviousMSSbreakout: prevMSS.breakoutPrice,
  PreviousMSSbreakoutIndex: prevMSS.breakoutIndex,
  PreviousMSSbreakoutFormattedTime: prevMSS.breakoutFormattedTime,
  
  // Next MSS
  NextMSSExtreme: nextMSS.extremePrice,
  NextMSSExtremeIndex: nextMSS.extremeIndex,
  NextMSSExtremeFormattedTime: nextMSS.extremeFormattedTime,
  NextMSSbreakout: nextMSS.breakoutPrice,
  NextMSSbreakoutIndex: nextMSS.breakoutIndex,
  NextMSSbreakoutFormattedTime: nextMSS.breakoutFormattedTime,

  // Next Final Retest
  NextFinalRetest: nextFinalRetest.Status,
  NextFinalRetestIndex: nextFinalRetest.Index,
  NextFinalRetestFormattedTime: nextFinalRetest.FormattedTime,

  // Previous Final Retest
  PreviousFinalRetest: prevFinalRetest.Status,
  PreviousFinalRetestIndex: prevFinalRetest.Index,
  PreviousFinalRetestFormattedTime: prevFinalRetest.FormattedTime,
  
  NextStatus: nextStatus,
  PreviousStatus: previousStatus,

  NextRetestStatus: nextRetestStatus,
  PreviousRetestStatus: prevRetestStatus,

  NextTradeStatus: nextTradeStatus,
  PrevTradeStatus: prevTradeStatus,

  NextConfidenceScore,
  PrevConfidenceScore,
  NextConfidenceReasons,
  PrevConfidenceReasons,
});
    }

    // Update cache
    this.retestCache[cacheKey] = retests;
    this.lastProcessedCandle[cacheKey] = candles.length > 0 ? candles[candles.length - 1].index : null;
    this.setupsHash[cacheKey] = this._getSetupsHash(confirmedSetups);

    return retests;
  }

_findRetestState(setup, candles, swings, candleIndexMap) {
    const breakoutIndex = setup.ConfirmedSetupBreakoutStatusIndex;
    const invalidationLevel = setup.setupVshapeDepth;
    const breakoutLevel = setup.impulseExtremeDepth;
    const isBearish = setup.type === 'EQL';
    const targetSwingType = isBearish ? 'high' : 'low';

    // Validate critical data
    if (!candles || candles.length === 0) {
      this._logDataWarning('_findRetestState', 'Empty candles array', { 
        symbol: setup.symbol, 
        granularity: setup.granularity 
      });
    }
    if (!swings || swings.length === 0) {
      this._logDataWarning('_findRetestState', 'Empty swings array', { 
        symbol: setup.symbol, 
        granularity: setup.granularity 
      });
    }

    // 1. Collect all candidate swings after breakout
    let candidates = [];
    for (let i = 0; i < swings.length; i++) {
      const s = swings[i];
      if (s.index <= breakoutIndex) continue;
      if (s.type === targetSwingType) {
        candidates.push({ swing: s, indexInArray: i });
      }
    }

    if (candidates.length === 0) {
      return {
        status: 'WAITING',
        retestSwing: null,
        prevSwing: null,
        nextSwing: null,
        violationReason: null,
      };
    }

    // 2. Find the most extreme swing among candidates
    let extremeCandidate = candidates[0];
    for (let i = 1; i < candidates.length; i++) {
      const c = candidates[i];
      if (isBearish) {
        // For Bearish EQL, we want the Highest High
        if (c.swing.price > extremeCandidate.swing.price) {
          extremeCandidate = c;
        }
      } else {
        // For Bullish EQH, we want the Lowest Low
        if (c.swing.price < extremeCandidate.swing.price) {
          extremeCandidate = c;
        }
      }
    }

    const s = extremeCandidate.swing;
    const i = extremeCandidate.indexInArray;

    // 3. Check if it actually retraced back to the breakout level
    const hasRetraced = isBearish ? s.price > breakoutLevel : s.price < breakoutLevel;

    if (!hasRetraced) {
      return {
        status: 'WAITING',
        retestSwing: null,
        prevSwing: null,
        nextSwing: null,
        violationReason: null,
      };
    }

    // 4. Check Invalidation
    const invalidationResult = this._checkInvalidation(s, invalidationLevel, isBearish, candles, candleIndexMap);

    if (invalidationResult.isValid) {
      // Valid Retest
      
      // Find Previous "Extreme" Swing of SAME TYPE
      let prevSwing = null;
      const prevSwingCandidates = [];
      // Collect all swings of the same type that occurred after the breakout
      // but before the main RetestExtremeSwing.
      // 'i' is the index of the RetestExtremeSwing in the main 'swings' array.
      for (let k = 0; k < i; k++) {
        const currentSwing = swings[k];
        if (currentSwing.index > breakoutIndex && currentSwing.type === targetSwingType) {
          prevSwingCandidates.push(currentSwing);
        }
      }

      if (prevSwingCandidates.length > 0) {
        let extremePrevSwing = prevSwingCandidates[0];
        for (let k = 1; k < prevSwingCandidates.length; k++) {
          const currentCandidate = prevSwingCandidates[k];
          if (isBearish) { // Find highest high
            if (currentCandidate.price > extremePrevSwing.price) {
              extremePrevSwing = currentCandidate;
            }
          } else { // Find lowest low
            if (currentCandidate.price < extremePrevSwing.price) {
              extremePrevSwing = currentCandidate;
            }
          }
        }
        prevSwing = extremePrevSwing;
      }

      // Find Next Swing of SAME TYPE (most extreme one after retest swing)
      let nextSwing = null;
      const nextCandidates = [];
      for (let k = i + 1; k < swings.length; k++) {
        if (swings[k].type === targetSwingType) {
          nextCandidates.push(swings[k]);
        }
      }

      if (nextCandidates.length > 0) {
        let extremeNextCandidate = nextCandidates[0];
        for (const candidate of nextCandidates) {
          if (isBearish) {
            // For Bearish EQL, next "extreme" is the highest high (a lower high)
            if (candidate.price > extremeNextCandidate.price) {
              extremeNextCandidate = candidate;
            }
          } else {
            // For Bullish EQH, next "extreme" is the lowest low (a higher low)
            if (candidate.price < extremeNextCandidate.price) {
              extremeNextCandidate = candidate;
            }
          }
        }
        nextSwing = extremeNextCandidate;
      }

      return {
        status: 'FORMED',
        retestSwing: s,
        prevSwing,
        nextSwing,
        violationReason: null,
      };
    } else {
      // Violated
      return {
        status: 'VIOLATED',
        violationReason: invalidationResult.reason,
        retestSwing: s,
        prevSwing: null,
        nextSwing: null,
      };
    }
  }

  /**
   * Checks if the retest swing respects the invalidation level with tolerance.
   * Tolerance: Allowed if only wick crosses, OR max 1 candle closes beyond level.
   */
_checkInvalidation(swing, invalidationLevel, isBearish, candles, candleIndexMap) {
    const swingPos = candleIndexMap.get(swing.index);
    if (swingPos == null) return { isValid: false, reason: 'SWING_CANDLE_NOT_FOUND' };
    
    const candle = candles[swingPos];

    if (isBearish) {
      // Bearish: Should stay BELOW invalidationLevel (setupVshapeDepth).
      if (candle.high <= invalidationLevel + this.PRICE_EPSILON) return { isValid: true }; // Perfect.
      if (candle.close <= invalidationLevel + this.PRICE_EPSILON) return { isValid: true }; // Valid wick.

      // Close is ABOVE. Check for the "one candle rule".
      const nextC = candles[swingPos + 1];
      if (nextC && nextC.close < invalidationLevel - this.PRICE_EPSILON) {
        return { isValid: true }; // Accepted as a 1-candle sweep.
      }
      
      return { isValid: false, reason: 'SUSTAINED_CLOSE_ABOVE_INVALIDATION' };

    } else {
      // Bullish: Should stay ABOVE invalidationLevel.
      if (candle.low >= invalidationLevel - this.PRICE_EPSILON) return { isValid: true }; // Perfect.
      if (candle.close >= invalidationLevel - this.PRICE_EPSILON) return { isValid: true }; // Valid wick.

      // Close is BELOW. Check "one candle rule".
      const nextC = candles[swingPos + 1];
      if (nextC && nextC.close > invalidationLevel + this.PRICE_EPSILON) {
        return { isValid: true }; // Accepted as a 1-candle sweep.
      }

      return { isValid: false, reason: 'SUSTAINED_CLOSE_BELOW_INVALIDATION' };
    }
  }

  _calculateNextStatus(retestSwing, nextSwing, nextMSS, isBearish, candles, candleIndexMap) {
    // If any required data is missing, we can't determine the status.
    if (!retestSwing || !nextSwing || !nextMSS || !nextMSS.extremePrice) {
      return 'WAITING FOR SETUP';
    }

    const retestPrice = retestSwing.price;
    const nextSwingPrice = nextSwing.price;
    const mssPrice = nextMSS.extremePrice;

    // Condition 1: Check for "OTE"
    const highPoint = isBearish ? retestPrice : mssPrice;
    const lowPoint = isBearish ? mssPrice : retestPrice;

    // Ensure we have a valid range to check against
    if (highPoint > lowPoint) {
        const range = highPoint - lowPoint;
        let oteLower, oteUpper;

   if (isBearish) {
            // Bearish: Retracement UP from Low to High (Premium)
            oteLower = lowPoint + (range * this.OTE_LOWER_RATIO);
            oteUpper = lowPoint + (range * this.OTE_UPPER_RATIO);
        } else {
            // Bullish: Retracement DOWN from High to Low (Discount)
            oteLower = highPoint - (range * this.OTE_UPPER_RATIO);
            oteUpper = highPoint - (range * this.OTE_LOWER_RATIO);
        }

        if (nextSwingPrice >= oteLower && nextSwingPrice <= oteUpper) {
            return 'OTE';
        }
    }

    // Condition 2: Check for "DOUBLE EQ"
   const retestCandleIndex = candleIndexMap.get(retestSwing.index);
    if (retestCandleIndex != null) {
        const retestCandle = candles[retestCandleIndex];
        const retestCandleHigh = retestCandle.high;
        const retestCandleLow = retestCandle.low;
        const retestCandleBodyUpper = Math.max(retestCandle.open, retestCandle.close);
        const retestCandleBodyLower = Math.min(retestCandle.open, retestCandle.close);

        if (isBearish) {
            // For bearish, check if next swing high is within the wick of the retest high
            if (nextSwingPrice <= retestCandleHigh && nextSwingPrice >= retestCandleBodyUpper) {
                return 'DOUBLE EQ';
            }
        } else {
            // For bullish, check if next swing low is within the wick of the retest low
            if (nextSwingPrice >= retestCandleLow && nextSwingPrice <= retestCandleBodyLower) {
                return 'DOUBLE EQ';
            }
        }
    }

    // Condition 3: Default
    return 'WAITING FOR SETUP';
  }

  _calculatePreviousStatus(retestSwing, prevSwing, isBearish, candles, candleIndexMap) {
    if (!retestSwing || !prevSwing) {
      return 'WRONG S SETUP';
    }

    const level = prevSwing.price;

    // First, ensure the retest swing actually went past the previous swing's level at some point.
    // This is the fundamental requirement for a sweep.
const retestCandleIndex = candleIndexMap.get(retestSwing.index);
    if (retestCandleIndex == null) {
      this._logDataWarning('_calculatePreviousStatus', 'Retest swing index not found', { 
        retestSwingIndex: retestSwing?.index 
      });
      return 'WRONG S SETUP';
    }
    const retestCandle = candles[retestCandleIndex];

    if (isBearish) { // EQL
      if (retestCandle.high <= level) return 'WRONG S SETUP'; // Must break the high
    } else { // EQH
      if (retestCandle.low >= level) return 'WRONG S SETUP'; // Must break the low
    }
    
    // Now, check for a sustained break (2+ consecutive closes) in the window leading up to and including the retest swing.
const startIdx = candleIndexMap.get(prevSwing.index);
    const endIdx = retestCandleIndex;
    
    if (startIdx == null || startIdx >= endIdx) {
      if (startIdx == null) {
        this._logDataWarning('_calculatePreviousStatus', 'Previous swing index not found', { 
          prevSwingIndex: prevSwing?.index 
        });
      }
      return 'WRONG S SETUP';
    }

let firstCandlePastLevel = null;
let firstCandleIndex = null;

// Iterate from the candle AFTER prevSwing up to and INCLUDING the retestSwing candle.
for (let i = startIdx + 1; i <= endIdx; i++) {
    const candle = candles[i];
    if (!candle) continue;

    let closedPast = false;
    if (isBearish) { // EQL
        closedPast = candle.close > level;
    } else { // EQH
        closedPast = candle.close < level;
    }

    // Track the first candle that closes past the level
    if (closedPast && firstCandlePastLevel === null) {
        firstCandlePastLevel = candle;
        firstCandleIndex = i;
    }

    // If we have a first candle, check for SUSTAINED close back (2+ consecutive)
    if (firstCandlePastLevel !== null && i > firstCandleIndex) {
        let consecutiveClosesBack = 0;
        
        // Check from current position for consecutive closes
        for (let j = i; j <= endIdx; j++) {
            const checkCandle = candles[j];
            if (!checkCandle) break;
            
            let closedBack = false;
            if (isBearish) { // EQL
                // Check if candle closes below the swept level
                closedBack = checkCandle.close < level;
            } else { // EQH
                // Check if candle closes above the swept level
                closedBack = checkCandle.close > level;
            }
            
         if (closedBack) {
                consecutiveClosesBack++;
                if (consecutiveClosesBack >= this.CONSECUTIVE_CLOSE_THRESHOLD) {
                    return 'WRONG S SETUP'; // Sustained close back invalidates sweep
                }
            } else {
                break; // Reset if not consecutive
            }
        }
    }
}

// If the loop completes without finding 2+ consecutive closes back, it's a valid sweep.
return 'RIGHT S SETUP';
  }

  _formatTimeAgo(date1, date2) {
    const ms = date1.getTime() - date2.getTime();
    const secs = Math.round(ms / 1000);
    const mins = Math.round(secs / 60);
    const hours = Math.round(mins / 60);
    const days = Math.round(hours / 24);

    if (secs < 60) return `${secs} seconds ago`;
    if (mins < 60) return `${mins} minutes ago`;
    if (hours < 24) return `${hours} hours ago`;
    return `${days} days ago`;
  }

_findFinalRetest(mssExtremePrice, mssBreakoutIndex, isBearish, candles, candleIndexMap, lastCandleTime) {
  const result = {
    Status: 'WAITING FOR FINAL RETEST',
    Index: null,
    FormattedTime: null,
  };

  if (mssBreakoutIndex == null || mssExtremePrice == null) {
    return result;
  }

  const breakoutCandlePos = candleIndexMap.get(mssBreakoutIndex);
  if (breakoutCandlePos == null) {
    return result;
  }
  
  let extremeRetestCandle = null;
  let extremeRetestValue = isBearish ? -Infinity : Infinity;
  
  // Scan all candles after breakout to find the MOST EXTREME retest
  for (let i = breakoutCandlePos + 1; i < candles.length; i++) {
    const c = candles[i];
    const retested = isBearish 
      ? c.high >= mssExtremePrice - this.PRICE_EPSILON 
      : c.low <= mssExtremePrice + this.PRICE_EPSILON;

    if (retested) {
      // For bearish, track the HIGHEST high that retested
      // For bullish, track the LOWEST low that retested
      if (isBearish) {
        if (c.high > extremeRetestValue) {
          extremeRetestValue = c.high;
          extremeRetestCandle = c;
        }
      } else {
        if (c.low < extremeRetestValue) {
          extremeRetestValue = c.low;
          extremeRetestCandle = c;
        }
      }
    }
  }

  // If we found at least one retest, return the most extreme one
  if (extremeRetestCandle) {
    result.Status = `RETESTED ${this._formatTimeAgo(lastCandleTime, new Date(extremeRetestCandle.formattedTime))}`;
    result.Index = extremeRetestCandle.index;
    result.FormattedTime = extremeRetestCandle.formattedTime;
  }

  return result;
}

_findExpirationTime(mssExtreme, mssBreakoutIndex, isBearish, candles, candleIndexMap) {
    if (mssExtreme == null || mssBreakoutIndex == null) return null;
    const startPos = candleIndexMap.get(mssBreakoutIndex);
    if (startPos == null) return null;

    for (let i = startPos + 1; i < candles.length; i++) {
      const c = candles[i];
      // Expired if price breaks the extreme
      // EQL (Bearish): Price goes BELOW extreme (which is a Low) -> Wait, EQL setup is bearish.
      // MSS Extreme for EQL is the Lowest Low of the MSS.
      // If price breaks BELOW that, the MSS continues? 
      // Wait, logic in getRetests: "if (currentPrice < nextMSS.extremePrice) isNextExpired = true;" for EQL.
      // So yes, break below extreme expires the retest opportunity (continuation).
      if (isBearish) {
        if (c.close < mssExtreme - this.PRICE_EPSILON) return c.formattedTime;
      } else {
        if (c.close > mssExtreme + this.PRICE_EPSILON) return c.formattedTime;
      }
    }
    return null;
  }

_findActivationTime(swingPrice, mssExtreme, mssBreakoutIndex, candles, candleIndexMap) {
    if (swingPrice == null || mssExtreme == null || mssBreakoutIndex == null) return null;
    const startPos = candleIndexMap.get(mssBreakoutIndex);
    if (startPos == null) return null;

    const minP = Math.min(swingPrice, mssExtreme);
    const maxP = Math.max(swingPrice, mssExtreme);

    // Iterate backwards from the last candle to find the most recent activation
    for (let i = candles.length - 1; i > startPos; i--) {
      const c = candles[i];
      // Active if price touches the zone
      // Check High/Low intersection with [minP, maxP]
      if (c.high >= minP && c.low <= maxP) {
        return c.formattedTime; // This is now the last time
      }
    }
    // If not found, it means the price never entered the zone after the breakout.
    return null;
  }

  /**
   * Calculates MSS Extreme and Breakout between two swings.
   */
  _calculateMSS(startSwing, endSwing, isBearish, candles, candleIndexMap) {
    if (!startSwing || !endSwing) {
      return { extremePrice: null, extremeIndex: null, extremeFormattedTime: null, breakoutPrice: null, breakoutIndex: null, breakoutFormattedTime: null };
    }

    let extremeCandle = null;
    let extremeVal = isBearish ? Infinity : -Infinity;

 const startPos = nextArrayIdx(candleIndexMap, candles, startSwing.index);
    const endPos = candleIndexMap.get(endSwing.index);
    
    if (startPos == null || endPos == null || startPos >= endPos) {
      return { extremePrice: null, extremeIndex: null, extremeFormattedTime: null, breakoutPrice: null, breakoutIndex: null, breakoutFormattedTime: null };
    }

    for (let i = startPos; i <= endPos; i++) {
      const c = candles[i];
      if (isBearish) {
        if (c.low < extremeVal) {
          extremeVal = c.low;
          extremeCandle = c;
        }
      } else {
        if (c.high > extremeVal) {
          extremeVal = c.high;
          extremeCandle = c;
        }
      }
    }

    if (!extremeCandle) {
      return { extremePrice: null, extremeIndex: null, extremeFormattedTime: null, breakoutPrice: null, breakoutIndex: null, breakoutFormattedTime: null };
    }

    // Find Breakout of this MSS Extreme
    // Must occur AFTER the extreme candle
let breakoutCandle = null;
    const mssPos = candleIndexMap.get(extremeCandle.index);
    
    if (mssPos != null) {
      for (let i = mssPos + 1; i < candles.length; i++) {
        const c = candles[i];
        if (isBearish) {
          // Bearish MSS Breakout: Close BELOW the Low
          if (c.close < extremeVal - this.PRICE_EPSILON) {
            breakoutCandle = c;
            break;
          }
        } else {
          // Bullish MSS Breakout: Close ABOVE the High
          if (c.close > extremeVal + this.PRICE_EPSILON) {
            breakoutCandle = c;
            break;
          }
        }
      }
    }

    return {
      extremePrice: extremeVal,
      extremeIndex: extremeCandle.index,
      extremeFormattedTime: extremeCandle.formattedTime,
      breakoutPrice: breakoutCandle ? breakoutCandle.close : null,
      breakoutIndex: breakoutCandle ? breakoutCandle.index : null,
      breakoutFormattedTime: breakoutCandle ? breakoutCandle.formattedTime : null
    };
  }

  /**
   * Manually invalidate cache for a symbol/granularity
   * Call this when you know setups or swings have changed
   */
  invalidateCache(symbol, granularity) {
    const cacheKey = this._getCacheKey(symbol, granularity);
    delete this.retestCache[cacheKey];
    delete this.lastProcessedCandle[cacheKey];
    delete this.setupsHash[cacheKey];
  }

  /**
   * Clear all caches
   */
  clearAllCaches() {
    this.retestCache = {};
    this.lastProcessedCandle = {};
    this.setupsHash = {};
    this.oblvCache = {};
  }
}

module.exports = new RetestEngine();