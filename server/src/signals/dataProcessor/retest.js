const confirmedSetupEngine = require('./confirmedSetup');
const swingEngine = require('./swings');
const signalEngine = require('../signalEngine');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');

class RetestEngine {
  /**
   * Identifies retest patterns on confirmed setups.
   * @param {string} symbol
   * @param {number} granularity
   */
  getRetests(symbol, granularity) {
    // 1. Get Confirmed Setups
    const confirmedSetups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);
    if (!confirmedSetups.length) return [];

    // 2. Get Swings and Candles
    const swings = swingEngine.get(symbol, granularity);
    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length || !swings.length) return [];

    const candleIndexMap = buildCandleIndexMap(candles);
    const lastCandleTime = candles.length > 0 ? new Date(candles[candles.length - 1].formattedTime) : new Date();
    const currentPrice = candles.length > 0 ? candles[candles.length - 1].close : 0;
    const retests = [];

    for (const setup of confirmedSetups) {
      // Only process setups that have actually broken out
      if (setup.ConfirmedSetupBreakoutStatus !== 'YES') continue;
      
      const isBearish = setup.type === 'EQL';
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
      });
    }

    return retests;
  }

  _findRetestState(setup, candles, swings, candleIndexMap) {
    const breakoutIndex = setup.ConfirmedSetupBreakoutStatusIndex;
    const invalidationLevel = setup.setupVshapeDepth;
    const breakoutLevel = setup.impulseExtremeDepth;
    const isBearish = setup.type === 'EQL';
    const targetSwingType = isBearish ? 'high' : 'low';

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
      
      // Find Previous Swing of SAME TYPE
      let prevSwing = null;
      for (let k = i - 1; k >= 0; k--) {
        // Constraint: Previous swing must also be after the breakout
        if (swings[k].index <= breakoutIndex) break;

        if (swings[k].type === targetSwingType) {
          prevSwing = swings[k];
          break;
        }
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
    if (swingPos === undefined) return { isValid: false, reason: 'SWING_CANDLE_NOT_FOUND' };
    
    const candle = candles[swingPos];

    if (isBearish) {
      // Bearish: Should stay BELOW invalidationLevel (setupVshapeDepth).
      if (candle.high <= invalidationLevel) return { isValid: true }; // Perfect.
      if (candle.close <= invalidationLevel) return { isValid: true }; // Valid wick.

      // Close is ABOVE. Check for the "one candle rule".
      const nextC = candles[swingPos + 1];
      if (nextC && nextC.close < invalidationLevel) {
        return { isValid: true }; // Accepted as a 1-candle sweep.
      }
      
      return { isValid: false, reason: 'SUSTAINED_CLOSE_ABOVE_INVALIDATION' };

    } else {
      // Bullish: Should stay ABOVE invalidationLevel.
      if (candle.low >= invalidationLevel) return { isValid: true }; // Perfect.
      if (candle.close >= invalidationLevel) return { isValid: true }; // Valid wick.

      // Close is BELOW. Check "one candle rule".
      const nextC = candles[swingPos + 1];
      if (nextC && nextC.close > invalidationLevel) {
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
        const oteLevel62 = lowPoint + (range * 0.625);
        const oteLevel79 = lowPoint + (range * 0.79);

        if (nextSwingPrice >= oteLevel62 && nextSwingPrice <= oteLevel79) {
            return 'OTE';
        }
    }

    // Condition 2: Check for "DOUBLE EQ"
    const retestCandleIndex = candleIndexMap.get(retestSwing.index);
    if (retestCandleIndex !== undefined) {
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
    const retestCandleIndex = candleIndexMap.get(retestSwing.index);

    if (retestCandleIndex === undefined) {
      return 'WRONG S SETUP';
    }
    const retestCandle = candles[retestCandleIndex];

    if (isBearish) { // EQL
      // Condition: Retest high must cross above previous swing high level
      if (retestCandle.high <= level) {
        return 'WRONG S SETUP'; // Did not cross
      }
      // Now check if it was a valid sweep (wick or 1-candle rule)
      if (retestCandle.close <= level) {
        return 'RIGHT S SETUP'; // Wick crossing
      }
      // If close is above, check next candle
      const nextC = candles[retestCandleIndex + 1];
      if (nextC && nextC.close < level) {
        return 'RIGHT S SETUP'; // 1-candle rule met
      }
    } else { // EQH (Bullish)
      // Condition: Retest low must cross below previous swing low level
      if (retestCandle.low >= level) {
        return 'WRONG S SETUP'; // Did not cross
      }
      // Now check if it was a valid sweep
      if (retestCandle.close >= level) {
        return 'RIGHT S SETUP'; // Wick crossing
      }
      // If close is below, check next candle
      const nextC = candles[retestCandleIndex + 1];
      if (nextC && nextC.close > level) {
        return 'RIGHT S SETUP'; // 1-candle rule met
      }
    }

    // If none of the "RIGHT S SETUP" conditions were met after a crossing, it's a sustained break.
    return 'WRONG S SETUP';
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

    if (mssBreakoutIndex === null || mssExtremePrice === null) {
      return result;
    }

    const breakoutCandlePos = candleIndexMap.get(mssBreakoutIndex);
    if (breakoutCandlePos === undefined) {
      return result;
    }
    
    for (let i = breakoutCandlePos + 1; i < candles.length; i++) {
      const c = candles[i];
      const retested = isBearish ? c.high >= mssExtremePrice : c.low <= mssExtremePrice;

      if (retested) {
        result.Status = `RETESTED ${this._formatTimeAgo(lastCandleTime, new Date(c.formattedTime))}`;
        result.Index = c.index;
        result.FormattedTime = c.formattedTime;
        return result; // Found the first retest, stop searching
      }
    }

    return result;
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
    
    if (startPos === undefined || endPos === undefined || startPos >= endPos) {
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
    
    if (mssPos !== undefined) {
      for (let i = mssPos + 1; i < candles.length; i++) {
        const c = candles[i];
        if (isBearish) {
          // Bearish MSS Breakout: Close BELOW the Low
          if (c.close < extremeVal) {
            breakoutCandle = c;
            break;
          }
        } else {
          // Bullish MSS Breakout: Close ABOVE the High
          if (c.close > extremeVal) {
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
}

module.exports = new RetestEngine();