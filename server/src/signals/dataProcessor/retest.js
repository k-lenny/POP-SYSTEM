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
      }

      retests.push({
        ...setup,
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