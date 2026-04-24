// server/src/signals/dataProcessor/confirmedSetup.js
const fs = require('fs');
const path = require('path');
const setupEngine = require('./setup');
const signalEngine = require('../signalEngine');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');
const { processOBLV } = require('./OBLV');
const patternEngine = require('../../strategies/patterns/pattern');
const pattern2Engine = require('../../strategies/patterns/pattern2');
const pattern3Engine = require('../../strategies/patterns/pattern3');

const LOCKED_STATUSES_PATH = path.join(__dirname, '..', '..', '..', 'data', 'lockedStatuses.json');

class ConfirmedSetupEngine {
  constructor() {
    // Cache for locked setupStatus once breakout is confirmed (YES)
    // Key: `${symbol}_${granularity}_${brokenIndex}` → setupStatus string
    this._lockedStatuses = this._loadLockedStatuses();
  }

  _loadLockedStatuses() {
    try {
      const raw = fs.readFileSync(LOCKED_STATUSES_PATH, 'utf8');
      return JSON.parse(raw);
    } catch {
      return {};
    }
  }

  _saveLockedStatuses() {
    try {
      const dir = path.dirname(LOCKED_STATUSES_PATH);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(LOCKED_STATUSES_PATH, JSON.stringify(this._lockedStatuses, null, 2), 'utf8');
    } catch {
      // Silent fail — next restart will just re-derive locks
    }
  }

  _lockKey(symbol, granularity, setup) {
    return `${symbol}_${granularity}_${setup.brokenIndex}`;
  }

  /**
   * Takes setups and classifies them into confirmed patterns like OTE, DOUBLE EQ, or S-SETUP.
   * @param {string} symbol The symbol to check.
   * @param {number} granularity The granularity in seconds.
   * @returns {Array<Object>} An array of confirmed setup objects.
   */
  getConfirmedSetups(symbol, granularity) {
    const setups = setupEngine.getSetups(symbol, granularity);
    if (!setups.length) {
      return [];
    }

    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length) {
      return [];
    }

    const candleIndexMap = buildCandleIndexMap(candles);

    // Build formattedTime → candle map so we can resolve OB candle indexes
    const ftMap = new Map();
    for (const c of candles) ftMap.set(c.formattedTime, c);

    const oblvData = processOBLV(symbol, granularity, candles);

    // Precompute pattern price sets for O(1) lookup when identifying OBSetupExtreme.
    const p1Prices = new Set(
      (patternEngine.get(symbol, granularity) || [])
        .map(p => p.currentSwingPrice)
        .filter(v => v != null)
    );
    const p2Prices = new Set(
      (pattern2Engine.get(symbol, granularity) || [])
        .map(p => p.firstSwingPrice)
        .filter(v => v != null)
    );
    const p3Prices = new Set(
      (pattern3Engine.get(symbol, granularity) || [])
        .map(p => p.firstSwingPrice)
        .filter(v => v != null)
    );

    const confirmedSetups = [];

    for (const setup of setups) {
      // A setup must have these values to be classifiable
      if (setup.setupVshapeDepth === null || setup.preBreakoutVDepth === null || setup.impulseExtremeDepth === null || !isFinite(setup.preBreakoutVDepth)) {
        continue;
      }

      const lockKey = this._lockKey(symbol, granularity, setup);
      const lockedStatus = this._lockedStatuses[lockKey];

      // Compute breakout first so the S SETUP invalidation scan can be bounded by it.
      // This makes cold-start replay and live-incremental runs produce identical results.
      const breakoutResult = this._getBreakoutStatus(setup, candles, candleIndexMap);
      const breakoutPos = breakoutResult.index !== null
        ? candleIndexMap.get(breakoutResult.index)
        : undefined;
      const sSetupScanEnd = breakoutPos !== undefined ? breakoutPos : candles.length;

      let status;
      let isValid;

      if (lockedStatus) {
        // Breakout was previously confirmed — setupStatus is locked
        status = lockedStatus;
        isValid = true;
      } else {
        const result = this._getSetupStatus(setup, candles, candleIndexMap, sSetupScanEnd);
        status = result.status;
        isValid = result.isValid;
      }

      // If the setup status is invalid (e.g., a failed S-SETUP), skip it.
      if (!isValid) {
        continue;
      }

      // If no specific status was matched, it's not a "confirmed" setup type we're looking for.
      if (status === null) {
        continue;
      }

      // Lock the setupStatus once breakout is confirmed and persist to disk
      if (breakoutResult.status === 'YES' && !lockedStatus) {
        this._lockedStatuses[lockKey] = status;
        this._saveLockedStatuses();
      }

      const setupOB = this._findSetupOB(
        oblvData,
        ftMap,
        setup.setupVshapeIndex,
        breakoutResult.index,
        candles,
        candleIndexMap,
        setup.type
      );

      let OBCross = null;
      let OBSetupExtreme = null;
      if (breakoutResult.status === 'YES' && setupOB) {
        const obCrossResult = this._findOBCross(setup.type, setupOB, candleIndexMap, candles, breakoutResult.index);
        if (obCrossResult.obCrossCandle) {
          const cc = obCrossResult.obCrossCandle;
          OBCross = {
            index: cc.index,
            formattedTime: cc.formattedTime,
            data: {
              open: cc.open,
              high: cc.high,
              low: cc.low,
              close: cc.close,
            },
          };
        }
        if (obCrossResult.obCrossIndex !== null) {
          OBSetupExtreme = this._findOBSetupExtreme(
            setup.type,
            obCrossResult.obCrossIndex,
            candleIndexMap,
            candles,
            p1Prices,
            p2Prices,
            p3Prices
          );
        }
      }

      confirmedSetups.push({
        ...setup,
        setupStatus: status,
        setupStatusIndex: setup.setupVshapeIndex,
        setupStatusFormattedTime: setup.setupVshapeFormattedTime,
        ConfirmedSetupBreakoutStatus: breakoutResult.status,
        ConfirmedSetupBreakoutStatusIndex: breakoutResult.index,
        ConfirmedSetupBreakoutStatusFormattedTime: breakoutResult.formattedTime,
        setupOB,
        OBCross,
        OBSetupExtreme,
      });
    }

    return confirmedSetups;
  }

  /**
   * Determines the classification (OTE, DOUBLE EQ, S SETUP) of a setup.
   * @private
   */
  _getSetupStatus(setup, candles, candleIndexMap, sSetupScanEnd = candles.length) {
    const {
      type,
      setupVshapeDepth,
      preBreakoutVDepth,
      impulseExtremeDepth,
      preBreakoutVIndex,
      setupVshapeIndex,
    } = setup;

    // --- OTE Check ---
    const impulseRange = Math.abs(preBreakoutVDepth - impulseExtremeDepth);
    if (impulseRange > 0) {
      const oteLowerBound = type === 'EQL' ? impulseExtremeDepth + (impulseRange * 0.625) : impulseExtremeDepth - (impulseRange * 0.79);
      const oteUpperBound = type === 'EQL' ? impulseExtremeDepth + (impulseRange * 0.79) : impulseExtremeDepth - (impulseRange * 0.625);

      if (type === 'EQL' && setupVshapeDepth >= oteLowerBound && setupVshapeDepth <= oteUpperBound) {
        return { status: 'OTE', isValid: true };
      }
      if (type === 'EQH' && setupVshapeDepth <= oteUpperBound && setupVshapeDepth >= oteLowerBound) {
        return { status: 'OTE', isValid: true };
      }
    }

    // --- DOUBLE EQ Check ---
    const preBreakoutVPos = candleIndexMap.get(preBreakoutVIndex);
    if (preBreakoutVPos !== undefined) {
      const preBreakoutVCandle = candles[preBreakoutVPos];
      if (type === 'EQL') {
        const preBreakoutVKeyPrice = Math.max(preBreakoutVCandle.open, preBreakoutVCandle.close);
        if (setupVshapeDepth < preBreakoutVDepth && setupVshapeDepth > preBreakoutVKeyPrice) {
          return { status: 'DOUBLE EQ', isValid: true };
        }
      } else { // EQH
        const preBreakoutVKeyPrice = Math.min(preBreakoutVCandle.open, preBreakoutVCandle.close);
        if (setupVshapeDepth > preBreakoutVDepth && setupVshapeDepth < preBreakoutVKeyPrice) {
          return { status: 'DOUBLE EQ', isValid: true };
        }
      }
    }

    // --- S SETUP (Sweep) Check ---
    const setupVPos = candleIndexMap.get(setupVshapeIndex);
    if (setupVPos !== undefined) {
      const isSweep = (type === 'EQL' && setupVshapeDepth > preBreakoutVDepth) || (type === 'EQH' && setupVshapeDepth < preBreakoutVDepth);

      if (isSweep) {
        // Locate the FIRST candle (from just after preBreakoutV up to setupV inclusive)
        // whose wick or body crossed preBreakoutVDepth. That candle is the reference.
        const preBreakoutVPos = candleIndexMap.get(preBreakoutVIndex);
        let firstCrosser = null;
        let firstCrosserPos = -1;
        if (preBreakoutVPos !== undefined) {
          for (let i = preBreakoutVPos + 1; i <= setupVPos; i++) {
            const c = candles[i];
            const crossed = (type === 'EQL' && c.high > preBreakoutVDepth) || (type === 'EQH' && c.low < preBreakoutVDepth);
            if (crossed) {
              firstCrosser = c;
              firstCrosserPos = i;
              break;
            }
          }
        }

        if (!firstCrosser) {
          return { status: 'S SETUP FAILED', isValid: false };
        }

        // Wick-chain rule: the reference extreme may ratchet forward as long as each
        // subsequent candle that exceeds the current reference does so by WICK ONLY.
        // Any candle whose open OR close pushes past the current reference is a body
        // cross and invalidates the setup. Bounded by sSetupScanEnd so post-breakout
        // price can never retroactively invalidate.
        let refCandle = firstCrosser;
        for (let i = firstCrosserPos + 1; i < sSetupScanEnd; i++) {
          const c = candles[i];
          if (type === 'EQL') {
            if (c.open > refCandle.high || c.close > refCandle.high) {
              return { status: 'S SETUP FAILED', isValid: false };
            }
            if (c.high > refCandle.high) {
              refCandle = c; // wick-only chain advances the reference
            }
          } else {
            if (c.open < refCandle.low || c.close < refCandle.low) {
              return { status: 'S SETUP FAILED', isValid: false };
            }
            if (c.low < refCandle.low) {
              refCandle = c;
            }
          }
        }
        return { status: 'S SETUP', isValid: true };
      }
    }

    return { status: null, isValid: true }; // No specific status matched, but not explicitly invalid.
  }

  /**
   * Checks if the price has broken the impulse extreme after the setup formed.
   * Uses strict validation: first candle that crosses by body confirms breakout,
   * but if crossed by wick only, subsequent body crosses must exceed all previous crossing candles' extremes.
   * @private
   */
  _getBreakoutStatus(setup, candles, candleIndexMap) {
    const startScanPos = nextArrayIdx(candleIndexMap, candles, setup.setupVshapeIndex);
    if (startScanPos === undefined) {
      return { status: 'NO', index: null, formattedTime: null };
    }

    const isEQH = setup.type === 'EQH';
    const impulseExtreme = setup.impulseExtremeDepth;
    const crossingCandles = []; // Track all candles that crossed the impulse extreme

    for (let i = startScanPos; i < candles.length; i++) {
      const candle = candles[i];
      
      // Check if this candle crosses the impulse extreme
      const crossesByWickOrBody = isEQH 
        ? candle.high > impulseExtreme 
        : candle.low < impulseExtreme;
      
      if (!crossesByWickOrBody) {
        continue; // This candle doesn't cross, skip it
      }

      // Check if it crosses by body (close is beyond the impulse extreme)
      const crossesByBody = isEQH 
        ? candle.close > impulseExtreme 
        : candle.close < impulseExtreme;

      if (crossesByBody) {
        // Body cross detected
        if (crossingCandles.length === 0) {
          // First candle that crossed - if it's by body, immediate breakout
          return { status: 'YES', index: candle.index, formattedTime: candle.formattedTime };
        } else {
          // Not the first crossing candle - must exceed all previous crossing candles' extremes
          let exceedsAllPrevious = true;
          
          for (const prevCandle of crossingCandles) {
            if (isEQH) {
              // For EQH: current close must be above all previous crossing candles' highs
              if (candle.close <= prevCandle.high) {
                exceedsAllPrevious = false;
                break;
              }
            } else {
              // For EQL: current close must be below all previous crossing candles' lows
              if (candle.close >= prevCandle.low) {
                exceedsAllPrevious = false;
                break;
              }
            }
          }

          if (exceedsAllPrevious) {
            // Valid breakout - close exceeded all previous crossing candles' extremes
            return { status: 'YES', index: candle.index, formattedTime: candle.formattedTime };
          }
        }
      }

      // Track this crossing candle (whether by wick or body)
      crossingCandles.push(candle);
    }

    return { status: 'NO', index: null, formattedTime: null };
  }

  /**
   * Finds the first OB (from OBLV data) whose candle index falls strictly
   * between setupStatusIndex and breakoutStatusIndex (end of candles if no breakout).
   * An OB that was retested AND fully crossed is invalidated and we look at the next one:
   *   - EQH: any post-OB candle with low < OB.low
   *   - EQL: any post-OB candle with high > OB.high
   * @private
   */
  _findSetupOB(oblvData, ftMap, setupStatusIndex, breakoutStatusIndex, candles, candleIndexMap, type) {
    const isEQH = type === 'EQH';

    for (const oblv of oblvData) {
      if (!oblv.OB || !oblv.OBFormattedTime) continue;

      const obCandle = ftMap.get(oblv.OBFormattedTime);
      if (!obCandle) continue;

      const obIdx = obCandle.index;

      // The setupStatusIndex candle itself is allowed to be the OB
      if (obIdx < setupStatusIndex) continue;

      // Must be strictly before breakout (if one exists)
      if (breakoutStatusIndex !== null && obIdx >= breakoutStatusIndex) continue;

      // If this OB has been retested and price then crossed fully past it,
      // treat it as invalidated and move to the next candidate.
      if (oblv.OBRetest === 'yes' && candles && candleIndexMap) {
        const obArrPos = candleIndexMap.get(obIdx);
        const breakoutArrPos = breakoutStatusIndex !== null
          ? candleIndexMap.get(breakoutStatusIndex)
          : undefined;
        const scanEnd = breakoutArrPos !== undefined ? breakoutArrPos : candles.length;

        if (obArrPos !== undefined) {
          let crossedPast = false;
          for (let i = obArrPos + 1; i < scanEnd; i++) {
            const c = candles[i];
            if (!c) continue;
            if (isEQH && c.low < oblv.OB.low) { crossedPast = true; break; }
            if (!isEQH && c.high > oblv.OB.high) { crossedPast = true; break; }
          }
          if (crossedPast) continue;
        }
      }

      return {
        index: obCandle.index,
        formattedTime: obCandle.formattedTime,
        open: oblv.OB.open,
        high: oblv.OB.high,
        low: oblv.OB.low,
        close: oblv.OB.close,
      };
    }
    return null;
  }

  /**
   * Finds the first candle after the breakout that crosses the setupOB.
   * For EQL: first candle whose high crosses above setupOB.high
   * For EQH: first candle whose low crosses below setupOB.low
   * @private
   */
  _findOBCross(type, setupOB, candleIndexMap, candles, breakoutIndex) {
    const isEQL = type === 'EQL';
    const obHigh = setupOB.high;
    const obLow = setupOB.low;

    const breakoutPos = candleIndexMap.get(breakoutIndex);
    const scanStart = breakoutPos !== undefined ? breakoutPos + 1 : nextArrayIdx(candleIndexMap, candles, setupOB.index);
    let obCrossIndex = null;
    let obCrossCandle = null;
    if (scanStart !== undefined) {
      for (let i = scanStart; i < candles.length; i++) {
        const c = candles[i];
        if (isEQL && c.high > obHigh) {
          obCrossIndex = c.index;
          obCrossCandle = c;
          break;
        }
        if (!isEQL && c.low < obLow) {
          obCrossIndex = c.index;
          obCrossCandle = c;
          break;
        }
      }
    }

    return { obCrossIndex, obCrossCandle };
  }

  /**
   * Finds the OBSetupExtreme — scans forward from just after the OB-crossing candle,
   * maintaining a running extreme (lowest low for EQL, highest high for EQH). The
   * first running-extreme candle whose price matches any pattern/pattern2/pattern3
   * swing price is returned. Returns null if no running extreme matches before
   * candles end.
   * @private
   */
  _findOBSetupExtreme(type, obCrossIndex, candleIndexMap, candles, p1Prices, p2Prices, p3Prices) {
    const isEQL = type === 'EQL';

    const crossPos = candleIndexMap.get(obCrossIndex);
    if (crossPos === undefined) return null;

    let extremePrice = isEQL ? Infinity : -Infinity;
    let extremeCandle = null;

    for (let i = crossPos + 1; i < candles.length; i++) {
      const c = candles[i];
      const candlePrice = isEQL ? c.low : c.high;
      const isNewExtreme = isEQL ? candlePrice < extremePrice : candlePrice > extremePrice;
      if (!isNewExtreme) continue;

      extremePrice = candlePrice;
      extremeCandle = c;

      if (p1Prices.has(candlePrice) || p2Prices.has(candlePrice) || p3Prices.has(candlePrice)) {
        return {
          index: extremeCandle.index,
          price: candlePrice,
          formattedTime: extremeCandle.formattedTime,
        };
      }
    }

    return null;
  }
}

module.exports = new ConfirmedSetupEngine();