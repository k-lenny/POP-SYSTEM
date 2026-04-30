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
    } catch {}
  }

  _lockKey(symbol, granularity, setup) {
    return `${symbol}_${granularity}_${setup.brokenIndex}`;
  }

  getConfirmedSetups(symbol, granularity) {
    const setups = setupEngine.getSetups(symbol, granularity);
    if (!setups.length) return [];

    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length) return [];

    const candleIndexMap = buildCandleIndexMap(candles);

    const ftMap = new Map();
    for (const c of candles) ftMap.set(c.formattedTime, c);

    const oblvData = processOBLV(symbol, granularity, candles);

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
      if (
        setup.setupVshapeDepth === null ||
        setup.preBreakoutVDepth === null ||
        setup.impulseExtremeDepth === null ||
        !isFinite(setup.preBreakoutVDepth)
      ) {
        continue;
      }

      const lockKey = this._lockKey(symbol, granularity, setup);
      const lockedStatus = this._lockedStatuses[lockKey];

      const breakoutResult = this._getBreakoutStatus(setup, candles, candleIndexMap);
      const breakoutPos = breakoutResult.index !== null
        ? candleIndexMap.get(breakoutResult.index)
        : undefined;
      const sSetupScanEnd = breakoutPos !== undefined ? breakoutPos : candles.length;

      let status;
      let isValid;

      if (lockedStatus) {
        status = lockedStatus;
        isValid = true;
      } else {
        const result = this._getSetupStatus(setup, candles, candleIndexMap, sSetupScanEnd);
        status = result.status;
        isValid = result.isValid;
      }

      if (!isValid) continue;
      if (status === null) continue;

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
      let OBSetupExtremeCandidates = [];

      if (breakoutResult.status === 'YES' && setupOB) {
        const obCrossResult = this._findOBCross(
          setup.type,
          setupOB,
          candleIndexMap,
          candles,
          breakoutResult.index
        );

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
          // Returns all pattern-matching running-extreme candles in order.
          // final.js will walk this array and pick the first whose patternMatch
          // retest did not violate the currentSwing.
          OBSetupExtremeCandidates = this._findOBSetupExtreme(
            setup.type,
            obCrossResult.obCrossIndex,
            candleIndexMap,
            candles,
            p1Prices,
            p2Prices,
            p3Prices
          );
          // Default to the first candidate for consumers that don't need fallback.
          OBSetupExtreme = OBSetupExtremeCandidates.length > 0
            ? OBSetupExtremeCandidates[0]
            : null;
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
        OBSetupExtremeCandidates,
      });
    }

    return confirmedSetups;
  }

  _getSetupStatus(setup, candles, candleIndexMap, sSetupScanEnd = candles.length) {
    const {
      type,
      setupVshapeDepth,
      preBreakoutVDepth,
      impulseExtremeDepth,
      preBreakoutVIndex,
      setupVshapeIndex,
    } = setup;

    const impulseRange = Math.abs(preBreakoutVDepth - impulseExtremeDepth);
    if (impulseRange > 0) {
      const oteLowerBound = type === 'EQL'
        ? impulseExtremeDepth + (impulseRange * 0.625)
        : impulseExtremeDepth - (impulseRange * 0.79);
      const oteUpperBound = type === 'EQL'
        ? impulseExtremeDepth + (impulseRange * 0.79)
        : impulseExtremeDepth - (impulseRange * 0.625);

      if (type === 'EQL' && setupVshapeDepth >= oteLowerBound && setupVshapeDepth <= oteUpperBound) {
        return { status: 'OTE', isValid: true };
      }
      if (type === 'EQH' && setupVshapeDepth <= oteUpperBound && setupVshapeDepth >= oteLowerBound) {
        return { status: 'OTE', isValid: true };
      }
    }

    const preBreakoutVPos = candleIndexMap.get(preBreakoutVIndex);
    if (preBreakoutVPos !== undefined) {
      const preBreakoutVCandle = candles[preBreakoutVPos];
      if (type === 'EQL') {
        const preBreakoutVKeyPrice = Math.max(preBreakoutVCandle.open, preBreakoutVCandle.close);
        if (setupVshapeDepth < preBreakoutVDepth && setupVshapeDepth > preBreakoutVKeyPrice) {
          return { status: 'DOUBLE EQ', isValid: true };
        }
      } else {
        const preBreakoutVKeyPrice = Math.min(preBreakoutVCandle.open, preBreakoutVCandle.close);
        if (setupVshapeDepth > preBreakoutVDepth && setupVshapeDepth < preBreakoutVKeyPrice) {
          return { status: 'DOUBLE EQ', isValid: true };
        }
      }
    }

    const setupVPos = candleIndexMap.get(setupVshapeIndex);
    if (setupVPos !== undefined) {
      const isSweep =
        (type === 'EQL' && setupVshapeDepth > preBreakoutVDepth) ||
        (type === 'EQH' && setupVshapeDepth < preBreakoutVDepth);

      if (isSweep) {
        const preBreakoutVPos = candleIndexMap.get(preBreakoutVIndex);
        let firstCrosser = null;
        let firstCrosserPos = -1;
        if (preBreakoutVPos !== undefined) {
          for (let i = preBreakoutVPos + 1; i <= setupVPos; i++) {
            const c = candles[i];
            const crossed =
              (type === 'EQL' && c.high > preBreakoutVDepth) ||
              (type === 'EQH' && c.low < preBreakoutVDepth);
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

        let refCandle = firstCrosser;
        for (let i = firstCrosserPos + 1; i < sSetupScanEnd; i++) {
          const c = candles[i];
          if (type === 'EQL') {
            if (c.open > refCandle.high || c.close > refCandle.high) {
              return { status: 'S SETUP FAILED', isValid: false };
            }
            if (c.high > refCandle.high) refCandle = c;
          } else {
            if (c.open < refCandle.low || c.close < refCandle.low) {
              return { status: 'S SETUP FAILED', isValid: false };
            }
            if (c.low < refCandle.low) refCandle = c;
          }
        }
        return { status: 'S SETUP', isValid: true };
      }
    }

    return { status: null, isValid: true };
  }

  _getBreakoutStatus(setup, candles, candleIndexMap) {
    const startScanPos = nextArrayIdx(candleIndexMap, candles, setup.setupVshapeIndex);
    if (startScanPos === undefined) {
      return { status: 'NO', index: null, formattedTime: null };
    }

    const isEQH = setup.type === 'EQH';
    const impulseExtreme = setup.impulseExtremeDepth;
    const crossingCandles = [];

    for (let i = startScanPos; i < candles.length; i++) {
      const candle = candles[i];

      const crossesByWickOrBody = isEQH
        ? candle.high > impulseExtreme
        : candle.low < impulseExtreme;

      if (!crossesByWickOrBody) continue;

      const crossesByBody = isEQH
        ? candle.close > impulseExtreme
        : candle.close < impulseExtreme;

      if (crossesByBody) {
        if (crossingCandles.length === 0) {
          return { status: 'YES', index: candle.index, formattedTime: candle.formattedTime };
        } else {
          let exceedsAllPrevious = true;
          for (const prevCandle of crossingCandles) {
            if (isEQH) {
              if (candle.close <= prevCandle.high) { exceedsAllPrevious = false; break; }
            } else {
              if (candle.close >= prevCandle.low) { exceedsAllPrevious = false; break; }
            }
          }
          if (exceedsAllPrevious) {
            return { status: 'YES', index: candle.index, formattedTime: candle.formattedTime };
          }
        }
      }

      crossingCandles.push(candle);
    }

    return { status: 'NO', index: null, formattedTime: null };
  }

  _findSetupOB(oblvData, ftMap, setupStatusIndex, breakoutStatusIndex, candles, candleIndexMap, type) {
    const isEQH = type === 'EQH';

    for (const oblv of oblvData) {
      if (!oblv.OB || !oblv.OBFormattedTime) continue;

      const obCandle = ftMap.get(oblv.OBFormattedTime);
      if (!obCandle) continue;

      const obIdx = obCandle.index;

      if (obIdx < setupStatusIndex) continue;
      if (breakoutStatusIndex !== null && obIdx >= breakoutStatusIndex) continue;

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

  _findOBCross(type, setupOB, candleIndexMap, candles, breakoutIndex) {
    const isEQL = type === 'EQL';
    const obHigh = setupOB.high;
    const obLow = setupOB.low;

    const breakoutPos = candleIndexMap.get(breakoutIndex);
    const scanStart = breakoutPos !== undefined
      ? breakoutPos + 1
      : nextArrayIdx(candleIndexMap, candles, setupOB.index);

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
   * Scans forward from just after the OB-crossing candle maintaining a running
   * extreme (lowest low for EQL, highest high for EQH). Every time the running
   * extreme is updated AND its price matches any pattern/pattern2/pattern3 swing
   * price, that candle is recorded as a candidate.
   *
   * Returns ALL candidates in discovery order (i.e. progressively deeper
   * extremes). final.js walks this array and picks the first whose patternMatch
   * retest did not violate the currentSwing.
   */
  _findOBSetupExtreme(type, obCrossIndex, candleIndexMap, candles, p1Prices, p2Prices, p3Prices) {
    const isEQL = type === 'EQL';

    const crossPos = candleIndexMap.get(obCrossIndex);
    if (crossPos === undefined) return [];

    let extremePrice = isEQL ? Infinity : -Infinity;
    const candidates = [];

    for (let i = crossPos + 1; i < candles.length; i++) {
      const c = candles[i];
      const candlePrice = isEQL ? c.low : c.high;
      const isNewExtreme = isEQL ? candlePrice < extremePrice : candlePrice > extremePrice;
      if (!isNewExtreme) continue;

      extremePrice = candlePrice;

      if (p1Prices.has(candlePrice) || p2Prices.has(candlePrice) || p3Prices.has(candlePrice)) {
        candidates.push({
          index: c.index,
          price: candlePrice,
          formattedTime: c.formattedTime,
        });
      }
    }

    return candidates;
  }
}

module.exports = new ConfirmedSetupEngine();