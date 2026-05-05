// server/src/signals/dataProcessor/confirmedSetup.js
const setupEngine = require('./setup');
const signalEngine = require('../signalEngine');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');
const { processOBLV } = require('./OBLV');
const patternEngine = require('../../strategies/patterns/pattern');
const pattern2Engine = require('../../strategies/patterns/pattern2');
const pattern3Engine = require('../../strategies/patterns/pattern3');

class ConfirmedSetupEngine {
  constructor() {
    // In-memory cache of statuses identified BEFORE breakout was confirmed.
    // Keyed by lockKey. Lives only for the current process session.
    //
    // Why this exists: when a setup is first seen with no breakout yet, we
    // resolve a status (e.g. DOUBLE EQ) and cache it. On a later run when
    // breakout flips to YES, we reuse the cached status instead of
    // re-evaluating — because re-evaluating against the post-sweep market
    // structure could return a different status (e.g. S SETUP).
    //
    // No disk persistence — setup.js now returns deterministic values
    // (setupVshapeDepth and impulseExtremeDepth are frozen the moment the
    // structure completes), so reruns naturally produce the same result.
    this._pendingStatuses = {};
  }

  _lockKey(symbol, granularity, setup) {
    return `${symbol}_${granularity}_${setup.brokenIndex}`;
  }

  getConfirmedSetups(symbol, granularity) {
    const setups = setupEngine.getSetups(symbol, granularity);

    console.log(`\n[ConfirmedSetup] ===== getConfirmedSetups called: ${symbol} ${granularity} =====`);
    console.log(`[ConfirmedSetup] Total raw setups from setupEngine: ${setups.length}`);

    if (!setups.length) {
      console.log(`[ConfirmedSetup] EARLY EXIT - no setups returned by setupEngine`);
      return [];
    }

    const candles = signalEngine.getCandles(symbol, granularity, true);
    console.log(`[ConfirmedSetup] Total candles available: ${candles.length}`);

    if (!candles.length) {
      console.log(`[ConfirmedSetup] EARLY EXIT - no candles returned by signalEngine`);
      return [];
    }

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

    // Dedup guard — setupEngine sometimes returns duplicate brokenIndex entries
    const seenKeys = new Set();

    const confirmedSetups = [];

    for (const setup of setups) {
      const setupId = `brokenIndex=${setup.brokenIndex} type=${setup.type}`;

      // ── Dedup guard ───────────────────────────────────────────────────────
      const dedupKey = `${setup.brokenIndex}_${setup.type}`;
      if (seenKeys.has(dedupKey)) {
        console.log(`[ConfirmedSetup] SKIPPING duplicate setup: ${setupId}`);
        continue;
      }
      seenKeys.add(dedupKey);

      console.log(`\n[ConfirmedSetup] --- Processing setup: ${setupId} ---`);

      // ── Guard: null / non-finite values ──────────────────────────────────
      if (
        setup.setupVshapeDepth === null ||
        setup.preBreakoutVDepth === null ||
        setup.impulseExtremeDepth === null ||
        !isFinite(setup.preBreakoutVDepth)
      ) {
        console.log(`[ConfirmedSetup] DROPPED [null/infinity guard] ${setupId}`, {
          setupVshapeDepth: setup.setupVshapeDepth,
          preBreakoutVDepth: setup.preBreakoutVDepth,
          impulseExtremeDepth: setup.impulseExtremeDepth,
        });
        continue;
      }

      const lockKey = this._lockKey(symbol, granularity, setup);

      const breakoutResult = this._getBreakoutStatus(setup, candles, candleIndexMap);
      console.log(`[ConfirmedSetup] breakoutResult:`, breakoutResult);

      const breakoutPos = breakoutResult.index !== null
        ? candleIndexMap.get(breakoutResult.index)
        : undefined;
      const sSetupScanEnd = breakoutPos !== undefined ? breakoutPos : candles.length;

      let status;
      let isValid;

      if (breakoutResult.status === 'YES') {
        // ── Breakout confirmed ────────────────────────────────────────────────
        // Priority 1: use the status cached from a previous run when breakout
        //   was still NO. This protects setups that transitioned during a
        //   session — e.g. DOUBLE EQ identified, then breakout flipped YES on
        //   the next run. Re-evaluating now would see post-breakout structure
        //   and could return a different status.
        //
        // Priority 2: no cache exists (breakout already YES on first observation).
        //   Evaluate at breakoutPos as the scan ceiling — reconstruct exactly
        //   what the market looked like AT the moment of breakout, nothing after.

        const pendingStatus = this._pendingStatuses[lockKey];

        if (pendingStatus) {
          status = pendingStatus;
          isValid = true;
          console.log(`[ConfirmedSetup] Breakout YES — using pre-breakout cached status "${status}" for "${lockKey}"`);
        } else {
          const result = this._getSetupStatus(setup, candles, candleIndexMap, breakoutPos);
          status = result.status;
          isValid = true; // breakout happened — never drop based on isValid

          console.log(`[ConfirmedSetup] _getSetupStatus (at-breakout) result:`, { status, originalIsValid: result.isValid });

          if (status === null) {
            // Dead zone — no condition matched at breakout time. Drop silently.
            console.log(`[ConfirmedSetup] Breakout YES but no condition matched — dropping ${setupId}`);
            this._pendingStatuses[lockKey] = null;
            continue;
          }
        }

        // Cache the resolved status so future runs in this session reuse it
        this._pendingStatuses[lockKey] = status;
        console.log(`[ConfirmedSetup] Breakout YES — locked in-memory as "${status}" for "${lockKey}"`);

      } else {
        // ── No breakout yet — full normal validation ─────────────────────────
        const result = this._getSetupStatus(setup, candles, candleIndexMap, sSetupScanEnd);
        status = result.status;
        isValid = result.isValid;
        console.log(`[ConfirmedSetup] _getSetupStatus result:`, { status, isValid });

        // Cache the pre-breakout status for the case where breakout flips YES later.
        // Only cache valid resolved statuses — never cache S SETUP FAILED or null.
        if (isValid && status !== null) {
          this._pendingStatuses[lockKey] = status;
          console.log(`[ConfirmedSetup] No breakout yet — caching pre-breakout status "${status}" for "${lockKey}"`);
        }
      }

      // ── Whitelist of allowed final statuses ───────────────────────────────
      // Anything else (S SETUP FAILED, null, undefined, etc.) is dropped.
      const ALLOWED_STATUSES = new Set(['OTE', 'DOUBLE EQ', 'S SETUP']);
      if (!ALLOWED_STATUSES.has(status)) {
        console.log(`[ConfirmedSetup] DROPPED [non-allowed status="${status}", isValid=${isValid}] ${setupId}`);
        continue;
      }

      console.log(`[ConfirmedSetup] PASSED all guards — status="${status}" breakout="${breakoutResult.status}"`);

      const setupOB = this._findSetupOB(
        oblvData,
        ftMap,
        setup.setupVshapeIndex,
        breakoutResult.index,
        candles,
        candleIndexMap,
        setup.type
      );
      console.log(`[ConfirmedSetup] setupOB found:`, setupOB ? `index=${setupOB.index}` : 'null');

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
        console.log(`[ConfirmedSetup] OBCross result:`, {
          obCrossIndex: obCrossResult.obCrossIndex,
          obCrossCandle: obCrossResult.obCrossCandle ? `index=${obCrossResult.obCrossCandle.index}` : null,
        });

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
          OBSetupExtremeCandidates = this._findOBSetupExtreme(
            setup.type,
            obCrossResult.obCrossIndex,
            candleIndexMap,
            candles,
            p1Prices,
            p2Prices,
            p3Prices
          );
          console.log(`[ConfirmedSetup] OBSetupExtremeCandidates count: ${OBSetupExtremeCandidates.length}`);
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

    console.log(`\n[ConfirmedSetup] ===== RESULT: ${confirmedSetups.length} confirmed setups for ${symbol} ${granularity} =====\n`);
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

    const setupId = `brokenIndex=${setup.brokenIndex} type=${type}`;

    const impulseRange = Math.abs(preBreakoutVDepth - impulseExtremeDepth);
    console.log(`[_getSetupStatus] ${setupId} — impulseRange=${impulseRange} setupVshapeDepth=${setupVshapeDepth}`);

    if (impulseRange > 0) {
      const oteLowerBound = type === 'EQL'
        ? impulseExtremeDepth + (impulseRange * 0.625)
        : impulseExtremeDepth - (impulseRange * 0.79);
      const oteUpperBound = type === 'EQL'
        ? impulseExtremeDepth + (impulseRange * 0.79)
        : impulseExtremeDepth - (impulseRange * 0.625);

      console.log(`[_getSetupStatus] OTE bounds — lower=${oteLowerBound} upper=${oteUpperBound}`);

      if (type === 'EQL' && setupVshapeDepth >= oteLowerBound && setupVshapeDepth <= oteUpperBound) {
        console.log(`[_getSetupStatus] → OTE matched (EQL)`);
        return { status: 'OTE', isValid: true };
      }
      if (type === 'EQH' && setupVshapeDepth <= oteUpperBound && setupVshapeDepth >= oteLowerBound) {
        console.log(`[_getSetupStatus] → OTE matched (EQH)`);
        return { status: 'OTE', isValid: true };
      }
    }

    const preBreakoutVPos = candleIndexMap.get(preBreakoutVIndex);
    if (preBreakoutVPos !== undefined) {
      const preBreakoutVCandle = candles[preBreakoutVPos];
      if (type === 'EQL') {
        const preBreakoutVKeyPrice = Math.max(preBreakoutVCandle.open, preBreakoutVCandle.close);
        console.log(`[_getSetupStatus] DOUBLE EQ check (EQL) — preBreakoutVDepth=${preBreakoutVDepth} keyPrice=${preBreakoutVKeyPrice} setupVshapeDepth=${setupVshapeDepth}`);
        if (setupVshapeDepth < preBreakoutVDepth && setupVshapeDepth > preBreakoutVKeyPrice) {
          console.log(`[_getSetupStatus] → DOUBLE EQ matched (EQL)`);
          return { status: 'DOUBLE EQ', isValid: true };
        }
      } else {
        const preBreakoutVKeyPrice = Math.min(preBreakoutVCandle.open, preBreakoutVCandle.close);
        console.log(`[_getSetupStatus] DOUBLE EQ check (EQH) — preBreakoutVDepth=${preBreakoutVDepth} keyPrice=${preBreakoutVKeyPrice} setupVshapeDepth=${setupVshapeDepth}`);
        if (setupVshapeDepth > preBreakoutVDepth && setupVshapeDepth < preBreakoutVKeyPrice) {
          console.log(`[_getSetupStatus] → DOUBLE EQ matched (EQH)`);
          return { status: 'DOUBLE EQ', isValid: true };
        }
      }
    } else {
      console.log(`[_getSetupStatus] preBreakoutVIndex=${preBreakoutVIndex} not found in candleIndexMap — skipping DOUBLE EQ check`);
    }

    const setupVPos = candleIndexMap.get(setupVshapeIndex);
    if (setupVPos !== undefined) {
      const isSweep =
        (type === 'EQL' && setupVshapeDepth > preBreakoutVDepth) ||
        (type === 'EQH' && setupVshapeDepth < preBreakoutVDepth);

      console.log(`[_getSetupStatus] isSweep=${isSweep} (setupVshapeDepth=${setupVshapeDepth} preBreakoutVDepth=${preBreakoutVDepth})`);

      if (isSweep) {
        const preBreakoutVPos2 = candleIndexMap.get(preBreakoutVIndex);
        let firstCrosser = null;
        let firstCrosserPos = -1;

        if (preBreakoutVPos2 !== undefined) {
          for (let i = preBreakoutVPos2 + 1; i <= setupVPos; i++) {
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
          console.log(`[_getSetupStatus] → S SETUP FAILED — no firstCrosser found between preBreakoutVPos and setupVPos`);
          return { status: 'S SETUP FAILED', isValid: false };
        }

        console.log(`[_getSetupStatus] firstCrosser found at pos=${firstCrosserPos} index=${firstCrosser.index}`);

        let refCandle = firstCrosser;
        for (let i = firstCrosserPos + 1; i < sSetupScanEnd; i++) {
          const c = candles[i];
          if (type === 'EQL') {
            if (c.open > refCandle.high || c.close > refCandle.high) {
              console.log(`[_getSetupStatus] → S SETUP FAILED — candle at pos=${i} index=${c.index} broke above refCandle.high=${refCandle.high} (open=${c.open} close=${c.close})`);
              return { status: 'S SETUP FAILED', isValid: false };
            }
            if (c.high > refCandle.high) refCandle = c;
          } else {
            if (c.open < refCandle.low || c.close < refCandle.low) {
              console.log(`[_getSetupStatus] → S SETUP FAILED — candle at pos=${i} index=${c.index} broke below refCandle.low=${refCandle.low} (open=${c.open} close=${c.close})`);
              return { status: 'S SETUP FAILED', isValid: false };
            }
            if (c.low < refCandle.low) refCandle = c;
          }
        }

        console.log(`[_getSetupStatus] → S SETUP matched`);
        return { status: 'S SETUP', isValid: true };
      }
    } else {
      console.log(`[_getSetupStatus] setupVshapeIndex=${setupVshapeIndex} not found in candleIndexMap — skipping sweep check`);
    }

    console.log(`[_getSetupStatus] → No condition matched — returning status=null`);
    return { status: null, isValid: true };
  }

  _getBreakoutStatus(setup, candles, candleIndexMap) {
    const startScanPos = nextArrayIdx(candleIndexMap, candles, setup.setupVshapeIndex);
    if (startScanPos === undefined) {
      console.log(`[_getBreakoutStatus] No candle after setupVshapeIndex=${setup.setupVshapeIndex} — returning NO`);
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