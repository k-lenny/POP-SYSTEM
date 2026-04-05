const EventEmitter = require('events');
const swingEngine = require('../../signals/dataProcessor/swings');
const { processOBLV } = require('../../signals/dataProcessor/OBLV');
const Logger = require('../../utils/logger');
const { getConfig } = require('../../config');

class Pattern2Engine extends EventEmitter {
  constructor(options = {}) {
    super();
    this.store = {};
    this.logger = options.logger || new Logger('Pattern2Engine');
    this.emitEvents = options.emitEvents ?? getConfig().ENABLE_EVENTS;
    this.debug = options.debug ?? false;

    // Minimal instance fields
    this.lookaheadLimit = options.lookaheadLimit ?? 200; // tuneable scan limit

    // OTE ratios
    this.OTE_LOWER_RATIO = 0.625;
    this.OTE_UPPER_RATIO = 0.805;
    this.EPSILON = 1e-8;
  }

  _initStore(symbol, granularity) {
    if (!this.store[symbol]) this.store[symbol] = {};
    if (!this.store[symbol][granularity]) this.store[symbol][granularity] = [];
  }

  _enrichCandles(candles) {
    return candles.map((c, idx) => {
      let ts = null;

      if (c.timestamp != null) {
        ts = Number(c.timestamp);
        if (ts > 0 && ts < 1e11) ts = ts * 1000;
      } else if (c.time != null) {
        ts = Number(c.time);
        if (ts > 0 && ts < 1e11) ts = ts * 1000;
      } else if (c.formattedTime) {
        const parsed = Date.parse(c.formattedTime);
        if (!Number.isNaN(parsed)) ts = parsed;
      }

      const formattedTime = ts ? new Date(ts).toISOString().replace('T', ' ').substring(0, 19) : null;

      return {
        ...c,
        index: idx,
        bodySize: Math.abs((c.open ?? 0) - (c.close ?? 0)),
        upperWick: (c.high ?? 0) - Math.max(c.open ?? 0, c.close ?? 0),
        lowerWick: Math.min(c.open ?? 0, c.close ?? 0) - (c.low ?? 0),
        timestampMs: ts,
        formattedTime
      };
    });
  }

  _formatTimeFromMs(ms) {
    if (!ms) return null;
    try {
      return new Date(Number(ms)).toISOString().replace('T', ' ').substring(0, 19);
    } catch (e) {
      return null;
    }
  }

  _getPatternDirection(firstSwing, secondSwing) {
    if (firstSwing.type === 'low' && secondSwing.type === 'low') return 'bullish';
    if (firstSwing.type === 'high' && secondSwing.type === 'high') return 'bearish';
    return null;
  }

  _findVShapeSimple(s1, s2, candles, direction) {
    const minIdx = Math.min(s1.index, s2.index);
    const maxIdx = Math.max(s1.index, s2.index);
    const start = minIdx + 1;
    const end = maxIdx;
    if (end <= start) return null;

    let extremum = null;
    if (direction === 'bullish') {
      let maxHigh = -Infinity;
      for (let i = start; i < end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.high > maxHigh) { maxHigh = c.high; extremum = c; }
      }
      const prevC = candles[minIdx];
      if (prevC && prevC.high > (extremum?.high ?? -Infinity)) extremum = prevC;
    } else {
      let minLow = Infinity;
      for (let i = start; i < end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low < minLow) { minLow = c.low; extremum = c; }
      }
      const prevC = candles[minIdx];
      if (prevC && prevC.low < (extremum?.low ?? Infinity)) extremum = prevC;
    }
    return extremum;
  }

  _identifyBreakoutSimple(level, candles, startIndex, direction, currentSwing) {
    for (let i = startIndex; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (c.index === currentSwing.index) continue;
      if (direction === 'bullish') {
        if (c.low < currentSwing.low) return null;
        if (c.close > level) return c;
      } else {
        if (c.high > currentSwing.high) return null;
        if (c.close < level) return c;
      }
    }
    return null;
  }

  /**
   * Check if pattern qualifies as DOUBLE EQ
   * Bullish: secondSwing.low is between min(firstSwing.close, firstSwing.open) and firstSwing.low
   * Bearish: secondSwing.high is between max(firstSwing.close, firstSwing.open) and firstSwing.high
   */
  _isDoubleEQ(firstSwing, secondSwing, enrichedCandles, direction) {
    const firstCandle = enrichedCandles[firstSwing.index];
    const secondCandle = enrichedCandles[secondSwing.index];
    
    if (!firstCandle || !secondCandle) return false;

    if (direction === 'bullish') {
      const bodyBottom = Math.min(firstCandle.open, firstCandle.close);
      const candleLow = firstCandle.low;
      const secondLow = secondCandle.low;
      
      return secondLow >= candleLow && secondLow <= bodyBottom;
    } else {
      const bodyTop = Math.max(firstCandle.open, firstCandle.close);
      const candleHigh = firstCandle.high;
      const secondHigh = secondCandle.high;
      
      return secondHigh <= candleHigh && secondHigh >= bodyTop;
    }
  }

  /**
   * Find mitigation block candle between firstSwingIndex and secondSwingIndex
   * Bullish: green candle (close > open) with highest high in range
   * Bearish: red candle (close < open) with lowest low in range
   */
  _findMitigationBlock(firstSwingIndex, secondSwingIndex, candles, direction) {
    const start = Math.min(firstSwingIndex, secondSwingIndex);
    const end = Math.max(firstSwingIndex, secondSwingIndex);

    let result = null;

    if (direction === 'bullish') {
      let maxHigh = -Infinity;
      for (let i = start; i <= end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.close > c.open && c.high > maxHigh) {
          maxHigh = c.high;
          result = c;
        }
      }
    } else {
      let minLow = Infinity;
      for (let i = start; i <= end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.close < c.open && c.low < minLow) {
          minLow = c.low;
          result = c;
        }
      }
    }

    return result;
  }

  /**
   * Compute mitigation status relative to the mitigationBlock price.
   * Scans candles from retestData.index (or breakoutIndex+1 if no retest).
   *
   * Bullish — checks candles going below mitigationPrice:
   *   false   : price never reached mitigationPrice
   *   true    : first touch was a wick only (low < price, but open & close >= price)
   *             OR first candle to close/open below it had NO subsequent candle
   *             closing or opening below that candle's low
   *   expired : first candle to close/open below mitigationPrice was followed by
   *             a subsequent candle closing or opening below that candle's low
   *
   * Bearish — mirror logic above the mitigationPrice.
   * Scan starts from retestData.index (falls back to breakoutIndex+1).
   */
  _resolveStartIdx(retestData, breakoutData, candles) {
    let idx = retestData?.index ?? null;
    if (idx == null && breakoutData?.index != null) idx = breakoutData.index + 1;
    return (idx == null || idx >= candles.length) ? null : idx;
  }

  // Returns true if any candle from startIdx onward crossed firstPrice in the given direction
  _hasCrossedPrice(candles, startIdx, firstPrice, direction) {
    for (let i = startIdx; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === 'bullish' && c.low < firstPrice) return true;
      if (direction === 'bearish' && c.high > firstPrice) return true;
    }
    return false;
  }

  // Shared wick-touch → body-entry → expiry scan used by both status methods.
  // reaches(c): true when a candle first interacts with the zone
  // isWick(c): true when body stayed outside (wick-only touch)
  // isExpired(next, firstC): true when a subsequent candle invalidates the entry
  _scanCandleInteraction(startIdx, candles, reaches, isWick, isExpired) {
    let wickTouched = false;
    for (let i = startIdx; i < candles.length; i++) {
      const c = candles[i];
      if (!c || !reaches(c)) continue;
      if (isWick(c)) { wickTouched = true; continue; }
      for (let j = i + 1; j < candles.length; j++) {
        const next = candles[j];
        if (!next) continue;
        if (isExpired(next, c)) return 'expired';
      }
      return true;
    }
    return wickTouched;
  }

  _computeMitigationStatus(retestData, mitigationBlockData, breakoutData, enrichedCandles, direction) {
    const p = mitigationBlockData?.price ?? null;
    if (p == null) return null;
    const startIdx = this._resolveStartIdx(retestData, breakoutData, enrichedCandles);
    if (startIdx == null) return false;

    return direction === 'bullish'
      ? this._scanCandleInteraction(startIdx, enrichedCandles,
          c => c.low < p,
          c => c.close >= p && c.open >= p,
          (next, fc) => next.close < fc.low || next.open < fc.low)
      : this._scanCandleInteraction(startIdx, enrichedCandles,
          c => c.high > p,
          c => c.close <= p && c.open <= p,
          (next, fc) => next.close > fc.high || next.open > fc.high);
  }

  _computeOBStatus(retestData, obData, breakoutData, enrichedCandles, direction) {
    if (!obData || obData.high == null || obData.low == null) return null;
    const startIdx = this._resolveStartIdx(retestData, breakoutData, enrichedCandles);
    if (startIdx == null) return false;

    return direction === 'bullish'
      ? this._scanCandleInteraction(startIdx, enrichedCandles,
          c => c.low <= obData.high,
          c => c.close > obData.high && c.open > obData.high,
          next => next.close < obData.low || next.open < obData.low)
      : this._scanCandleInteraction(startIdx, enrichedCandles,
          c => c.high >= obData.low,
          c => c.close < obData.low && c.open < obData.low,
          next => next.close > obData.high || next.open > obData.high);
  }

  /**
   * Find first OB (by candle index) between secondSwingIndex and breakoutIndex where:
   *   Bullish: OB.low < vShapePrice (lowest low of OB stayed below vShape high)
   *   Bearish: OB.high > vShapePrice (highest high of OB stayed above vShape low)
   */
  _findOBData(oblvData, secondSwingIndex, breakoutIndex, vShapePrice, direction, ftMap) {
    let best = null;

    for (const entry of oblvData) {
      if (!entry.OB || !entry.OBFormattedTime) continue;
      const obCandle = ftMap[entry.OBFormattedTime];
      if (!obCandle) continue;

      const obIndex = obCandle.index;
      if (obIndex < secondSwingIndex) continue;
      if (breakoutIndex != null && obIndex > breakoutIndex) continue;

      if (direction === 'bullish' && entry.OB.low >= vShapePrice) continue;
      if (direction === 'bearish' && entry.OB.high <= vShapePrice) continue;

      if (best === null || obIndex < best.index) {
        best = {
          price: direction === 'bullish' ? entry.OB.low : entry.OB.high,
          index: obIndex,
          formattedTime: entry.OBFormattedTime,
          high: entry.OB.high,
          low: entry.OB.low
        };
      }
    }

    return best;
  }

  /**
   * Find retest candle after breakout
   * Bullish: extreme low between vShapeData and secondSwing after breakout
   * Bearish: extreme high between vShapeData and secondSwing after breakout
   * Returns null if no valid retest found
   */
  _findRetest(vshape, secondSwing, breakout, candles, direction) {
    if (!breakout || !vshape || !secondSwing) return null;
    
    const startIdx = breakout.index + 1;
    if (startIdx >= candles.length) return null;

    const vshapePrice = direction === 'bullish' ? vshape.high : vshape.low;
    const secondSwingPrice = secondSwing.price;

    let retestCandle = null;

    if (direction === 'bullish') {
      // Find extreme low between vshapePrice and secondSwingPrice
      let extremeLow = Infinity;
      for (let i = startIdx; i < candles.length; i++) {
        const c = candles[i];
        if (!c) continue;
        
        // Check if low is between secondSwing and vShape
        if (c.low >= secondSwingPrice && c.low <= vshapePrice) {
          if (c.low < extremeLow) {
            extremeLow = c.low;
            retestCandle = c;
          }
        }
      }
    } else {
      // Bearish: Find extreme high between secondSwingPrice and vshapePrice
      let extremeHigh = -Infinity;
      for (let i = startIdx; i < candles.length; i++) {
        const c = candles[i];
        if (!c) continue;
        
        // Check if high is between vShape and secondSwing
        if (c.high <= secondSwingPrice && c.high >= vshapePrice) {
          if (c.high > extremeHigh) {
            extremeHigh = c.high;
            retestCandle = c;
          }
        }
      }
    }

    return retestCandle;
  }

  /**
   * Helper: ensure there is no intervening same-type swing between firstIdx and candidateIdx
   * that is more extreme than firstSwing. For highs: no intervening high > firstSwing.price.
   * For lows: no intervening low < firstSwing.price.
   */
  _hasInterveningMoreExtremeSwing(swings, firstIdx, candidateIdx, firstSwing) {
    const type = firstSwing.type;
    const firstPrice = firstSwing.price;
    for (let k = firstIdx + 1; k < candidateIdx; k++) {
      const s = swings[k];
      if (!s) continue;
      if (s.type !== type) continue;
      if (type === 'high' && s.price > firstPrice) return true;
      if (type === 'low' && s.price < firstPrice) return true;
    }
    return false;
  }

  /**
   * detect
   * - For each swing treat it as firstSwing
   * - scan forward for same-type swings (bounded by lookaheadLimit)
   * - prefer the earliest same-type candidate that:
   *     • sits in the OTE band relative to first->vshape AND
   *     • has a breakout after that candidate
   * - if no such chronological candidate exists, fallback to the previous "extreme" selection among candidates
   *   (still requires v-shape + breakout + OTE qualification)
   *
   * Important fix: before accepting a pair (firstSwing, candidateSwing) we ensure there is NO intervening
   * same-type swing between them that is more extreme than firstSwing. This prevents invalid pairs like:
   *   100 (first), 104 (intervening higher high), 99 (candidate)  -> invalid (104 invalidates first=100)
   *
   * The returned pattern includes breakoutData with price, index and formattedTime.
   * Status is either 'OTE' or 'DOUBLE EQ' based on qualification.
   */
  async detect(symbol, granularity, candles) {
    this._initStore(symbol, granularity);

    // Ensure swings are detected before proceeding
    await swingEngine.detectAll(symbol, granularity, candles.slice(0, -1), 1);

    const swings = swingEngine.get(symbol, granularity) || [];
    this.logger.info(`[Pattern2Engine] Detecting patterns for ${symbol} ${granularity}`);
    this.logger.info(`[Pattern2Engine] Candles: ${candles.length}, Swings: ${swings.length}`);

    if (swings.length < 2) {
      this.logger.warn(`[Pattern2Engine] Not enough swings (${swings.length}) for pattern detection`);
      return [];
    }

    const enrichedCandles = this._enrichCandles(candles);
    const oblvData = processOBLV(symbol, granularity, enrichedCandles);

    // Build formattedTime -> enriched candle map once for OB lookups
    const ftMap = {};
    for (const c of enrichedCandles) {
      if (c.formattedTime && !(c.formattedTime in ftMap)) ftMap[c.formattedTime] = c;
    }

    // Hoist OTE band constants out of the inner loops
    const lowerPct = this.OTE_LOWER_RATIO * 100;
    const upperPct = this.OTE_UPPER_RATIO * 100;

    const patterns = [];

    let pairsChecked = 0;
    let noCandidates = 0;
    let noDirection = 0;
    let noVShape = 0;
    let noBreakout = 0;
    let failedOTE = 0;

    for (let i = 0; i < swings.length - 1; i++) {
      const firstSwing = swings[i];
      pairsChecked++;

      // gather same-type candidates ahead (bounded)
      const candidates = [];
      const maxJ = Math.min(swings.length - 1, i + this.lookaheadLimit);
      for (let j = i + 1; j <= maxJ; j++) {
        if (swings[j].type === firstSwing.type) {
          candidates.push({ swing: swings[j], idx: j });
        }
      }

      if (candidates.length === 0) {
        noCandidates++;
        continue;
      }

      // 1) Prefer earliest chronological candidate that qualifies AND has a breakout after it.
      let acceptedCandidate = null;

      for (const c of candidates) {
        const candidateSwing = c.swing;
        const candidateIdx = c.idx;

        // Reject if there is an intervening same-type swing more extreme than firstSwing
        if (this._hasInterveningMoreExtremeSwing(swings, i, candidateIdx, firstSwing)) {
          // invalid pair because a more extreme same-type swing exists between first and candidate
          if (this.debug) {
            this.logger.debug(`[Pattern2Engine] Pair invalidated by intervening more-extreme swing: firstIdx=${i}, candidateIdx=${candidateIdx}`);
          }
          continue;
        }

        // direction (same type by construction)
        const direction = this._getPatternDirection(firstSwing, candidateSwing);
        if (!direction) {
          continue;
        }

        // v-shape between first and this candidate
        const vshape = this._findVShapeSimple(firstSwing, candidateSwing, enrichedCandles, direction);
        if (!vshape) {
          continue;
        }

        const vshapePrice = direction === 'bullish' ? vshape.high : vshape.low;
        if (vshapePrice == null) continue;

        // compute range and percent for this candidate
        const isBullish = direction === 'bullish';
        const firstPrice = firstSwing.price;
        const secondPrice = candidateSwing.price;

        if (firstPrice == null || secondPrice == null) continue;

        const range = isBullish ? (vshapePrice - firstPrice) : (firstPrice - vshapePrice);
        if (!(range > this.EPSILON)) continue;

        const percentValue = isBullish
          ? ((vshapePrice - secondPrice) / range) * 100
          : ((secondPrice - vshapePrice) / range) * 100;

        if (!Number.isFinite(percentValue)) continue;

        // Check if this is DOUBLE EQ (independent of OTE percentage)
        const isDoubleEQ = this._isDoubleEQ(firstSwing, candidateSwing, enrichedCandles, direction);

        // Check OTE range only if NOT DOUBLE EQ
        const inOTERange = percentValue + 1e-9 >= lowerPct && percentValue - 1e-9 <= upperPct;

        if (!isDoubleEQ && !inOTERange) continue;

        // check breakout AFTER this candidate (start from candidate.index)
        const breakout = this._identifyBreakoutSimple(
          direction === 'bullish' ? vshape.high : vshape.low,
          enrichedCandles,
          candidateSwing.index,
          direction,
          candidateSwing
        );

        if (breakout) {
          // This chronological candidate qualifies and has a breakout after it — accept it.
          acceptedCandidate = { candidateSwing, vshape, percentValue, breakout, candidateIdx, isDoubleEQ };
          break;
        }
        // else continue scanning candidates
      }

      // 2) If no chronological candidate accepted, fallback to extreme selection among candidates
      if (!acceptedCandidate) {
        // pick extreme candidate
        let extreme = candidates[0];
        for (const c of candidates) {
          if (firstSwing.type === 'high') {
            if (c.swing.price > extreme.swing.price) extreme = c;
          } else {
            if (c.swing.price < extreme.swing.price) extreme = c;
          }
        }

        const candidateSwing = extreme.swing;
        const candidateIdx = extreme.idx;

        // Reject if there is an intervening same-type swing more extreme than firstSwing
        if (this._hasInterveningMoreExtremeSwing(swings, i, candidateIdx, firstSwing)) {
          if (this.debug) {
            this.logger.debug(`[Pattern2Engine] Fallback extreme invalidated by intervening more-extreme swing: firstIdx=${i}, candidateIdx=${candidateIdx}`);
          }
          noCandidates++;
          continue;
        }

        const direction = this._getPatternDirection(firstSwing, candidateSwing);
        if (!direction) {
          noDirection++;
          continue;
        }

        const vshape = this._findVShapeSimple(firstSwing, candidateSwing, enrichedCandles, direction);
        if (!vshape) {
          noVShape++;
          continue;
        }

        const vshapePrice = direction === 'bullish' ? vshape.high : vshape.low;
        if (vshapePrice == null) {
          noVShape++;
          continue;
        }

        const isBullish = direction === 'bullish';
        const firstPrice = firstSwing.price;
        const secondPrice = candidateSwing.price;

        if (firstPrice == null || secondPrice == null) continue;

        const range = isBullish ? (vshapePrice - firstPrice) : (firstPrice - vshapePrice);
        if (!(range > this.EPSILON)) continue;

        const percentValue = isBullish
          ? ((vshapePrice - secondPrice) / range) * 100
          : ((secondPrice - vshapePrice) / range) * 100;

        if (!Number.isFinite(percentValue)) {
          failedOTE++;
          continue;
        }

        // Check if this is DOUBLE EQ (independent of OTE percentage)
        const isDoubleEQ = this._isDoubleEQ(firstSwing, candidateSwing, enrichedCandles, direction);

        // Check OTE range only if NOT DOUBLE EQ
        const inOTERange = percentValue + 1e-9 >= lowerPct && percentValue - 1e-9 <= upperPct;

        if (!isDoubleEQ && !inOTERange) {
          failedOTE++;
          continue;
        }

        const breakout = this._identifyBreakoutSimple(
          direction === 'bullish' ? vshape.high : vshape.low,
          enrichedCandles,
          candidateSwing.index,
          direction,
          candidateSwing
        );

        if (!breakout) {
          noBreakout++;
          continue;
        }

        acceptedCandidate = { candidateSwing, vshape, percentValue, breakout, candidateIdx, isDoubleEQ };
      }

      // If still no accepted candidate, continue
      if (!acceptedCandidate) continue;

      // Build pattern from acceptedCandidate
      const { candidateSwing, vshape, percentValue, breakout, isDoubleEQ } = acceptedCandidate;
      const direction = this._getPatternDirection(firstSwing, candidateSwing);
      const isBullish = direction === 'bullish';
      const firstPrice = firstSwing.price;
      const secondPrice = candidateSwing.price;

      const vshapePrice = isBullish ? vshape.high : vshape.low;
      const vshapeIndex = vshape.index ?? null;
      const vshapeFormattedTime =
        enrichedCandles[vshapeIndex]?.formattedTime
        ?? this._formatTimeFromMs(enrichedCandles[vshapeIndex]?.timestampMs)
        ?? vshape.formattedTime
        ?? null;

      // breakout metadata
      const breakoutIndex = breakout?.index ?? null;
      const breakoutPrice = breakout?.close ?? breakout?.price ?? breakout?.high ?? breakout?.low ?? null;
      const breakoutFormattedTime =
        enrichedCandles[breakoutIndex]?.formattedTime
        ?? this._formatTimeFromMs(enrichedCandles[breakoutIndex]?.timestampMs)
        ?? breakout?.formattedTime
        ?? null;

      // Find first qualifying OB scanning from secondSwingIndex
      const obData = this._findOBData(oblvData, candidateSwing.index, breakoutIndex, vshapePrice, direction, ftMap);

      // Find mitigation block between firstSwing and secondSwing
      const mitigationBlock = this._findMitigationBlock(firstSwing.index, candidateSwing.index, enrichedCandles, direction);
      const mitigationBlockIndex = mitigationBlock?.index ?? null;
      const mitigationBlockPrice = direction === 'bullish' ? mitigationBlock?.low ?? null : mitigationBlock?.high ?? null;
      const mitigationBlockFormattedTime = mitigationBlock
        ? (enrichedCandles[mitigationBlockIndex]?.formattedTime
          ?? this._formatTimeFromMs(enrichedCandles[mitigationBlockIndex]?.timestampMs)
          ?? mitigationBlock?.formattedTime
          ?? null)
        : null;

      // Check if price crossed firstSwing after breakout — if so, retest is nullified
      const firstSwingCrossed = breakoutIndex != null &&
        this._hasCrossedPrice(enrichedCandles, breakoutIndex + 1, firstPrice, direction);

      const retest = firstSwingCrossed ? null : this._findRetest(vshape, candidateSwing, breakout, enrichedCandles, direction);
      const retestIndex = retest?.index ?? null;
      const retestPrice = direction === 'bullish' ? retest?.low : retest?.high;
      const retestFormattedTime = retest
        ? (enrichedCandles[retestIndex]?.formattedTime
          ?? this._formatTimeFromMs(enrichedCandles[retestIndex]?.timestampMs)
          ?? retest?.formattedTime
          ?? null)
        : null;

      // Compute retest status
      let retestStatus = null;
      if (firstSwingCrossed) {
        retestStatus = 'expired';
      } else if (retestIndex != null) {
        retestStatus = this._hasCrossedPrice(enrichedCandles, retestIndex + 1, firstPrice, direction)
          ? 'invalid'
          : 'valid';
      }

      // Cascade expired to status fields if retest was compromised; skip expensive scans otherwise
      const isRetestCompromised = retestStatus === 'expired' || retestStatus === 'invalid';
      const finalMitigationStatus = isRetestCompromised ? 'expired' : this._computeMitigationStatus(
        { index: retestIndex }, { price: mitigationBlockPrice }, { index: breakoutIndex }, enrichedCandles, direction
      );
      const finalOBStatus = isRetestCompromised ? 'expired' : this._computeOBStatus(
        { index: retestIndex }, obData, { index: breakoutIndex }, enrichedCandles, direction
      );

      const percentRounded = Math.round(percentValue) + '%';

      const firstFormattedTime =
        enrichedCandles[firstSwing.index]?.formattedTime
        ?? this._formatTimeFromMs(enrichedCandles[firstSwing.index]?.timestampMs)
        ?? firstSwing.formattedTime
        ?? null;

      const secondFormattedTime =
        enrichedCandles[candidateSwing.index]?.formattedTime
        ?? this._formatTimeFromMs(enrichedCandles[candidateSwing.index]?.timestampMs)
        ?? candidateSwing.formattedTime
        ?? null;

      // Determine status: DOUBLE EQ or OTE (isDoubleEQ already computed)
      const status = isDoubleEQ ? 'DOUBLE EQ' : 'OTE';

      const pattern = {
        type: 'PATTERN',
        direction,
        firstSwingPrice: firstPrice,
        firstSwingIndex: firstSwing.index,
        firstSwingFormattedTime: firstFormattedTime,
        secondSwingPrice: secondPrice,
        secondSwingIndex: candidateSwing.index,
        secondSwingFormattedTime: secondFormattedTime,
        vShapeData: {
          price: vshapePrice,
          index: vshapeIndex,
          formattedTime: vshapeFormattedTime,
          high: vshape.high ?? null,
          low: vshape.low ?? null,
          open: vshape.open ?? null,
          close: vshape.close ?? null
        },
        // breakout object (original candle) is still included for full context
        breakout,
        // breakoutData field with price, index and formattedTime
        breakoutData: {
          price: breakoutPrice,
          index: breakoutIndex,
          formattedTime: breakoutFormattedTime
        },
        retestData: {
          price: retestPrice ?? null,
          index: retestIndex,
          formattedTime: retestFormattedTime
        },
        mitigationBlockData: {
          price: mitigationBlockPrice,
          index: mitigationBlockIndex,
          formattedTime: mitigationBlockFormattedTime
        },
        OBData: {
          price: obData?.price ?? null,
          index: obData?.index ?? null,
          formattedTime: obData?.formattedTime ?? null,
          high: obData?.high ?? null,
          low: obData?.low ?? null
        },
        retestStatus,
        mitigationStatusData: finalMitigationStatus,
        OBStatus: finalOBStatus,
        level: percentRounded,
        status: status,
        timestamp: enrichedCandles[candidateSwing.index]?.timestampMs || Date.now()
      };

      patterns.push(this._enrichPatternMetadata(pattern));
      if (this.emitEvents) this.emit('patternDetected', { symbol, granularity, pattern });

      // optional: skip ahead to avoid overlapping pairs using same first swing
      // i = acceptedCandidate.candidateIdx - 1; // uncomment to advance outer loop past the selected second swing
    }

    this.logger.info(`[Pattern2Engine] Detection Summary for ${symbol} ${granularity}:`);
    this.logger.info(`  Pairs Checked: ${pairsChecked}`);
    this.logger.info(`  Rejected - No Candidates: ${noCandidates}`);
    this.logger.info(`  Rejected - No V-Shape: ${noVShape}`);
    this.logger.info(`  Rejected - No Breakout: ${noBreakout}`);
    this.logger.info(`  Rejected - Failed OTE: ${failedOTE}`);
    this.logger.info(`  ✓ Patterns Found: ${patterns.length}`);

    this.store[symbol][granularity] = patterns;
    return patterns;
  }

  _enrichPatternMetadata(pattern) {
    return {
      type: 'PATTERN2',
      direction: pattern.direction,
      firstSwingPrice: pattern.firstSwingPrice ?? null,
      firstSwingIndex: pattern.firstSwingIndex ?? null,
      firstSwingFormattedTime: pattern.firstSwingFormattedTime ?? null,
      secondSwingPrice: pattern.secondSwingPrice ?? null,
      secondSwingIndex: pattern.secondSwingIndex ?? null,
      secondSwingFormattedTime: pattern.secondSwingFormattedTime ?? null,
      vShapeData: {
        price: pattern.vShapeData?.price ?? null,
        index: pattern.vShapeData?.index ?? null,
        formattedTime: pattern.vShapeData?.formattedTime ?? null,
        high: pattern.vShapeData?.high ?? null,
        low: pattern.vShapeData?.low ?? null,
        open: pattern.vShapeData?.open ?? null,
        close: pattern.vShapeData?.close ?? null
      },
      breakoutData: {
        price: pattern.breakoutData?.price ?? null,
        index: pattern.breakoutData?.index ?? null,
        formattedTime: pattern.breakoutData?.formattedTime ?? null
      },
      retestData: {
        price: pattern.retestData?.price ?? null,
        index: pattern.retestData?.index ?? null,
        formattedTime: pattern.retestData?.formattedTime ?? null
      },
      mitigationBlockData: {
        price: pattern.mitigationBlockData?.price ?? null,
        index: pattern.mitigationBlockData?.index ?? null,
        formattedTime: pattern.mitigationBlockData?.formattedTime ?? null
      },
      OBData: {
        price: pattern.OBData?.price ?? null,
        index: pattern.OBData?.index ?? null,
        formattedTime: pattern.OBData?.formattedTime ?? null,
        high: pattern.OBData?.high ?? null,
        low: pattern.OBData?.low ?? null
      },
      retestStatus: pattern.retestStatus ?? null,
      mitigationStatusData: pattern.mitigationStatusData ?? null,
      OBStatus: pattern.OBStatus ?? null,
      level: pattern.level ?? null,
      status: pattern.status ?? null,
      timestamp: pattern.timestamp ?? null
    };
  }

  get(symbol, granularity) {
    return this.store[symbol]?.[granularity] || [];
  }

  getStats(symbol, granularity) {
    const patterns = this.get(symbol, granularity);
    return {
      total: patterns.length,
      bullish: patterns.filter(p => p.direction === 'bullish').length,
      bearish: patterns.filter(p => p.direction === 'bearish').length
    };
  }

  clearOld(symbol, granularity, maxAge) {
    const patterns = this.get(symbol, granularity);
    const cutoff = Date.now() - maxAge;
    this.store[symbol][granularity] = patterns.filter(p => p.timestamp > cutoff);
  }
}

module.exports = new Pattern2Engine();