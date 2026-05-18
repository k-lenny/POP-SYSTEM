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
      const nextC = candles[maxIdx];
      if (nextC && nextC.high > (extremum?.high ?? -Infinity)) extremum = nextC;
    } else {
      let minLow = Infinity;
      for (let i = start; i < end; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low < minLow) { minLow = c.low; extremum = c; }
      }
      const prevC = candles[minIdx];
      if (prevC && prevC.low < (extremum?.low ?? Infinity)) extremum = prevC;
      const nextC = candles[maxIdx];
      if (nextC && nextC.low < (extremum?.low ?? Infinity)) extremum = nextC;
    }
    return extremum;
  }

  _identifyBreakoutSimple(level, candles, startIndex, direction, currentSwing) {
    let firstCrossCandle = null;

    for (let i = startIndex; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (c.index === currentSwing.index) continue;

      if (direction === 'bullish') {
        if (c.low < currentSwing.low) return null;

        if (!firstCrossCandle) {
          if (c.close > level || c.open > level) return c;
          if (c.high > level) { firstCrossCandle = c; continue; }
        } else {
          if (c.close > firstCrossCandle.high || c.open > firstCrossCandle.high) return c;
          if (c.high > firstCrossCandle.high) firstCrossCandle = c;
        }
      } else {
        if (c.high > currentSwing.high) return null;

        if (!firstCrossCandle) {
          if (c.close < level || c.open < level) return c;
          if (c.low < level) { firstCrossCandle = c; continue; }
        } else {
          if (c.close < firstCrossCandle.low || c.open < firstCrossCandle.low) return c;
          if (c.low < firstCrossCandle.low) firstCrossCandle = c;
        }
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

  _resolveStartIdx(retestData, breakoutData, candles) {
    let idx = retestData?.index ?? null;
    if (idx == null && breakoutData?.index != null) idx = breakoutData.index + 1;
    return (idx == null || idx >= candles.length) ? null : idx;
  }

  _hasCrossedPrice(candles, startIdx, firstPrice, direction) {
    for (let i = startIdx; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === 'bullish' && c.low < firstPrice) return true;
      if (direction === 'bearish' && c.high > firstPrice) return true;
    }
    return false;
  }

  /**
   * Returns the index of the first candle after retestIndex whose body breaks
   * the retestVshapePrice, or null if no such candle exists.
   * Bullish: close or open > retestVshapePrice
   * Bearish: close or open < retestVshapePrice
   */
  _findRetestVshapeLockIndex(candles, retestIndex, retestVshapePrice, direction) {
    if (retestIndex == null || retestVshapePrice == null) return null;
    for (let i = retestIndex + 1; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === 'bullish' && (c.close > retestVshapePrice || c.open > retestVshapePrice)) return i;
      if (direction === 'bearish' && (c.close < retestVshapePrice || c.open < retestVshapePrice)) return i;
    }
    return null;
  }

  /**
   * Returns the index of the first candle from startIdx onward that crossed
   * firstPrice in the given direction, or null if none.
   */
  _findFirstSwingCrossIndex(candles, startIdx, firstPrice, direction) {
    for (let i = startIdx; i < candles.length; i++) {
      const c = candles[i];
      if (!c) continue;
      if (direction === 'bullish' && c.low < firstPrice) return i;
      if (direction === 'bearish' && c.high > firstPrice) return i;
    }
    return null;
  }

  // Shared wick-touch → body-entry → expiry scan used by both status methods.
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
   * Compute retestOBStatus.
   * Returns 'yes' if the retestData price interacted with the OB zone.
   * Returns 'no' if the retest price never reached the OB zone.
   * Returns null if retestPrice or OB bounds are unavailable.
   */
  _computeRetestOBStatus(retestPrice, obData, direction) {
    if (retestPrice == null || !obData || obData.high == null || obData.low == null) return null;

    if (direction === 'bullish') {
      return retestPrice <= obData.high ? 'yes' : 'no';
    } else {
      return retestPrice >= obData.low ? 'yes' : 'no';
    }
  }

  /**
   * Find first OB (by candle index) between secondSwingIndex and breakoutIndex where:
   *   Bullish: OB.low < vShapePrice
   *   Bearish: OB.high > vShapePrice
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
   * Find retest candle after breakout.
   * Bullish: within [secondSwingPrice, vshapePrice]; bearish: within [vshapePrice, secondSwingPrice].
   *
   * No scan limit — scans all candles after the breakout.
   *
   * The scan finds the first TWO distinct retest candidates whose retestVshape gets
   * body-broken (close or open, not a wick), in chronological order:
   *
   *   Phase 1 — scan forward:
   *     For each candle in the zone, track the running extreme retest candidate and its
   *     retestVshapePrice (highest high / lowest low from secondSwingIndex to that candle).
   *     When a candle body-breaks the current extreme candidate's retestVshapePrice,
   *     that candidate is recorded as the Nth confirmed candidate (N=1 first, N=2 second).
   *     After recording, reset and continue scanning for the next candidate.
   *
   *   Phase 2 — pick winner:
   *     Of the (up to two) confirmed candidates, return the one with the more extreme
   *     retest price (lower low for bullish, higher high for bearish).
   *     If only one candidate was confirmed, return it.
   *     If neither was confirmed, return null.
   */
  _findRetest(vshape, secondSwing, breakout, candles, direction) {
    if (!breakout || !vshape || !secondSwing) return null;

    const breakoutStartIdx = breakout.index + 1;
    if (breakoutStartIdx >= candles.length) return null;

    const vshapePrice = direction === 'bullish' ? vshape.high : vshape.low;
    const secondSwingPrice = secondSwing.price;
    const secondSwingIndex = secondSwing.index;

    const computeVshapePrice = (candIdx) => {
      if (direction === 'bullish') {
        let maxHigh = -Infinity;
        for (let k = secondSwingIndex; k <= candIdx; k++) {
          const kc = candles[k];
          if (kc && kc.high > maxHigh) maxHigh = kc.high;
        }
        return maxHigh === -Infinity ? null : maxHigh;
      } else {
        let minLow = Infinity;
        for (let k = secondSwingIndex; k <= candIdx; k++) {
          const kc = candles[k];
          if (kc && kc.low < minLow) minLow = kc.low;
        }
        return minLow === Infinity ? null : minLow;
      }
    };

    const bodyBreaks = (c, threshold) => {
      if (threshold == null) return false;
      if (direction === 'bullish') return c.close > threshold || c.open > threshold;
      return c.close < threshold || c.open < threshold;
    };

    const isMoreExtreme = (a, b) =>
      direction === 'bullish' ? a.low < b.low : a.high > b.high;

    const confirmed = [];

    let currentExtreme = null;
    let currentExtremeVshapePrice = null;

    for (let i = breakoutStartIdx; i < candles.length; i++) {
      if (confirmed.length === 2) break;

      const c = candles[i];
      if (!c) continue;

      const inZone = direction === 'bullish'
        ? (c.low >= secondSwingPrice && c.low <= vshapePrice)
        : (c.high <= secondSwingPrice && c.high >= vshapePrice);

      if (inZone) {
        if (currentExtreme === null || isMoreExtreme(c, currentExtreme)) {
          currentExtreme = c;
          currentExtremeVshapePrice = computeVshapePrice(i);
        }
      }

      if (currentExtreme !== null && bodyBreaks(c, currentExtremeVshapePrice)) {
        confirmed.push(currentExtreme);
        currentExtreme = null;
        currentExtremeVshapePrice = null;
      }
    }

    if (confirmed.length === 0) return null;
    if (confirmed.length === 1) return confirmed[0];

    return isMoreExtreme(confirmed[0], confirmed[1]) ? confirmed[0] : confirmed[1];
  }

  /**
   * Helper: ensure there is no intervening same-type swing between firstIdx and candidateIdx
   * that is more extreme than firstSwing.
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
   * Check G — Body-break of firstSwing's opposite extreme.
   *
   * Scans candles from firstSwing.index to candidateSwing.index (inclusive).
   * At least one candle in that range must have body-broken beyond the firstSwing
   * candle's opposite extreme:
   *
   *   Bullish (two lows): at least one candle where close > firstSwingCandle.high
   *                       OR open > firstSwingCandle.high
   *   Bearish (two highs): at least one candle where close < firstSwingCandle.low
   *                        OR open < firstSwingCandle.low
   *
   * Returns true if the condition is met, false otherwise.
   */
  _hasBodyBrokenFirstSwingOppositeExtreme(firstSwing, candidateSwing, enrichedCandles, direction) {
    const firstCandle = enrichedCandles[firstSwing.index];
    if (!firstCandle) return false;

    const start = firstSwing.index;
    const end = candidateSwing.index;

    if (direction === 'bullish') {
      const threshold = firstCandle.high;
      for (let i = start; i <= end; i++) {
        const c = enrichedCandles[i];
        if (!c) continue;
        if (c.close > threshold || c.open > threshold) return true;
      }
    } else {
      const threshold = firstCandle.low;
      for (let i = start; i <= end; i++) {
        const c = enrichedCandles[i];
        if (!c) continue;
        if (c.close < threshold || c.open < threshold) return true;
      }
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
   * Retest locking: the lock (retestVshape body-break) is only valid if it occurred
   * BEFORE the firstSwing was crossed. If firstSwing was crossed before the lock
   * was established, the retest is nullified regardless.
   *
   * Check G: before accepting any candidate as secondSwing, at least one candle
   * between firstSwing.index and candidateSwing.index (inclusive) must have
   * body-broken the firstSwing candle's opposite extreme:
   *   Bullish: close or open > firstSwingCandle.high
   *   Bearish: close or open < firstSwingCandle.low
   */
  async detect(symbol, granularity, candles) {
    this._initStore(symbol, granularity);

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

    const ftMap = {};
    for (const c of enrichedCandles) {
      if (c.formattedTime && !(c.formattedTime in ftMap)) ftMap[c.formattedTime] = c;
    }

    const lowerPct = this.OTE_LOWER_RATIO * 100;
    const upperPct = this.OTE_UPPER_RATIO * 100;

    const patterns = [];

    let pairsChecked = 0;
    let noCandidates = 0;
    let noDirection = 0;
    let noVShape = 0;
    let noBreakout = 0;
    let failedOTE = 0;
    let failedBodyBreak = 0;

    for (let i = 0; i < swings.length - 1; i++) {
      const firstSwing = swings[i];
      pairsChecked++;

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

      let acceptedCandidate = null;

      for (const c of candidates) {
        const candidateSwing = c.swing;
        const candidateIdx = c.idx;

        if (this._hasInterveningMoreExtremeSwing(swings, i, candidateIdx, firstSwing)) {
          if (this.debug) {
            this.logger.debug(`[Pattern2Engine] Pair invalidated by intervening more-extreme swing: firstIdx=${i}, candidateIdx=${candidateIdx}`);
          }
          continue;
        }

        const direction = this._getPatternDirection(firstSwing, candidateSwing);
        if (!direction) continue;

        // Check G: body must have broken firstSwing's opposite extreme between the two swings
        if (!this._hasBodyBrokenFirstSwingOppositeExtreme(firstSwing, candidateSwing, enrichedCandles, direction)) {
          if (this.debug) {
            this.logger.debug(`[Pattern2Engine] Candidate rejected — no body-break of firstSwing opposite extreme: firstIdx=${i}, candidateIdx=${candidateIdx}`);
          }
          failedBodyBreak++;
          continue;
        }

        const vshape = this._findVShapeSimple(firstSwing, candidateSwing, enrichedCandles, direction);
        if (!vshape) continue;

        const vshapePrice = direction === 'bullish' ? vshape.high : vshape.low;
        if (vshapePrice == null) continue;

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

        const isDoubleEQ = this._isDoubleEQ(firstSwing, candidateSwing, enrichedCandles, direction);
        const inOTERange = percentValue + 1e-9 >= lowerPct && percentValue - 1e-9 <= upperPct;

        if (!isDoubleEQ && !inOTERange) continue;

        const breakout = this._identifyBreakoutSimple(
          direction === 'bullish' ? vshape.high : vshape.low,
          enrichedCandles,
          candidateSwing.index,
          direction,
          candidateSwing
        );

        if (breakout) {
          acceptedCandidate = { candidateSwing, vshape, percentValue, breakout, candidateIdx, isDoubleEQ };
          break;
        }
      }

      if (!acceptedCandidate) {
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

        // Check G (fallback path): body must have broken firstSwing's opposite extreme
        if (!this._hasBodyBrokenFirstSwingOppositeExtreme(firstSwing, candidateSwing, enrichedCandles, direction)) {
          if (this.debug) {
            this.logger.debug(`[Pattern2Engine] Fallback candidate rejected — no body-break of firstSwing opposite extreme: firstIdx=${i}, candidateIdx=${candidateIdx}`);
          }
          failedBodyBreak++;
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

        const isDoubleEQ = this._isDoubleEQ(firstSwing, candidateSwing, enrichedCandles, direction);
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

      if (!acceptedCandidate) continue;

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

      const breakoutIndex = breakout?.index ?? null;
      const breakoutPrice = breakout?.close ?? breakout?.price ?? breakout?.high ?? breakout?.low ?? null;
      const breakoutFormattedTime =
        enrichedCandles[breakoutIndex]?.formattedTime
        ?? this._formatTimeFromMs(enrichedCandles[breakoutIndex]?.timestampMs)
        ?? breakout?.formattedTime
        ?? null;

      const obData = this._findOBData(oblvData, candidateSwing.index, breakoutIndex, vshapePrice, direction, ftMap);

      const mitigationBlock = this._findMitigationBlock(firstSwing.index, candidateSwing.index, enrichedCandles, direction);
      const mitigationBlockIndex = mitigationBlock?.index ?? null;
      const mitigationBlockPrice = direction === 'bullish' ? mitigationBlock?.low ?? null : mitigationBlock?.high ?? null;
      const mitigationBlockFormattedTime = mitigationBlock
        ? (enrichedCandles[mitigationBlockIndex]?.formattedTime
          ?? this._formatTimeFromMs(enrichedCandles[mitigationBlockIndex]?.timestampMs)
          ?? mitigationBlock?.formattedTime
          ?? null)
        : null;

      const retestRaw = this._findRetest(vshape, candidateSwing, breakout, enrichedCandles, direction);

      const retestVshapeRaw = retestRaw
        ? this._findVShapeSimple(
            { index: candidateSwing.index },
            { index: retestRaw.index },
            enrichedCandles,
            direction
          )
        : null;
      const retestVshapeIndexRaw = retestVshapeRaw?.index ?? null;
      const retestVshapePriceRaw = retestVshapeRaw
        ? (direction === 'bullish' ? retestVshapeRaw.high : retestVshapeRaw.low)
        : null;

      // Find the candle index at which the retestVshape lock was established.
      // Find the candle index at which firstSwing was first crossed after breakout.
      // Lock is only valid if it was established BEFORE firstSwing was crossed.
      const retestLockIndex = retestRaw != null
        ? this._findRetestVshapeLockIndex(enrichedCandles, retestRaw.index, retestVshapePriceRaw, direction)
        : null;

      const firstSwingCrossIndex = this._findFirstSwingCrossIndex(
        enrichedCandles, breakoutIndex + 1, firstPrice, direction
      );

      // Lock is only valid if it happened before firstSwing was crossed (or firstSwing never crossed)
      const retestVshapeBroken = retestLockIndex != null &&
        (firstSwingCrossIndex === null || retestLockIndex < firstSwingCrossIndex);

      // firstSwingCrossed nullifies the retest only when the lock was NOT established first
      const firstSwingCrossed = !retestVshapeBroken && firstSwingCrossIndex !== null;

      const retest = firstSwingCrossed ? null : retestRaw;
      const retestIndex = retest?.index ?? null;
      const retestPrice = direction === 'bullish' ? retest?.low : retest?.high;
      const retestFormattedTime = retest
        ? (enrichedCandles[retestIndex]?.formattedTime
          ?? this._formatTimeFromMs(enrichedCandles[retestIndex]?.timestampMs)
          ?? retest?.formattedTime
          ?? null)
        : null;

      const retestVshape = retest ? retestVshapeRaw : null;
      const retestVshapeIndex = retest ? retestVshapeIndexRaw : null;
      const retestVshapePrice = retest ? retestVshapePriceRaw : null;
      const retestVshapeFormattedTime = retestVshape
        ? (enrichedCandles[retestVshapeIndex]?.formattedTime
          ?? this._formatTimeFromMs(enrichedCandles[retestVshapeIndex]?.timestampMs)
          ?? retestVshape?.formattedTime
          ?? null)
        : null;

      // ─── Retest Status ───────────────────────────────────────────────────────
      // Priority order:
      //   1. 'locked'   — lock established BEFORE firstSwing was crossed
      //   2. 'expired'  — NOT locked AND firstSwing was crossed (retest never formed or pre-empted)
      //   3. 'invalid'  — retest exists, NOT locked, AND firstSwing later crossed
      //   4. 'valid'    — retest exists, NOT locked, firstSwing NOT crossed
      //   5. null       — no retest found
      let retestStatus = null;
      if (retestVshapeBroken) {
        retestStatus = 'locked';
      } else if (firstSwingCrossed) {
        retestStatus = 'expired';
      } else if (retestIndex != null) {
        retestStatus = this._hasCrossedPrice(enrichedCandles, retestIndex + 1, firstPrice, direction)
          ? 'invalid'
          : 'valid';
      }
      // ─────────────────────────────────────────────────────────────────────────

      const isRetestCompromised = retestStatus === 'expired' || retestStatus === 'invalid';
      const finalMitigationStatus = isRetestCompromised ? 'expired' : this._computeMitigationStatus(
        { index: retestIndex }, { price: mitigationBlockPrice }, { index: breakoutIndex }, enrichedCandles, direction
      );
      const finalOBStatus = isRetestCompromised ? 'expired' : this._computeOBStatus(
        { index: retestIndex }, obData, { index: breakoutIndex }, enrichedCandles, direction
      );
      const retestOBStatus = this._computeRetestOBStatus(retestPrice ?? null, obData, direction);

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
        breakout,
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
        retestVshapeData: {
          price: retestVshapePrice,
          index: retestVshapeIndex,
          formattedTime: retestVshapeFormattedTime,
          high: retestVshape?.high ?? null,
          low: retestVshape?.low ?? null,
          open: retestVshape?.open ?? null,
          close: retestVshape?.close ?? null
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
        retestOBStatus,
        level: percentRounded,
        status: status,
        timestamp: enrichedCandles[candidateSwing.index]?.timestampMs || Date.now()
      };

      patterns.push(this._enrichPatternMetadata(pattern));
      if (this.emitEvents) this.emit('patternDetected', { symbol, granularity, pattern });
    }

    this.logger.info(`[Pattern2Engine] Detection Summary for ${symbol} ${granularity}:`);
    this.logger.info(`  Pairs Checked: ${pairsChecked}`);
    this.logger.info(`  Rejected - No Candidates: ${noCandidates}`);
    this.logger.info(`  Rejected - No V-Shape: ${noVShape}`);
    this.logger.info(`  Rejected - No Breakout: ${noBreakout}`);
    this.logger.info(`  Rejected - Failed OTE: ${failedOTE}`);
    this.logger.info(`  Rejected - Failed Body-Break Check: ${failedBodyBreak}`);
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
      retestVshapeData: {
        price: pattern.retestVshapeData?.price ?? null,
        index: pattern.retestVshapeData?.index ?? null,
        formattedTime: pattern.retestVshapeData?.formattedTime ?? null,
        high: pattern.retestVshapeData?.high ?? null,
        low: pattern.retestVshapeData?.low ?? null,
        open: pattern.retestVshapeData?.open ?? null,
        close: pattern.retestVshapeData?.close ?? null
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
      retestOBStatus: pattern.retestOBStatus ?? null,
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