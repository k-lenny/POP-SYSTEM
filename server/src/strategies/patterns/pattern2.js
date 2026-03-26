const EventEmitter = require('events');
const swingEngine = require('../../signals/dataProcessor/swings');
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

        const lowerPct = this.OTE_LOWER_RATIO * 100;
        const upperPct = this.OTE_UPPER_RATIO * 100;
        if (percentValue + 1e-9 < lowerPct || percentValue - 1e-9 > upperPct) {
          // candidate not in OTE band
          continue;
        }

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
          acceptedCandidate = { candidateSwing, vshape, percentValue, breakout, candidateIdx };
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

        const lowerPct = this.OTE_LOWER_RATIO * 100;
        const upperPct = this.OTE_UPPER_RATIO * 100;
        if (percentValue + 1e-9 < lowerPct || percentValue - 1e-9 > upperPct) {
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

        acceptedCandidate = { candidateSwing, vshape, percentValue, breakout, candidateIdx };
      }

      // If still no accepted candidate, continue
      if (!acceptedCandidate) continue;

      // Build pattern from acceptedCandidate
      const { candidateSwing, vshape, percentValue, breakout } = acceptedCandidate;
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
        level: percentRounded,
        status: 'OTE',
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
