// server/src/strategies/patterns/pattern3.js
const EventEmitter = require('events');
const swingEngine = require('../../signals/dataProcessor/swings');
const { processLV } = require('../../signals/dataProcessor/LV');
const { processOBLV } = require('../../signals/dataProcessor/OBLV');
const Logger = require('../../utils/logger');
const { getConfig } = require('../../config');

class Pattern3Engine extends EventEmitter {
  constructor(options = {}) {
    super();
    this.store = {};
    this.logger = options.logger || new Logger('Pattern3Engine');
    this.emitEvents = options.emitEvents ?? getConfig().ENABLE_EVENTS;
    this.lookaheadLimit = options.lookaheadLimit ?? 200;
    this.retestLookahead = options.retestLookahead ?? 30;
  }

  _initStore(symbol, granularity) {
    if (!this.store[symbol]) this.store[symbol] = {};
    if (!this.store[symbol][granularity]) this.store[symbol][granularity] = [];
  }

  _formatTime(timestamp) {
    if (timestamp == null) return null;
    let ts = Number(timestamp);
    if (!Number.isFinite(ts)) return null;
    if (ts > 0 && ts < 1e11) ts = ts * 1000;
    return new Date(ts).toISOString().replace('T', ' ').substring(0, 19);
  }

  _findSecondExtremeSwing(swings, firstIdx) {
    const firstSwing = swings[firstIdx];
    const type = firstSwing.type;
    const firstPrice = firstSwing.price;

    let best = null;
    let bestIdx = -1;
    let bestPrice = type === 'high' ? -Infinity : Infinity;

    const maxJ = Math.min(swings.length - 1, firstIdx + this.lookaheadLimit);

    for (let j = firstIdx + 1; j <= maxJ; j++) {
      const s = swings[j];
      if (s.type !== type) continue;

      if (type === 'high' && s.price > firstPrice) break;
      if (type === 'low' && s.price < firstPrice) break;

      if (type === 'high' && s.price > bestPrice) {
        bestPrice = s.price;
        best = s;
        bestIdx = j;
      } else if (type === 'low' && s.price < bestPrice) {
        bestPrice = s.price;
        best = s;
        bestIdx = j;
      }
    }

    return best ? { swing: best, idx: bestIdx } : null;
  }

  _findBreakoutCandle(vShapeCandle, firstSwing, secondSwing, candles, direction) {
    if (!vShapeCandle || !firstSwing || !secondSwing) return null;
    const startIdx = (secondSwing.index ?? -1) + 1;
    if (startIdx <= 0 || startIdx >= candles.length) return null;

    const firstPrice = firstSwing.price;
    let firstCrossCandle = null;

    if (direction === 'bearish') {
      const level = vShapeCandle.low;
      for (let i = startIdx; i < candles.length; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.high > firstPrice) return null;

        if (!firstCrossCandle) {
          if (c.close < level || c.open < level) return c;
          if (c.low < level) { firstCrossCandle = c; continue; }
        } else {
          if (c.close < firstCrossCandle.low || c.open < firstCrossCandle.low) return c;
          if (c.low < firstCrossCandle.low) firstCrossCandle = c;
        }
      }
    } else {
      const level = vShapeCandle.high;
      for (let i = startIdx; i < candles.length; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low < firstPrice) return null;

        if (!firstCrossCandle) {
          if (c.close > level || c.open > level) return c;
          if (c.high > level) { firstCrossCandle = c; continue; }
        } else {
          if (c.close > firstCrossCandle.high || c.open > firstCrossCandle.high) return c;
          if (c.high > firstCrossCandle.high) firstCrossCandle = c;
        }
      }
    }
    return null;
  }

  _findFirstOBNoRetest(obEntries, candles, secondSwingIndex, breakoutIndex) {
    if (!Array.isArray(obEntries) || obEntries.length === 0) return null;
    if (secondSwingIndex == null || breakoutIndex == null) return null;

    const lo = Math.min(secondSwingIndex, breakoutIndex);
    const hi = Math.max(secondSwingIndex, breakoutIndex);

    const candidates = [];
    for (const entry of obEntries) {
      if (!entry?.OB) continue;

      const obStartTime = entry.bundles?.[0]?.startTime;
      if (!obStartTime) continue;
      const obTimeSec = Math.floor(new Date(obStartTime).getTime() / 1000);
      if (!Number.isFinite(obTimeSec)) continue;

      const obIdx = candles.findIndex(c => c.time === obTimeSec);
      if (obIdx === -1) continue;
      if (obIdx < lo || obIdx > hi) continue;

      candidates.push({ obIdx, entry });
    }

    if (candidates.length === 0) return null;
    candidates.sort((a, b) => a.obIdx - b.obIdx);
    const { obIdx, entry } = candidates[0];
    return {
      index: obIdx,
      formattedTime: entry.OBFormattedTime || candles[obIdx]?.formattedTime || null,
      high: entry.OB.high,
      low: entry.OB.low,
      open: entry.OB.open,
      close: entry.OB.close,
      OBRetest: entry.OBRetest,
    };
  }

  _countNoRetestLvs(lvs, candles, vShapeIndex, secondSwingIndex) {
    if (!Array.isArray(lvs) || lvs.length === 0) return 0;
    if (vShapeIndex == null || secondSwingIndex == null) return 0;

    const lo = Math.min(vShapeIndex, secondSwingIndex);
    const hi = Math.max(vShapeIndex, secondSwingIndex);

    let count = 0;
    for (const lv of lvs) {
      if (!lv?.liquidityVoid || !lv.endTime) continue;

      const lvEndTimeSec = Math.floor(new Date(lv.endTime).getTime() / 1000);
      if (!Number.isFinite(lvEndTimeSec)) continue;

      const lvEndIndex = candles.findIndex(c => c.time === lvEndTimeSec);
      if (lvEndIndex === -1) continue;
      if (lvEndIndex < lo || lvEndIndex > hi) continue;

      const { start, end } = lv.liquidityVoid;
      const lvLow = Math.min(start, end);
      const lvHigh = Math.max(start, end);

      let retested = false;
      for (let k = lvEndIndex + 1; k <= hi; k++) {
        const c = candles[k];
        if (!c) continue;
        if (c.low <= lvHigh && c.high >= lvLow) {
          retested = true;
          break;
        }
      }

      if (!retested) count++;
    }
    return count;
  }

  _findRetestCandle(breakout, vShapePrice, secondSwingPrice, firstSwingPrice, candles, direction) {
    if (!breakout || breakout.index == null) return null;
    const startIdx = breakout.index + 1;
    if (startIdx >= candles.length) return null;

    const zoneLow = Math.min(vShapePrice, secondSwingPrice);
    const zoneHigh = Math.max(vShapePrice, secondSwingPrice);

    const scanEnd = Math.min(candles.length - 1, startIdx + this.retestLookahead);

    let extreme = null;
    let extremeIdx = -1;

    if (direction === 'bearish') {
      let maxHigh = -Infinity;
      for (let i = startIdx; i <= scanEnd; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.high > zoneHigh) break;
        if (c.high >= zoneLow && c.high <= zoneHigh && c.high > maxHigh) {
          maxHigh = c.high;
          extreme = c;
          extremeIdx = i;
        }
      }
    } else {
      let minLow = Infinity;
      for (let i = startIdx; i <= scanEnd; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low < zoneLow) break;
        if (c.low >= zoneLow && c.low <= zoneHigh && c.low < minLow) {
          minLow = c.low;
          extreme = c;
          extremeIdx = i;
        }
      }
    }

    if (!extreme) return null;

    const postStart = extremeIdx + 1;
    const postEnd = Math.min(candles.length - 1, postStart + this.retestLookahead);

    if (direction === 'bearish') {
      for (let i = postStart; i <= postEnd; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.high > firstSwingPrice) return null;
        if (c.low < extreme.low) return { candle: extreme, index: extremeIdx };
      }
      return null;
    } else {
      for (let i = postStart; i <= postEnd; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low < firstSwingPrice) return null;
        if (c.high > extreme.high) return { candle: extreme, index: extremeIdx };
      }
      return null;
    }
  }

  _findVShapeCandle(firstSwing, secondSwing, candles) {
    const startIdx = Math.min(firstSwing.index, secondSwing.index) + 1;
    const endIdx = Math.max(firstSwing.index, secondSwing.index);
    if (endIdx <= startIdx) return null;

    let extremum = null;
    if (firstSwing.type === 'high') {
      let minLow = Infinity;
      for (let i = startIdx; i < endIdx; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.low < minLow) {
          minLow = c.low;
          extremum = c;
        }
      }
    } else {
      let maxHigh = -Infinity;
      for (let i = startIdx; i < endIdx; i++) {
        const c = candles[i];
        if (!c) continue;
        if (c.high > maxHigh) {
          maxHigh = c.high;
          extremum = c;
        }
      }
    }
    return extremum;
  }

  async detect(symbol, granularity, candles) {
    this._initStore(symbol, granularity);

    await swingEngine.detectAll(symbol, granularity, candles.slice(0, -1), 1);
    const swings = swingEngine.get(symbol, granularity) || [];

    this.logger.info(`[Pattern3Engine] ${symbol} @ ${granularity}s — Candles: ${candles.length}, Swings: ${swings.length}`);

    if (swings.length < 2) {
      this.logger.warn(`[Pattern3Engine] Not enough swings (${swings.length}) for pattern detection`);
      this.store[symbol][granularity] = [];
      return [];
    }

    const patterns = [];

    for (let i = 0; i < swings.length - 1; i++) {
      const firstSwing = swings[i];
      const found = this._findSecondExtremeSwing(swings, i);
      if (!found) continue;

      const secondSwing = found.swing;

      const vShape = this._findVShapeCandle(firstSwing, secondSwing, candles);
      if (!vShape) continue;

      const direction = firstSwing.type === 'high' ? 'bearish' : 'bullish';
      const vShapePrice = firstSwing.type === 'high' ? vShape.low : vShape.high;

      // 62.5% retracement check: secondSwing must retrace at least 62.5%
      // of the range between firstSwing and vShapePrice back toward firstSwing
      const range = Math.abs(firstSwing.price - vShapePrice);
      if (range === 0) continue;

      if (direction === 'bearish') {
        // firstSwing is a high, vShapePrice is the low between them
        // secondSwing (also a high) must be >= vShapePrice + 62.5% of range
        const minSecondPrice = vShapePrice + range * 0.625;
        if (secondSwing.price < minSecondPrice) continue;
      } else {
        // firstSwing is a low, vShapePrice is the high between them
        // secondSwing (also a low) must be <= vShapePrice - 62.5% of range
        const maxSecondPrice = vShapePrice - range * 0.625;
        if (secondSwing.price > maxSecondPrice) continue;
      }

      const vShapeTime = vShape.time ?? vShape.timestamp ?? null;

      const breakout = this._findBreakoutCandle(vShape, firstSwing, secondSwing, candles, direction);
      if (!breakout) continue;
      const breakoutTime = breakout.time ?? breakout.timestamp ?? null;

      const vIdx = vShape.index ?? null;
      const sIdx = secondSwing.index ?? null;
      let numberOfNoLv = 0;
      if (vIdx != null && sIdx != null && sIdx > vIdx) {
        const windowCandles = candles.slice(vIdx, sIdx + 1);
        const windowLvs = processLV(symbol, granularity, windowCandles) || [];
        numberOfNoLv = this._countNoRetestLvs(
          windowLvs,
          windowCandles,
          0,
          windowCandles.length - 1
        );
      }

      if (numberOfNoLv !== 0 && numberOfNoLv !== 1) continue;

      let obFound = null;
      if (breakout.index > secondSwing.index) {
        const obWindowCandles = candles.slice(secondSwing.index, breakout.index + 1);
        const obEntries = processOBLV(symbol, granularity, obWindowCandles) || [];
        obFound = this._findFirstOBNoRetest(
          obEntries,
          candles,
          secondSwing.index,
          breakout.index
        );
      }

      const retestFound = this._findRetestCandle(
        breakout,
        vShapePrice,
        secondSwing.price,
        firstSwing.price,
        candles,
        direction
      );
      const retestCandle = retestFound?.candle ?? null;
      const retestIndex = retestFound?.index ?? null;
      const retestPrice = retestCandle
        ? (direction === 'bearish' ? retestCandle.high : retestCandle.low)
        : null;
      const retestTime = retestCandle?.time ?? retestCandle?.timestamp ?? null;

      const pattern = {
        type: 'PATTERN3',
        direction,

        firstSwingType: firstSwing.type,
        firstSwingPrice: firstSwing.price,
        firstSwingIndex: firstSwing.index,
        firstSwingTime: firstSwing.time ?? null,
        firstSwingFormattedTime: firstSwing.formattedTime || this._formatTime(firstSwing.time),

        secondSwingType: secondSwing.type,
        secondSwingPrice: secondSwing.price,
        secondSwingIndex: secondSwing.index,
        secondSwingTime: secondSwing.time ?? null,
        secondSwingFormattedTime: secondSwing.formattedTime || this._formatTime(secondSwing.time),

        vShapeCandleIndex: vShape.index ?? null,
        vShapeCandlePrice: vShapePrice,
        vShapeCandleHigh: vShape.high ?? null,
        vShapeCandleLow: vShape.low ?? null,
        vShapeCandleOpen: vShape.open ?? null,
        vShapeCandleClose: vShape.close ?? null,
        vShapeCandleTime: vShapeTime,
        vShapeCandleFormattedTime: vShape.formattedTime || this._formatTime(vShapeTime),

        breakoutCandleIndex: breakout?.index ?? null,
        breakoutCandlePrice: breakout ? (breakout.close ?? breakout.open ?? null) : null,
        breakoutCandleOpen: breakout?.open ?? null,
        breakoutCandleClose: breakout?.close ?? null,
        breakoutCandleHigh: breakout?.high ?? null,
        breakoutCandleLow: breakout?.low ?? null,
        breakoutCandleTime: breakoutTime,
        breakoutCandleFormattedTime: breakout?.formattedTime || this._formatTime(breakoutTime),

        NumberOfNoLv: numberOfNoLv,

        OBFound: obFound,

        retest: retestCandle ? {
          index: retestIndex,
          price: retestPrice,
          open: retestCandle.open ?? null,
          close: retestCandle.close ?? null,
          high: retestCandle.high ?? null,
          low: retestCandle.low ?? null,
          time: retestTime,
          formattedTime: retestCandle.formattedTime || this._formatTime(retestTime),
        } : null,

        timestamp: candles[secondSwing.index]?.timestamp || Date.now(),
      };

      patterns.push(pattern);

      if (this.emitEvents) {
        this.emit('patternDetected', { symbol, granularity, pattern });
      }
    }

    this.logger.info(`[Pattern3Engine] Detected ${patterns.length} patterns for ${symbol} @ ${granularity}s`);
    this.store[symbol][granularity] = patterns;
    return patterns;
  }

  get(symbol, granularity) {
    return this.store[symbol]?.[granularity] || [];
  }

  getStats(symbol, granularity) {
    const patterns = this.get(symbol, granularity);
    return {
      total: patterns.length,
      bullish: patterns.filter(p => p.direction === 'bullish').length,
      bearish: patterns.filter(p => p.direction === 'bearish').length,
    };
  }
}

module.exports = new Pattern3Engine();