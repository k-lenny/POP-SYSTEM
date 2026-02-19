// server/src/utils/dataProcessorUtils.js

/**
 * Build a Map from candle.index → array position.
 * @param {Array} candles - Array of candle objects with .index property.
 * @returns {Map<number, number>}
 */
function buildCandleIndexMap(candles) {
  const map = new Map();
  candles.forEach((c, i) => map.set(c.index, i));
  return map;
}

/**
 * Given a candle index, return the array index of the next candle.
 * Handles non‑sequential indices (weekends, gaps) safely.
 * @param {Map<number, number>} candleIndexMap - Map from candle.index → array position.
 * @param {Array} candles - Full candle array.
 * @param {number} afterCandleIndex - The .index value of the reference candle.
 * @returns {number|undefined} Array index of the next candle, or undefined if none.
 */
function nextArrayIdx(candleIndexMap, candles, afterCandleIndex) {
  const pos = candleIndexMap.get(afterCandleIndex);
  if (pos === undefined) return undefined;
  const next = pos + 1;
  return next < candles.length ? next : undefined;
}

/**
 * Insert an item into a sorted array using binary search.
 * Default comparator compares by .time (numeric).
 * @param {Array} array - Sorted array.
 * @param {*} item - Item to insert.
 * @param {Function} comparator - (a, b) => negative if a < b, zero if equal, positive if a > b.
 */
function sortedInsert(array, item, comparator = (a, b) => a.time - b.time) {
  let lo = 0, hi = array.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (comparator(array[mid], item) <= 0) lo = mid + 1;
    else hi = mid;
  }
  array.splice(lo, 0, item);
}

/**
 * Simple counter class for tracking counts by key.
 */
class Counter {
  constructor(initial = {}) {
    this.counts = { ...initial };
  }

  inc(key) {
    this.counts[key] = (this.counts[key] || 0) + 1;
  }

  dec(key) {
    if (this.counts[key]) this.counts[key]--;
  }

  get(key) {
    return this.counts[key] || 0;
  }

  reset(initial = {}) {
    this.counts = { ...initial };
  }
}

module.exports = {
  buildCandleIndexMap,
  nextArrayIdx,
  sortedInsert,
  Counter,
};