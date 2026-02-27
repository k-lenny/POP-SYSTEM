const swingEngine = require('./swings');

/**
 * Finds all swings after the current one
 */
const getNextSwings = (swing, allSwings) => {
  const swingIndex = allSwings.findIndex((s) => s.time === swing.time);
  if (swingIndex === -1) return [];
  return allSwings.slice(swingIndex + 1); // return all swings after current
};

const processOBLV = (symbol, granularity, ohlcData) => {
  const swings = swingEngine.get(symbol, granularity);
  const oblvData = [];

  for (let i = 0; i < swings.length; i++) {
    const swing = swings[i];
    const isHighFirst = swing.type === 'high';
    const nextSwings = getNextSwings(swing, swings);

    if (nextSwings.length === 0) continue;

    // Progressive check: keep looking until LV is found or no more swings
    let lvFound = false;
    for (const nextSwing of nextSwings) {
      const startIndex = ohlcData.findIndex(c => c.time === swing.time);
      const endIndex = ohlcData.findIndex(c => c.time === nextSwing.time);
      if (startIndex === -1 || endIndex === -1) continue;

      const segment = ohlcData.slice(Math.min(startIndex, endIndex), Math.max(startIndex, endIndex) + 1);
      if (segment.length < 3) continue;

      const bundleData = [];

      for (let j = 0; j <= segment.length - 3; j++) {
        const c1 = segment[j];
        const c3 = segment[j + 2];
        let liquidityVoid = null;

        if (isHighFirst) {
          if (c1.low > c3.high) {
            liquidityVoid = { start: c3.high, end: c1.low };
          }
        } else {
          if (c1.high < c3.low) {
            liquidityVoid = { start: c1.high, end: c3.low };
          }
        }

        if (liquidityVoid) {
          bundleData.push({
            bundle: j + 1,
            startTime: new Date(c1.time * 1000).toISOString(),
            endTime: new Date(c3.time * 1000).toISOString(),
            liquidityVoid: { ...liquidityVoid, name: `LV${j + 1}` }
          });
        }
      }

      if (bundleData.length > 0) {
        lvFound = true; // LV found, stop checking further swings

        const firstLVBundleIndex = bundleData[0].bundle - 1;
        const obCandle = segment[firstLVBundleIndex];

        oblvData.push({
          swingHighTime: isHighFirst
            ? new Date(swing.time * 1000).toISOString()
            : new Date(nextSwing.time * 1000).toISOString(),
          swingLowTime: isHighFirst
            ? new Date(nextSwing.time * 1000).toISOString()
            : new Date(swing.time * 1000).toISOString(),
          OB: obCandle
            ? {
                high: obCandle.high,
                low: obCandle.low,
                open: obCandle.open,
                close: obCandle.close
              }
            : null,
          OBFormattedTime: obCandle ? obCandle.formattedTime : null,
          bundles: bundleData
        });

        break; // stop looking at further swings
      }
    }
  }

  return oblvData;
};

module.exports = { processOBLV };