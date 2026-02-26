// server/src/signals/dataProcessor/OBLV.js

const swingEngine = require('./swings');

const findNextSwing = (swing, allSwings) => {
  const swingIndex = allSwings.findIndex(s => s.time === swing.time);
  if (swing.type === 'high') {
    for (let i = swingIndex + 1; i < allSwings.length; i++) {
      if (allSwings[i].type === 'low') {
        return allSwings[i];
      }
    }
  } else { // swing.type === 'low'
    for (let i = swingIndex + 1; i < allSwings.length; i++) {
      if (allSwings[i].type === 'high') {
        return allSwings[i];
      }
    }
  }
  return null;
};

const processOBLV = (symbol, granularity, ohlcData) => {
    const swings = swingEngine.get(symbol, granularity);
    const oblvData = [];

    for (const swing of swings) {
        const nextSwing = findNextSwing(swing, swings);
        if (!nextSwing) continue;

        const isHighFirst = swing.type === 'high';
        const startIndex = ohlcData.findIndex(c => c.time === swing.time);
        const endIndex = ohlcData.findIndex(c => c.time === nextSwing.time);

        if (startIndex === -1 || endIndex === -1) continue;

        const segment = ohlcData.slice(Math.min(startIndex, endIndex), Math.max(startIndex, endIndex) + 1);

        const bundles = [];
        for (let i = 0; i <= segment.length - 3; i++) {
            bundles.push(segment.slice(i, i + 3));
        }

        if (bundles.length === 0) continue;

        const bundleData = bundles.reduce((acc, bundle, index) => {
            const [c1, c2, c3] = bundle;
            let liquidityVoid = null;
            if (isHighFirst) {
                if (c1.low > c3.high) {
                    liquidityVoid = { start: c3.high, end: c1.low };
                }
            } else { // Low first
                if (c1.high < c3.low) {
                    liquidityVoid = { start: c1.high, end: c3.low };
                }
            }

            if (liquidityVoid) {
                acc.push({
                    bundle: index + 1,
                    startTime: new Date(c1.time * 1000).toISOString(),
                    endTime: new Date(c3.time * 1000).toISOString(),
                    liquidityVoid: { ...liquidityVoid, name: `LV${index + 1}` }
                });
            }
            return acc;
        }, []);

        if (bundleData.length > 0) {
            // The first LV found is bundleData[0]. Its original index in `bundles` was `bundleData[0].bundle - 1`.
            const firstLVBundleIndex = bundleData[0].bundle - 1;
            
            // The OB is the first candle (c1) of the 3-candle pattern that forms the first LV.
            // This is the last candle before the void itself and is part of the LV's formation.
            const obCandle = segment[firstLVBundleIndex];

            oblvData.push({
                swingHighTime: isHighFirst ? new Date(swing.time * 1000).toISOString() : new Date(nextSwing.time * 1000).toISOString(),
                swingLowTime: isHighFirst ? new Date(nextSwing.time * 1000).toISOString() : new Date(swing.time * 1000).toISOString(),
                OB: obCandle ? { high: obCandle.high, low: obCandle.low, open: obCandle.open, close: obCandle.close } : null,
                OBFormattedTime: obCandle ? obCandle.formattedTime : null,
                bundles: bundleData,
            });
        }
    }

    return oblvData;
};


module.exports = { processOBLV };
