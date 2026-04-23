// server/src/signals/dataProcessor/LV.js
const { processOBLV } = require('./OBLV');

/**
 * Extracts only the Liquidity Voids (LVs) from the OBLV processor output.
 */
const processLV = (symbol, granularity, ohlcData) => {
  const oblvData = processOBLV(symbol, granularity, ohlcData);
  const lvs = [];

  for (const entry of oblvData) {
    if (!entry.bundles || entry.bundles.length === 0) continue;
    for (const bundle of entry.bundles) {
      if (!bundle.liquidityVoid) continue;

      // Check if price returned between the start and end of the LV after it formed
      let lvRetest = 'no';
      const lvEndTimeSec = Math.floor(new Date(bundle.endTime).getTime() / 1000);
      const lvEndIndex = ohlcData.findIndex(c => c.time === lvEndTimeSec);
      if (lvEndIndex !== -1) {
        const { start, end } = bundle.liquidityVoid;
        const lvLow = Math.min(start, end);
        const lvHigh = Math.max(start, end);
        for (let k = lvEndIndex + 1; k < ohlcData.length; k++) {
          const c = ohlcData[k];
          if (c.low <= lvHigh && c.high >= lvLow) {
            lvRetest = 'yes';
            break;
          }
        }
      }

      lvs.push({
        swingHighTime: entry.swingHighTime,
        swingLowTime: entry.swingLowTime,
        bundle: bundle.bundle,
        startTime: bundle.startTime,
        endTime: bundle.endTime,
        liquidityVoid: bundle.liquidityVoid,
        LVRetest: lvRetest
      });
    }
  }

  return lvs;
};

const processLVWithSummary = (symbol, granularity, ohlcData) => {
  const lvs = processLV(symbol, granularity, ohlcData);

  return {
    success: true,
    symbol,
    granularity,
    totalCandles: ohlcData ? ohlcData.length : 0,
    totalLVs: lvs.length,
    data: lvs
  };
};

module.exports = { processLV, processLVWithSummary };
