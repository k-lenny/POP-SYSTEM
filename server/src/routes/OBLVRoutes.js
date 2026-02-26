// server/src/routes/OBLVRoutes.js
const express = require('express');
const router = express.Router();
const { processOBLV } = require('../signals/dataProcessor/OBLV');
const signalEngine = require('../signals/signalEngine');
const swingEngine = require('../signals/dataProcessor/swings');

async function ensureDataLoaded(symbol, granularity) {
  // 1. Ensure Candles
  let candles = signalEngine.getCandles(symbol, granularity, true);
  if (!candles || candles.length === 0) {
    try { signalEngine.subscribeSymbol(symbol, granularity); } catch (e) {}
    
    // Wait for data
    const start = Date.now();
    while(Date.now() - start < 5000) { // 5 second timeout
         await new Promise(r => setTimeout(r, 500));
         candles = signalEngine.getCandles(symbol, granularity, true);
         if(candles && candles.length > 10) break;
    }
  }

  // 2. Ensure Swings
  if (candles && candles.length > 0) {
    const swings = swingEngine.get(symbol, granularity);
    if (swings.length === 0) {
      await swingEngine.detectAll(symbol, granularity, candles);
    }
  }
}

router.get('/:symbol/:granularity', async (req, res) => {
    try {
        const { symbol, granularity: granularityStr } = req.params;
        const granularity = parseInt(granularityStr, 10);

        if (!symbol) {
          return res.status(400).json({ error: 'Symbol parameter is required.' });
        }
        if (isNaN(granularity) || granularity <= 0) {
          return res.status(400).json({ error: 'Granularity must be a positive integer.' });
        }

        await ensureDataLoaded(symbol.toUpperCase(), granularity);
        const ohlcData = signalEngine.getCandles(symbol.toUpperCase(), granularity);

        if (!ohlcData || ohlcData.length === 0) {
            return res.status(404).json({ message: 'No OHLC data found for the given symbol and granularity.' });
        }

        const oblvResults = processOBLV(symbol.toUpperCase(), granularity, ohlcData);
        res.json(oblvResults);
    } catch (error) {
        console.error('Failed to process OBLV data:', error);
        res.status(500).json({ message: 'Error processing OBLV data.' });
    }
});

module.exports = router;
