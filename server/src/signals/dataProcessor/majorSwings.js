// server/src/signals/dataProcessor/majorSwings.js
const swingEngine = require('./swings');
const signalEngine = require('../signalEngine');
const { buildCandleIndexMap } = require('../../utils/dataProcessorUtils');

class MajorSwingsEngine {
  /**
   * Identifies Major Swings based on candle confirmation or subsequent structure,
   * with an invalidation condition.
   * @param {string} symbol
   * @param {number} granularity
   */
  getMajorSwings(symbol, granularity) {
    const swings = swingEngine.get(symbol, granularity);
    const candles = signalEngine.getCandles(symbol, granularity, true);

    if (!swings.length || !candles.length) return [];

    const majorSwings = [];
    const candleIndexMap = buildCandleIndexMap(candles);

    let activeMajorLow = null;
    let activeMajorHigh = null;

    for (let i = 0; i < swings.length; i++) {
      const swing = swings[i];
      let isMajor = false;
      let reason = null;

      const swingCandlePosition = candleIndexMap.get(swing.index);
      if (swingCandlePosition === undefined) continue;

      // Find the invalidation point for this swing
      let invalidationIndex = -1;
      for (let k = swingCandlePosition + 1; k < candles.length; k++) {
        const candle = candles[k];
        if (swing.type === 'low' && candle.close < swing.price) {
          invalidationIndex = candle.index;
          break;
        }
        if (swing.type === 'high' && candle.close > swing.price) {
          invalidationIndex = candle.index;
          break;
        }
      }

      // If no invalidation, the boundary is the end of the available candles
      const boundaryIndex = invalidationIndex !== -1 ? invalidationIndex : candles[candles.length - 1].index + 1;

      if (swing.type === 'low') {
        // ─── MAJOR SWING LOW LOGIC ───

        // Condition A: Chain of 3 candles closing above previous highs
        let chainCount = 0;
        let referenceLevel = -Infinity; // Will track the High of the previous confirming candle

        for (let k = swingCandlePosition + 1; k < candles.length; k++) {
          const candle = candles[k];
          if (candle.index >= boundaryIndex) break; // Stop at the boundary
          
          if (chainCount === 0) {
            // First candle must just close above the swing price
            if (candle.close > swing.price) {
              chainCount = 1;
              referenceLevel = candle.high;
            }
          } else {
            // Subsequent candles must close above the PREVIOUS confirming candle's HIGH
            if (candle.close > referenceLevel) {
              chainCount++;
              referenceLevel = candle.high;
            }
          }
          
          if (chainCount >= 3) break;
        }

        if (chainCount >= 3) {
          isMajor = true;
          reason = `3 Candle Chain Closing Above Highs`;
        } else {
          // Condition B: 4+ Swing Highs progressively HIGHER
          let structureChainCount = 0;
          let structureRefLevel = swing.price;

          for (let j = i + 1; j < swings.length; j++) {
            const subsequentSwing = swings[j];
            if (subsequentSwing.index >= boundaryIndex) break; // Stop at the boundary
            
            if (subsequentSwing.type === 'high') {
              if (subsequentSwing.price > structureRefLevel) {
                structureChainCount++;
                structureRefLevel = subsequentSwing.price;
              }
            }
            
            if (structureChainCount >= 4) break;
          }
          if (structureChainCount >= 4) {
            isMajor = true;
            reason = `4+ Swing Highs Chain (${structureChainCount}) Above`;
          }
        }

      } else if (swing.type === 'high') {
        // ─── MAJOR SWING HIGH LOGIC ───

        // Condition A: Chain of 3 candles closing below previous lows
        let chainCount = 0;
        let referenceLevel = Infinity; // Will track the Low of the previous confirming candle

        for (let k = swingCandlePosition + 1; k < candles.length; k++) {
          const candle = candles[k];
          if (candle.index >= boundaryIndex) break; // Stop at the boundary

          if (chainCount === 0) {
            // First candle must just close below the swing price
            if (candle.close < swing.price) {
              chainCount = 1;
              referenceLevel = candle.low;
            }
          } else {
            // Subsequent candles must close below the PREVIOUS confirming candle's LOW
            if (candle.close < referenceLevel) {
              chainCount++;
              referenceLevel = candle.low;
            }
          }

          if (chainCount >= 3) break;
        }

        if (chainCount >= 3) {
          isMajor = true;
          reason = `3 Candle Chain Closing Below Lows`;
        } else {
          // Condition B: 4+ Swing Lows progressively LOWER
          let structureChainCount = 0;
          let structureRefLevel = swing.price;

          for (let j = i + 1; j < swings.length; j++) {
            const subsequentSwing = swings[j];
            if (subsequentSwing.index >= boundaryIndex) break; // Stop at the boundary
            
            if (subsequentSwing.type === 'low') {
              if (subsequentSwing.price < structureRefLevel) {
                structureChainCount++;
                structureRefLevel = subsequentSwing.price;
              }
            }

            if (structureChainCount >= 4) break;
          }
          if (structureChainCount >= 4) {
            isMajor = true;
            reason = `4+ Swing Lows Chain (${structureChainCount}) Below`;
          }
        }
      }

      // ─── STRUCTURAL FILTERING (HIERARCHY CHECK) ───
      if (isMajor) {
        if (swing.type === 'low') {
          // Check if the previous active major low was broken by a CLOSE
          if (activeMajorLow) {
            const startPos = candleIndexMap.get(activeMajorLow.index);
            const endPos = candleIndexMap.get(swing.index);
            if (startPos !== undefined && endPos !== undefined) {
              for (let k = startPos + 1; k <= endPos; k++) {
                if (candles[k].close < activeMajorLow.price) {
                  activeMajorLow = null; // Previous structure broken
                  break;
                }
              }
            }
          }

          if (activeMajorLow) {
            if (swing.price > activeMajorLow.price) {
              isMajor = false; // Invalid: Higher Low inside unbroken Major Low structure
            } else {
              activeMajorLow = swing; // Valid: Lower Low (updates the structural low)
            }
          } else {
            activeMajorLow = swing; // New structural low
          }

        } else if (swing.type === 'high') {
          // Check if the previous active major high was broken by a CLOSE
          if (activeMajorHigh) {
            const startPos = candleIndexMap.get(activeMajorHigh.index);
            const endPos = candleIndexMap.get(swing.index);
            if (startPos !== undefined && endPos !== undefined) {
              for (let k = startPos + 1; k <= endPos; k++) {
                if (candles[k].close > activeMajorHigh.price) {
                  activeMajorHigh = null; // Previous structure broken
                  break;
                }
              }
            }
          }

          if (activeMajorHigh) {
            if (swing.price < activeMajorHigh.price) {
              isMajor = false; // Invalid: Lower High inside unbroken Major High structure
            } else {
              activeMajorHigh = swing; // Valid: Higher High (updates the structural high)
            }
          } else {
            activeMajorHigh = swing; // New structural high
          }
        }
      }

      if (isMajor) {
        majorSwings.push({
          ...swing,
          isMajor: true,
          majorReason: reason,
          invalidationIndex: invalidationIndex !== -1 ? invalidationIndex : null,
        });
      }
    }

    return majorSwings;
  }
}

module.exports = new MajorSwingsEngine();
