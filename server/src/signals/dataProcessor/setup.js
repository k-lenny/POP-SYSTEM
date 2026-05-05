// server/src/signals/dataProcessor/setup.js
const eqhEqlEngine = require('./eqhEql');
const signalEngine = require('../signalEngine');
const { getConfig } = require('../../config');
const { buildCandleIndexMap, nextArrayIdx } = require('../../utils/dataProcessorUtils');

class SetupEngine {
  /**
   * Finds setups based on broken EQH/EQL levels.
   * A setup is a broken level followed by a "V-shape" pullback/retracement.
   * This requires a "sustained" breakout before scanning for the V-shape.
   *
   * The correct structural order is:
   *
   *   anchorCandle
   *       → impulseExtremeDepth   (extreme between anchor and setupVshape)
   *           → setupVshapeDepth  (extreme after impulseExtreme,
   *                                BEFORE impulseExtremeDepth gets broken)
   *               → breakout      (candle closes/opens through impulseExtremeDepth)
   *
   * Key locking rules:
   * 1. impulseExtremeDepth is the running impulse extreme up to the breakout point.
   * 2. setupVshapeDepth is the opposite extreme found ONLY in the candles between
   *    impulseExtremeDepth and the breakout. It is frozen the moment any candle
   *    closes/opens through impulseExtremeDepth — no scanning past that point.
   * 3. Both values are deterministic across runs. New candles arriving after the
   *    breakout can never alter setupVshapeDepth or impulseExtremeDepth.
   *
   * For EQH (broken upward originally, sweep lower forms below):
   *   - impulseExtremeDepth = highest HIGH from anchor onward
   *   - setupVshapeDepth    = lowest LOW between impulse extreme and breakout
   *   - breakout            = candle close OR open ABOVE impulseExtremeDepth
   *
   * For EQL (broken downward originally, sweep higher forms above):
   *   - impulseExtremeDepth = lowest LOW from anchor onward
   *   - setupVshapeDepth    = highest HIGH between impulse extreme and breakout
   *   - breakout            = candle close OR open BELOW impulseExtremeDepth
   *
   * @param {string} symbol The symbol to check (e.g., 'BTCUSD').
   * @param {number} granularity The granularity in seconds (e.g., 3600).
   * @returns {Array<Object>} An array of setup objects.
   */
  getSetups(symbol, granularity) {
    const brokenLevels = eqhEqlEngine.getBroken(symbol, granularity)
      .filter(l => l.validityStatus !== 'INVALID');
    if (!brokenLevels.length) {
      return [];
    }

    const config = getConfig(symbol, granularity);
    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (!candles.length) {
      return [];
    }

    const candleIndexMap = buildCandleIndexMap(candles);
    const setups = [];

    const bosScanLimit = config.MAX_BOS_SCAN_CANDLES || 10;

    for (const level of brokenLevels) {
      // 1. Find the initial breaking candle.
      const breakArrayPos = candleIndexMap.get(level.brokenIndex);
      if (breakArrayPos === undefined) {
        continue;
      }
      const breakingCandle = candles[breakArrayPos];

      // 2. Determine the anchor candle for V-shape scanning.
      // BOS_CLOSE: the breaking candle itself already closed past the level, so use it directly.
      // BOS_SUSTAINED: need a confirming candle that closes past the breaking candle's high/low.
      let anchorCandlePos = -1;

      if (level.brokenBosType === 'BOS_CLOSE') {
        anchorCandlePos = breakArrayPos;
      } else {
        // BOS_SUSTAINED: find a confirming candle
        let confirmingCandle = null;
        const confirmationScanStart = breakArrayPos + 1;
        const confirmationScanEnd = Math.min(candles.length, confirmationScanStart + bosScanLimit);

        for (let i = confirmationScanStart; i < confirmationScanEnd; i++) {
          const c = candles[i];
          if (level.type === 'EQH' && c.close > breakingCandle.high) {
            confirmingCandle = c;
            anchorCandlePos = i;
            break;
          } else if (level.type === 'EQL' && c.close < breakingCandle.low) {
            confirmingCandle = c;
            anchorCandlePos = i;
            break;
          }
        }

        if (!confirmingCandle) {
          continue;
        }
      }

      // 3. Scan for the setup structure.
      //
      // Strategy:
      //   a. From anchorCandlePos forward within the 70-candle window, walk
      //      candle by candle maintaining a running impulse extreme.
      //   b. After each impulse update, scan from that impulse position forward
      //      for the opposite extreme — stopping as soon as a candle closes/opens
      //      through the impulse extreme (the breakout). The opposite extreme found
      //      in that pre-breakout window is the setupVshape candidate.
      //   c. The first impulse position for which a valid breakout exists locks in
      //      both impulseExtremeDepth and setupVshapeDepth. Stop immediately.
      //
      // This guarantees:
      //   - setupVshapeDepth is never found AFTER impulseExtremeDepth is broken
      //   - both values are frozen the moment the structure completes
      //   - no future candles can change either value

      let extremeCandle = null;        // setupVshapeDepth candle
      let impulseExtremeCandle = null; // impulseExtremeDepth candle

      const vShapeScanStart = anchorCandlePos + 1;
      const vShapeScanEnd = Math.min(candles.length, breakArrayPos + 1 + 70);

      if (vShapeScanStart >= vShapeScanEnd) {
        continue;
      }

      if (level.type === 'EQH') {
        // EQH: impulse high → sweep low → breakout above impulse high

        let currentImpulseHigh = -Infinity;
        let currentImpulseCandle = null;

        for (let i = vShapeScanStart; i < vShapeScanEnd; i++) {
          const candle = candles[i];

          // Update running impulse high
          if (candle.high > currentImpulseHigh) {
            currentImpulseHigh = candle.high;
            currentImpulseCandle = candle;
          }

          if (!currentImpulseCandle) continue;

          const impulseArrayPos = candleIndexMap.get(currentImpulseCandle.index);
          if (impulseArrayPos === undefined) continue;

          // From the impulse position forward, find the lowest low —
          // stopping the moment a candle closes OR opens above the impulse high.
          // The lowest low found before that breakout is the setupVshape candidate.
          let candidateSetupLow = Infinity;
          let candidateSetupCandle = null;
          let breakoutFound = false;

          for (let j = impulseArrayPos + 1; j < candles.length; j++) {
            const c = candles[j];

            // Breakout check FIRST — close OR open above impulse high
            if (c.close > currentImpulseHigh || c.open > currentImpulseHigh) {
              breakoutFound = true;
              break; // stop scanning — setupVshape cannot be found past here
            }

            // Track the lowest low in the pre-breakout window
            if (c.low < candidateSetupLow) {
              candidateSetupLow = c.low;
              candidateSetupCandle = c;
            }
          }

          if (breakoutFound && candidateSetupCandle) {
            // Valid structure — freeze both values and stop
            impulseExtremeCandle = currentImpulseCandle;
            extremeCandle = candidateSetupCandle;
            break;
          }
        }

      } else { // EQL: impulse low → sweep high → breakout below impulse low

        let currentImpulseLow = Infinity;
        let currentImpulseCandle = null;

        for (let i = vShapeScanStart; i < vShapeScanEnd; i++) {
          const candle = candles[i];

          // Update running impulse low
          if (candle.low < currentImpulseLow) {
            currentImpulseLow = candle.low;
            currentImpulseCandle = candle;
          }

          if (!currentImpulseCandle) continue;

          const impulseArrayPos = candleIndexMap.get(currentImpulseCandle.index);
          if (impulseArrayPos === undefined) continue;

          // From the impulse position forward, find the highest high —
          // stopping the moment a candle closes OR opens below the impulse low.
          // The highest high found before that breakout is the setupVshape candidate.
          let candidateSetupHigh = -Infinity;
          let candidateSetupCandle = null;
          let breakoutFound = false;

          for (let j = impulseArrayPos + 1; j < candles.length; j++) {
            const c = candles[j];

            // Breakout check FIRST — close OR open below impulse low
            if (c.close < currentImpulseLow || c.open < currentImpulseLow) {
              breakoutFound = true;
              break; // stop scanning — setupVshape cannot be found past here
            }

            // Track the highest high in the pre-breakout window
            if (c.high > candidateSetupHigh) {
              candidateSetupHigh = c.high;
              candidateSetupCandle = c;
            }
          }

          if (breakoutFound && candidateSetupCandle) {
            // Valid structure — freeze both values and stop
            impulseExtremeCandle = currentImpulseCandle;
            extremeCandle = candidateSetupCandle;
            break;
          }
        }
      }

      // Create a new setup object with all original level info + the frozen extreme info.
      // If no breakout was found yet (extremeCandle is null), setupVshapeDepth and
      // impulseExtremeDepth will be null — confirmedSetup.js will skip it via the
      // null/infinity guard until the structure fully forms.
      const setup = {
        ...level,
        setupVshapeDepth: extremeCandle
          ? (level.type === 'EQH' ? extremeCandle.low : extremeCandle.high)
          : null,
        setupVshapeTime: extremeCandle ? extremeCandle.time : null,
        setupVshapeFormattedTime: extremeCandle ? extremeCandle.formattedTime : null,
        setupVshapeIndex: extremeCandle ? extremeCandle.index : null,
        impulseExtremeDepth: impulseExtremeCandle
          ? (level.type === 'EQH' ? impulseExtremeCandle.high : impulseExtremeCandle.low)
          : null,
        impulseExtremeTime: impulseExtremeCandle ? impulseExtremeCandle.time : null,
        impulseExtremeFormattedTime: impulseExtremeCandle ? impulseExtremeCandle.formattedTime : null,
        impulseExtremeIndex: impulseExtremeCandle ? impulseExtremeCandle.index : null,
      };
      setups.push(setup);
    }

    return setups;
  }
}

module.exports = new SetupEngine();