// server/src/final/final.js
const confirmedSetupEngine = require('../signals/dataProcessor/confirmedSetup');
const patternEngine = require('../strategies/patterns/pattern');
const pattern2Engine = require('../strategies/patterns/pattern2');
const pattern3Engine = require('../strategies/patterns/pattern3');
const swingEngine = require('../signals/dataProcessor/swings');
const signalEngine = require('../signals/signalEngine');
const { findConsolidations } = require('../signals/dataProcessor/Consolidation');
const { buildCandleIndexMap } = require('../utils/dataProcessorUtils');

class FinalEngine {
  async getConfirmedSetups(symbol, granularity) {
    const candles = signalEngine.getCandles(symbol, granularity, true);
    if (candles && candles.length) {
      if (swingEngine.get(symbol, granularity).length === 0) {
        await swingEngine.detectAll(symbol, granularity, candles);
      }
      if (patternEngine.get(symbol, granularity).length === 0) {
        await patternEngine.detect(symbol, granularity, candles);
      }
      if (pattern2Engine.get(symbol, granularity).length === 0) {
        await pattern2Engine.detect(symbol, granularity, candles);
      }
      if (pattern3Engine.get(symbol, granularity).length === 0) {
        await pattern3Engine.detect(symbol, granularity, candles);
      }
    }

    const setups = confirmedSetupEngine.getConfirmedSetups(symbol, granularity);

    const pattern1Patterns = patternEngine.get(symbol, granularity);
    const pattern2Patterns = pattern2Engine.get(symbol, granularity);
    const pattern3Patterns = pattern3Engine.get(symbol, granularity);

    const candleIndexMap = candles && candles.length ? buildCandleIndexMap(candles) : null;
    const consolidations = candles && candles.length ? findConsolidations(candles) : [];

    const findConfirmationBreakout = (retestIndex) => {
      if (retestIndex == null) return null;
      for (const z of consolidations) {
        const s = z.start?.index;
        const e = z.end?.index;
        if (s == null || e == null) continue;
        if (retestIndex >= s && retestIndex <= e) {
          return z.retest?.confirmationConsolidation?.breakout ?? null;
        }
      }
      return null;
    };

    return setups.map(setup => {
      const obCrossFT = setup.OBCross?.formattedTime;

      const OBSetupExtremeCandidates = this._buildOBSetupExtremeCandidates(
        setup.type,
        setup.OBCross,
        setup.OBSetupExtremeCandidates || [],
        candleIndexMap,
        candles,
        setup.setupOB,
        symbol,
        granularity
      );

      let resolvedOBSetupExtreme = setup.OBSetupExtreme;
      let patternMatch = null;

      for (const candidate of OBSetupExtremeCandidates) {
        const candidateMatch = this._findPatternByCurrentSwing(pattern1Patterns, candidate.price);

        // Gate 1: previousSwing must not predate the OB cross
        if (
          candidateMatch &&
          obCrossFT &&
          candidateMatch.previousSwing?.formattedTime &&
          candidateMatch.previousSwing.formattedTime < obCrossFT
        ) {
          continue;
        }

        // Gate 2: retest violated currentSwing — evaluate OBOppositeExtreme
        // before deciding whether to skip
        if (candidateMatch?.retest?.violatedCurrentSwing === true) {
          const candidateOppositeExtreme = this._findOBOppositeExtreme(
            setup.type,
            candidate,
            candleIndexMap,
            candles
          );
          const oppositePrice = candidateOppositeExtreme?.price;

          const oppositePattern1Match =
            this._findPatternByCurrentSwing(pattern1Patterns, oppositePrice);
          const oppositePattern2Match =
            this._findPattern2ByFirstSwing(pattern2Patterns, oppositePrice);
          const oppositePattern3Match =
            this._findPattern3ByFirstSwing(pattern3Patterns, oppositePrice);

          const hasOppositeMatch =
            oppositePattern1Match != null ||
            oppositePattern2Match != null ||
            oppositePattern3Match != null;

          if (!hasOppositeMatch) {
            continue;
          }

          // Rescue cancelled if pattern2 opposite retest is expired
          if (
            oppositePattern2Match != null &&
            oppositePattern2Match.retestStatus === 'expired'
          ) {
            continue;
          }

          resolvedOBSetupExtreme = candidate;
          patternMatch = candidateMatch;
          break;
        }

        // Passed all gates cleanly — commit and stop
        resolvedOBSetupExtreme = candidate;
        patternMatch = candidateMatch;
        break;
      }

      const resolvedPrice = resolvedOBSetupExtreme?.price;
      const pattern2Match = this._findPattern2ByFirstSwing(pattern2Patterns, resolvedPrice);
      const pattern3Match = this._findPattern3ByFirstSwing(pattern3Patterns, resolvedPrice);

      const OBOppositeExtreme = this._findOBOppositeExtreme(
        setup.type,
        resolvedOBSetupExtreme,
        candleIndexMap,
        candles
      );

      const OBOppositeExtremeCandidates = this._findOBOppositeExtremeCandidates(
        setup.type,
        OBSetupExtremeCandidates,
        candleIndexMap,
        candles,
        pattern1Patterns,
        pattern2Patterns,
        pattern3Patterns
      );

      const oppositePrice = OBOppositeExtreme?.price;
      const OBOppositeExtremePatternMatch =
        this._findPatternByCurrentSwing(pattern1Patterns, oppositePrice);
      const OBOppositeExtremePattern2Match =
        this._findPattern2ByFirstSwing(pattern2Patterns, oppositePrice);
      const OBOppositeExtremePattern3Match =
        this._findPattern3ByFirstSwing(pattern3Patterns, oppositePrice);

      if (patternMatch) {
        patternMatch.confirmationConsolidationBreakout =
          findConfirmationBreakout(patternMatch.retest?.index);
      }
      if (pattern2Match) {
        pattern2Match.confirmationConsolidationBreakout =
          findConfirmationBreakout(pattern2Match.retestData?.index);
      }
      if (pattern3Match) {
        pattern3Match.confirmationConsolidationBreakout =
          findConfirmationBreakout(pattern3Match.retest?.index);
      }
      if (OBOppositeExtremePatternMatch) {
        OBOppositeExtremePatternMatch.confirmationConsolidationBreakout =
          findConfirmationBreakout(OBOppositeExtremePatternMatch.retest?.index);
      }
      if (OBOppositeExtremePattern2Match) {
        OBOppositeExtremePattern2Match.confirmationConsolidationBreakout =
          findConfirmationBreakout(OBOppositeExtremePattern2Match.retestData?.index);
      }
      if (OBOppositeExtremePattern3Match) {
        OBOppositeExtremePattern3Match.confirmationConsolidationBreakout =
          findConfirmationBreakout(OBOppositeExtremePattern3Match.retest?.index);
      }

      return {
        ...setup,
        OBSetupExtreme: resolvedOBSetupExtreme,
        OBSetupExtremeCandidates,
        OBOppositeExtreme,
        OBOppositeExtremeCandidates,
        patternMatch,
        pattern2Match,
        pattern3Match,
        OBOppositeExtremePatternMatch,
        OBOppositeExtremePattern2Match,
        OBOppositeExtremePattern3Match,
      };
    });
  }

  /**
   * Builds the full OBSetupExtremeCandidates list by merging three sources:
   *   1. Pattern-matched candidates already identified in confirmedSetup.js
   *   2. The single absolute extreme candle after the OB cross
   *      (lowest low for EQL, highest high for EQH across ALL candles after cross)
   *   3. The qualifying OB-range swing candle from swingEngine (see _findOBRangeSwingCandidate)
   *
   * All three are deduplicated by candle index and sorted in discovery order.
   */
  _buildOBSetupExtremeCandidates(type, OBCross, patternMatchedCandidates, candleIndexMap, candles, setupOB, symbol, granularity) {
    if (!OBCross || !candleIndexMap || !candles || !candles.length) {
      return [...patternMatchedCandidates];
    }

    const crossPos = candleIndexMap.get(OBCross.index);
    if (crossPos === undefined) return [...patternMatchedCandidates];

    const isEQL = type === 'EQL';

    // ── Source 1: pattern-matched candidates from confirmedSetup ─────────────
    const mergedMap = new Map();
    for (const c of patternMatchedCandidates) {
      mergedMap.set(c.index, c);
    }

    // ── Source 2: absolute extreme candle after the cross ────────────────────
    let extremeCandle = null;
    for (let i = crossPos + 1; i < candles.length; i++) {
      const c = candles[i];
      if (!extremeCandle) {
        extremeCandle = c;
        continue;
      }
      if (isEQL && c.low < extremeCandle.low) extremeCandle = c;
      else if (!isEQL && c.high > extremeCandle.high) extremeCandle = c;
    }
    if (extremeCandle) {
      const absoluteExtremeCandidate = {
        index: extremeCandle.index,
        price: isEQL ? extremeCandle.low : extremeCandle.high,
        formattedTime: extremeCandle.formattedTime,
      };
      if (!mergedMap.has(absoluteExtremeCandidate.index)) {
        mergedMap.set(absoluteExtremeCandidate.index, absoluteExtremeCandidate);
      }
    }

    // ── Source 3: qualifying OB-range swing from swingEngine ─────────────────
    const obRangeSwingCandidate = this._findOBRangeSwingCandidate(
      isEQL,
      crossPos,
      candles,
      candleIndexMap,
      setupOB,
      symbol,
      granularity
    );
    if (obRangeSwingCandidate && !mergedMap.has(obRangeSwingCandidate.index)) {
      mergedMap.set(obRangeSwingCandidate.index, obRangeSwingCandidate);
    }

    return Array.from(mergedMap.values()).sort((a, b) => a.index - b.index);
  }

  /**
   * Finds the first swing (from swingEngine) after the OB cross whose wick price
   * sits inside the OB range AND that has completed the acceptance→violation sequence:
   *
   *   EQL (swing low):
   *     - swing.price (low) must be >= setupOB.low and <= setupOB.high
   *     - After that swing candle, a subsequent candle must body-close/open ABOVE
   *       swing.high (acceptance above the wick high)
   *     - Then, after acceptance, a subsequent candle must body-close/open BELOW
   *       swing.price / swing.low (violation back below the swing low)
   *
   *   EQH (swing high):
   *     - swing.price (high) must be >= setupOB.low and <= setupOB.high
   *     - After that swing candle, a subsequent candle must body-close/open BELOW
   *       swing.low (acceptance below the wick low)
   *     - Then, after acceptance, a subsequent candle must body-close/open ABOVE
   *       swing.price / swing.high (violation back above the swing high)
   *
   * Returns the first qualifying swing as { index, price, formattedTime }, or null.
   */
  _findOBRangeSwingCandidate(isEQL, crossPos, candles, candleIndexMap, setupOB, symbol, granularity) {
    if (!setupOB || setupOB.high == null || setupOB.low == null) return null;

    const obHigh = setupOB.high;
    const obLow  = setupOB.low;

    // Pull actual swings from swingEngine — same engine already populated in getConfirmedSetups
    const allSwings = isEQL
      ? swingEngine.getLows(symbol, granularity)
      : swingEngine.getHighs(symbol, granularity);

    // Only consider swings whose candle array position is after the OB cross
    // and whose wick price falls inside the OB range
    const eligibleSwings = allSwings.filter(swing => {
      const swingPos = candleIndexMap.get(swing.index);
      if (swingPos === undefined || swingPos <= crossPos) return false;
      // For a swing low: swing.price is the low (wick). Must be inside OB range.
      // For a swing high: swing.price is the high (wick). Must be inside OB range.
      return swing.price >= obLow && swing.price <= obHigh;
    });

    // Sort by position ascending so we return the earliest qualifying swing
    eligibleSwings.sort((a, b) => {
      const posA = candleIndexMap.get(a.index) ?? Infinity;
      const posB = candleIndexMap.get(b.index) ?? Infinity;
      return posA - posB;
    });

    for (const swing of eligibleSwings) {
      const swingPos = candleIndexMap.get(swing.index);

      // swing.high / swing.low are the full wick extents of the swing candle
      const wickHigh = swing.high;
      const wickLow  = swing.low;
      const wickPrice = swing.price; // low for EQL swing, high for EQH swing

      let acceptanceSeen = false;

      for (let j = swingPos + 1; j < candles.length; j++) {
        const c = candles[j];
        const bodyHigh = Math.max(c.open, c.close);
        const bodyLow  = Math.min(c.open, c.close);

        if (isEQL) {
          if (!acceptanceSeen) {
            // Phase 1: body must close/open above the swing candle's wick high
            if (bodyHigh > wickHigh) {
              acceptanceSeen = true;
            }
          } else {
            // Phase 2: body must close/open below the swing low (violation)
            if (bodyLow < wickPrice) {
              return {
                index: swing.index,
                price: wickPrice,
                formattedTime: swing.formattedTime,
              };
            }
          }
        } else {
          // EQH
          if (!acceptanceSeen) {
            // Phase 1: body must close/open below the swing candle's wick low
            if (bodyLow < wickLow) {
              acceptanceSeen = true;
            }
          } else {
            // Phase 2: body must close/open above the swing high (violation)
            if (bodyHigh > wickPrice) {
              return {
                index: swing.index,
                price: wickPrice,
                formattedTime: swing.formattedTime,
              };
            }
          }
        }
      }
    }

    return null;
  }

  /**
   * For each candidate in OBSetupExtremeCandidates (including the raw absolute
   * extreme), derives the OBOppositeExtreme anchored on that candidate, then
   * checks if its price matches any pattern/pattern2/pattern3. Matching opposite
   * extremes are collected, deduplicated by index, and sorted in discovery order.
   */
  _findOBOppositeExtremeCandidates(type, OBSetupExtremeCandidates, candleIndexMap, candles, p1Patterns, p2Patterns, p3Patterns) {
    if (!OBSetupExtremeCandidates.length || !candleIndexMap || !candles || !candles.length) return [];

    const p1Prices = new Set(
      p1Patterns.map(p => p.currentSwingPrice).filter(v => v != null)
    );
    const p2Prices = new Set(
      p2Patterns.map(p => p.firstSwingPrice).filter(v => v != null)
    );
    const p3Prices = new Set(
      p3Patterns.map(p => p.firstSwingPrice).filter(v => v != null)
    );

    const mergedMap = new Map();

    for (const setupCandidate of OBSetupExtremeCandidates) {
      const oppositeExtreme = this._findOBOppositeExtreme(
        type,
        setupCandidate,
        candleIndexMap,
        candles
      );
      if (!oppositeExtreme) continue;

      if (
        p1Prices.has(oppositeExtreme.price) ||
        p2Prices.has(oppositeExtreme.price) ||
        p3Prices.has(oppositeExtreme.price)
      ) {
        if (!mergedMap.has(oppositeExtreme.index)) {
          mergedMap.set(oppositeExtreme.index, {
            index: oppositeExtreme.index,
            price: oppositeExtreme.price,
            formattedTime: oppositeExtreme.formattedTime,
          });
        }
      }
    }

    return Array.from(mergedMap.values()).sort((a, b) => a.index - b.index);
  }

  _findPattern3ByFirstSwing(patterns, firstSwingPrice) {
    if (firstSwingPrice == null) return null;

    for (const p of patterns) {
      if (p.firstSwingPrice === firstSwingPrice) {
        return {
          direction: p.direction ?? null,
          firstSwing: {
            type: p.firstSwingType ?? null,
            price: p.firstSwingPrice ?? null,
            index: p.firstSwingIndex ?? null,
            formattedTime: p.firstSwingFormattedTime ?? null,
          },
          secondSwing: {
            type: p.secondSwingType ?? null,
            price: p.secondSwingPrice ?? null,
            index: p.secondSwingIndex ?? null,
            formattedTime: p.secondSwingFormattedTime ?? null,
          },
          vShapeCandle: {
            index: p.vShapeCandleIndex ?? null,
            price: p.vShapeCandlePrice ?? null,
            open: p.vShapeCandleOpen ?? null,
            high: p.vShapeCandleHigh ?? null,
            low: p.vShapeCandleLow ?? null,
            close: p.vShapeCandleClose ?? null,
            formattedTime: p.vShapeCandleFormattedTime ?? null,
          },
          breakoutCandle: {
            index: p.breakoutCandleIndex ?? null,
            price: p.breakoutCandlePrice ?? null,
            open: p.breakoutCandleOpen ?? null,
            high: p.breakoutCandleHigh ?? null,
            low: p.breakoutCandleLow ?? null,
            close: p.breakoutCandleClose ?? null,
            formattedTime: p.breakoutCandleFormattedTime ?? null,
          },
          retest: p.retest ?? null,
          NumberOfNoLv: p.NumberOfNoLv ?? null,
          OBFound: p.OBFound ?? null,
        };
      }
    }
    return null;
  }

  _findOBOppositeExtreme(type, OBSetupExtreme, candleIndexMap, candles) {
    if (!OBSetupExtreme || !candleIndexMap || !candles || !candles.length) return null;

    const startPos = candleIndexMap.get(OBSetupExtreme.index);
    if (startPos === undefined) return null;

    const isEQL = type === 'EQL';
    const setupPrice = OBSetupExtreme.price;
    let extremeCandle = null;

    for (let i = startPos + 1; i < candles.length; i++) {
      const c = candles[i];
      if (isEQL && setupPrice != null && c.low < setupPrice) break;
      if (!isEQL && setupPrice != null && c.high > setupPrice) break;

      if (!extremeCandle) {
        extremeCandle = c;
        continue;
      }
      if (isEQL && c.high > extremeCandle.high) {
        extremeCandle = c;
      } else if (!isEQL && c.low < extremeCandle.low) {
        extremeCandle = c;
      }
    }

    if (!extremeCandle) return null;

    return {
      index: extremeCandle.index,
      price: isEQL ? extremeCandle.high : extremeCandle.low,
      formattedTime: extremeCandle.formattedTime,
    };
  }

  _findPatternByCurrentSwing(patterns, currentSwingPrice) {
    if (currentSwingPrice == null) return null;

    for (const p of patterns) {
      if (p.currentSwingPrice === currentSwingPrice) {
        return {
          breakoutData: {
            price: p.breakoutPrice ?? null,
            index: p.breakoutIndex ?? null,
            formattedTime: p.breakoutFormattedTime ?? null,
          },
          currentSwing: {
            price: p.currentSwingPrice ?? null,
            index: p.currentSwingIndex ?? null,
            formattedTime: p.currentSwingFormattedTime ?? null,
          },
          previousSwing: {
            price: p.previousSwingPrice ?? null,
            index: p.previousSwingIndex ?? null,
            formattedTime: p.previousSwingFormattedTime ?? null,
          },
          retest: {
            price: p.retestPrice ?? null,
            index: p.retestIndex ?? null,
            time: p.retestTime ?? null,
            formattedTime: p.retestFormattedTime ?? null,
            violatedCurrentSwing: p.retestViolatedCurrentSwing ?? false,
          },
        };
      }
    }
    return null;
  }

  _findPattern2ByFirstSwing(patterns, firstSwingPrice) {
    if (firstSwingPrice == null) return null;

    for (const p of patterns) {
      if (p.firstSwingPrice === firstSwingPrice) {
        return {
          breakoutData: p.breakoutData ?? null,
          firstSwing: {
            price: p.firstSwingPrice ?? null,
            index: p.firstSwingIndex ?? null,
            formattedTime: p.firstSwingFormattedTime ?? null,
          },
          secondSwing: {
            price: p.secondSwingPrice ?? null,
            index: p.secondSwingIndex ?? null,
            formattedTime: p.secondSwingFormattedTime ?? null,
          },
          retestData: p.retestData ?? null,
          retestStatus: p.retestStatus ?? null,
        };
      }
    }
    return null;
  }
}

module.exports = new FinalEngine();