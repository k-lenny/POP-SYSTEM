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
        candles
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

      // OBOppositeExtremeCandidates is derived by iterating every entry in
      // OBSetupExtremeCandidates (including the raw absolute extreme), finding
      // the OBOppositeExtreme anchored on each one, and collecting those whose
      // price matches any pattern/pattern2/pattern3. Deduplicated by index and
      // sorted in discovery order.
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
   * Finds the single absolute extreme candle after the OB cross:
   *   EQH — candle with the highest high after the cross (scan to end of candles)
   *   EQL — candle with the lowest low after the cross (scan to end of candles)
   * Merges it with the pattern-matched candidates from confirmedSetup.js,
   * deduplicates by candle index, and sorts in discovery order.
   */
  _buildOBSetupExtremeCandidates(type, OBCross, patternMatchedCandidates, candleIndexMap, candles) {
    if (!OBCross || !candleIndexMap || !candles || !candles.length) {
      return [...patternMatchedCandidates];
    }

    const crossPos = candleIndexMap.get(OBCross.index);
    if (crossPos === undefined) return [...patternMatchedCandidates];

    const isEQL = type === 'EQL';
    let extremeCandle = null;

    for (let i = crossPos + 1; i < candles.length; i++) {
      const c = candles[i];
      if (!extremeCandle) {
        extremeCandle = c;
        continue;
      }
      if (isEQL && c.low < extremeCandle.low) {
        extremeCandle = c;
      } else if (!isEQL && c.high > extremeCandle.high) {
        extremeCandle = c;
      }
    }

    if (!extremeCandle) return [...patternMatchedCandidates];

    const absoluteExtremeCandidate = {
      index: extremeCandle.index,
      price: isEQL ? extremeCandle.low : extremeCandle.high,
      formattedTime: extremeCandle.formattedTime,
    };

    const mergedMap = new Map();
    for (const c of patternMatchedCandidates) {
      mergedMap.set(c.index, c);
    }
    if (!mergedMap.has(absoluteExtremeCandidate.index)) {
      mergedMap.set(absoluteExtremeCandidate.index, absoluteExtremeCandidate);
    }

    return Array.from(mergedMap.values()).sort((a, b) => a.index - b.index);
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
      // Derive the OBOppositeExtreme anchored on this specific setup candidate
      const oppositeExtreme = this._findOBOppositeExtreme(
        type,
        setupCandidate,
        candleIndexMap,
        candles
      );
      if (!oppositeExtreme) continue;

      // Only collect if price matches any pattern — deduplicate by index so the
      // same opposite candle reached from multiple setup candidates is not repeated
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