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
      const candidates = setup.OBSetupExtremeCandidates || [];

      // Walk candidates in order (each is a progressively deeper extreme).
      //
      // Gate 1 — skip if patternMatch's previousSwing predates the OB cross.
      //
      // Gate 2 — if patternMatch retest violated currentSwing:
      //   a. Derive OBOppositeExtreme for this candidate and check all three
      //      pattern finders against it.
      //   b. If NO opposite match exists → skip to next candidate.
      //   c. If an opposite match EXISTS → candidate would normally hold, BUT
      //      first check if the opposite match came from pattern2 AND that
      //      pattern2's retestStatus is 'expired' (firstSwing crossed before
      //      the vshape lock was established). If that specific condition is
      //      true → the OBOppositeExtreme rescue is cancelled, skip to next
      //      candidate.
      //   d. Otherwise → candidate holds, patternMatch kept despite violation.
      let resolvedOBSetupExtreme = setup.OBSetupExtreme;
      let patternMatch = null;

      for (const candidate of candidates) {
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
          // Derive OBOppositeExtreme anchored on this specific candidate
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
            // No opposite match at all — skip to next deeper candidate
            continue;
          }

          // Opposite match exists — but check if the pattern2 opposite match
          // has retestStatus === 'expired', which cancels the rescue
          if (
            oppositePattern2Match != null &&
            oppositePattern2Match.retestStatus === 'expired'
          ) {
            // pattern2 opposite retest expired (firstSwing crossed before lock)
            // — rescue cancelled, skip to next deeper candidate
            continue;
          }

          // Opposite match is valid — candidate holds despite patternMatch violation
          resolvedOBSetupExtreme = candidate;
          patternMatch = candidateMatch;
          break;
        }

        // Passed all gates cleanly — commit and stop
        resolvedOBSetupExtreme = candidate;
        patternMatch = candidateMatch;
        break;
      }

      // pattern2 and pattern3 always resolve against the final OBSetupExtreme price
      const resolvedPrice = resolvedOBSetupExtreme?.price;
      const pattern2Match = this._findPattern2ByFirstSwing(pattern2Patterns, resolvedPrice);
      const pattern3Match = this._findPattern3ByFirstSwing(pattern3Patterns, resolvedPrice);

      const OBOppositeExtreme = this._findOBOppositeExtreme(
        setup.type,
        resolvedOBSetupExtreme,
        candleIndexMap,
        candles
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
        OBOppositeExtreme,
        patternMatch,
        pattern2Match,
        pattern3Match,
        OBOppositeExtremePatternMatch,
        OBOppositeExtremePattern2Match,
        OBOppositeExtremePattern3Match,
      };
    });
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