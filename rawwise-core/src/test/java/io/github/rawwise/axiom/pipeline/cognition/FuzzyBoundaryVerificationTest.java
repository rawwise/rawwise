package io.github.rawwise.axiom.pipeline.cognition;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.axiom.pipeline.feedback.FeedbackLoopCloser;
import io.github.rawwise.axiom.pipeline.feedback.retrieval.RerankerAbSwitcher;
import io.github.rawwise.model.record.StrategyAction;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Boundary verification test for all 15 fuzzy-ized truncation points. Each test verifies the
 * sigmoid output at and around the decision boundary, contrasting with the old hard-threshold
 * behavior.
 */
@DisplayName("Fuzzy Boundary Verification — 15 Truncation Points")
class FuzzyBoundaryVerificationTest {

    @Nested
    @DisplayName("Phase 0: SNR Threshold (DefaultEffectEvaluator)")
    class SnrThreshold {

        @Test
        @DisplayName("At SNR=2.0 (threshold), μ=0.5 — old: true (>=2.0), new: unreliable (μ<0.5)")
        void atThreshold() {
            double mu = MembershipFunctions.sigmoid(2.0, 2.0, 2.0);
            assertEquals(0.5, mu, 0.001);
            assertFalse(mu >= 0.5 + 0.001, "At threshold, μ should be exactly 0.5");
        }

        @Test
        @DisplayName("Just above: SNR=2.01 -> μ≈0.505 — old: true, new: barely reliable")
        void justAboveThreshold() {
            double mu = MembershipFunctions.sigmoid(2.01, 2.0, 2.0);
            assertTrue(mu > 0.5, "Expected μ > 0.5, got " + mu);
            assertTrue(mu < 0.51, "Expected μ < 0.51 (barely above), got " + mu);
        }

        @Test
        @DisplayName("Just below: SNR=1.99 -> μ≈0.495 — old: false (cliff!), new: almost reliable")
        void justBelowThreshold() {
            double mu = MembershipFunctions.sigmoid(1.99, 2.0, 2.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5, got " + mu);
            assertTrue(mu > 0.49, "Expected μ > 0.49 (almost reliable), got " + mu);
        }

        @Test
        @DisplayName("Far above: SNR=4.0 -> μ≈0.98 — high confidence")
        void farAbove() {
            double mu = MembershipFunctions.sigmoid(4.0, 2.0, 2.0);
            assertTrue(mu > 0.97, "Expected μ > 0.97, got " + mu);
        }

        @Test
        @DisplayName("Far below: SNR=0.5 -> μ≈0.04 — low confidence, not zero")
        void farBelow() {
            double mu = MembershipFunctions.sigmoid(0.5, 2.0, 2.0);
            assertTrue(mu < 0.05, "Expected μ < 0.05, got " + mu);
            assertTrue(mu > 0.0, "Expected μ > 0.0 (not zero), got " + mu);
        }
    }

    @Nested
    @DisplayName("Phase 1: EffectRecord + computeConfidence")
    class EffectRecordFuzzy {

        @Test
        @DisplayName("EffectRecord: μ=0.5 -> effective()=true (boundary)")
        void effectiveAtBoundary() {
            var record =
                    new StrategyAction.EffectRecord(
                            "a1", "v1", Map.of(), Map.of(), 0.3, 0.5, "ENV");
            assertTrue(record.effective());
        }

        @Test
        @DisplayName("EffectRecord: μ=0.49 -> effective()=false (just below boundary)")
        void effectiveJustBelow() {
            var record =
                    new StrategyAction.EffectRecord(
                            "a1", "v1", Map.of(), Map.of(), 0.3, 0.49, "ENV");
            assertFalse(record.effective());
        }

        @Test
        @DisplayName(
                "computeConfidence: mixed μ [0.98, 0.49] — old: 0.5 (1 true, 1 false), new: 0.735")
        void mixedConfidence() {
            var e1 =
                    new StrategyAction.EffectRecord(
                            "a1", "v1", Map.of(), Map.of(), 0.5, 0.98, "HUMAN");
            var e2 =
                    new StrategyAction.EffectRecord(
                            "a2", "v1", Map.of(), Map.of(), 0.3, 0.49, "HUMAN");
            double confidence = StrategyAction.computeConfidence(List.of(e1, e2));
            // old: e1.effective()=true (+1), e2.effective()=false (+0) -> 0.5
            // new: (0.98 + 0.49) / 2 = 0.735
            assertEquals(
                    0.735,
                    confidence,
                    0.01,
                    "Continuous weighting should preserve boundary info that hard threshold discards");
        }

        @Test
        @DisplayName("computeConfidence: all μ=0.49 — old: 0.0 (all false), new: 0.49")
        void allJustBelowBoundary() {
            var e1 =
                    new StrategyAction.EffectRecord(
                            "a1", "v1", Map.of(), Map.of(), 0.5, 0.49, "HUMAN");
            var e2 =
                    new StrategyAction.EffectRecord(
                            "a2", "v1", Map.of(), Map.of(), 0.3, 0.49, "HUMAN");
            double confidence = StrategyAction.computeConfidence(List.of(e1, e2));
            // old: all effective()=false -> 0.0
            // new: (0.49 + 0.49) / 2 = 0.49
            assertEquals(
                    0.49,
                    confidence,
                    0.01,
                    "Old hard threshold would zero out all near-boundary feedback; sigmoid preserves it");
        }
    }

    @Nested
    @DisplayName("Phase 1: DefaultStrategyRetriever — confidence > 0.3")
    class StrategyConfidence {

        @Test
        @DisplayName("confidence=0.3 -> μ=0.5 (boundary, passes)")
        void atConfidenceBoundary() {
            double mu = MembershipFunctions.sigmoid(0.3, 0.3, 4.0);
            assertEquals(0.5, mu, 0.001);
            assertTrue(mu >= 0.5);
        }

        @Test
        @DisplayName("confidence=0.29 -> μ≈0.49 (just below, filtered out)")
        void justBelowConfidence() {
            double mu = MembershipFunctions.sigmoid(0.29, 0.3, 4.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5, got " + mu);
            assertTrue(mu > 0.47, "Expected μ > 0.47 (close to boundary), got " + mu);
        }

        @Test
        @DisplayName("confidence=0.25 -> μ≈0.45 (below but close, k=4.0 gives smooth transition)")
        void wellBelowConfidence() {
            double mu = MembershipFunctions.sigmoid(0.25, 0.3, 4.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5, got " + mu);
            assertTrue(mu > 0.4, "Expected μ > 0.4 (smooth transition), got " + mu);
        }

        @Test
        @DisplayName("confidence=0.35 -> μ≈0.55 (above but close, k=4.0 gives smooth transition)")
        void aboveConfidence() {
            double mu = MembershipFunctions.sigmoid(0.35, 0.3, 4.0);
            assertTrue(mu > 0.5, "Expected μ > 0.5, got " + mu);
            assertTrue(mu < 0.6, "Expected μ < 0.6 (smooth transition), got " + mu);
        }
    }

    @Nested
    @DisplayName("Phase 1: DefaultStrategyRetriever — effectiveRatio <= 0.5")
    class StrategyEffectiveRatio {

        @Test
        @DisplayName("avgEffectiveness=0.5 -> μ=0.5 (boundary, passes)")
        void atBoundary() {
            double mu = MembershipFunctions.sigmoid(0.5, 0.5, 4.0);
            assertEquals(0.5, mu, 0.001);
        }

        @Test
        @DisplayName("avgEffectiveness=0.49 -> μ≈0.49 (just below, filtered)")
        void justBelow() {
            double mu = MembershipFunctions.sigmoid(0.49, 0.5, 4.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5, got " + mu);
            assertTrue(mu > 0.47, "Expected μ > 0.47 (close), got " + mu);
        }

        @Test
        @DisplayName("Continuous avg: [0.8, 0.3, 0.4] -> avg=0.5 -> μ=0.5 (boundary)")
        void mixedEffects() {
            // old: 1 effective (0.8), 2 ineffective (0.3, 0.4) -> ratio=0.33 <= 0.5 -> filtered
            // new: avg=0.5 -> μ=0.5 -> passes
            double avg = (0.8 + 0.3 + 0.4) / 3.0;
            double mu = MembershipFunctions.sigmoid(avg, 0.5, 4.0);
            assertEquals(
                    0.5,
                    mu,
                    0.01,
                    "Old: ratio 0.33 ≤ 0.5 → filtered. New: avg 0.5 → μ=0.5 → passes. Sigmoid preserves partial effectiveness.");
        }
    }

    @Nested
    @DisplayName("Phase 1.5: FeedbackLoopCloser — cascade threshold 0.05")
    class CascadeThreshold {

        @Test
        @DisplayName("delta=0.05 -> cascadeMembership=0.5 (boundary)")
        void atThreshold() {
            var closer = new FeedbackLoopCloser(null, null, null, null, null);
            double mu = closer.cascadeMembership(0.05);
            assertEquals(0.5, mu, 0.001);
        }

        @Test
        @DisplayName("delta=0.06 -> μ≈0.73 (k=100, clear transition above)")
        void justAbove() {
            var closer = new FeedbackLoopCloser(null, null, null, null, null);
            double mu = closer.cascadeMembership(0.06);
            assertTrue(mu > 0.7, "k=100, 0.01 above threshold should give μ>0.7, got " + mu);
            assertTrue(mu < 0.76, "Expected μ≈0.73, got " + mu);
        }

        @Test
        @DisplayName("delta=0.04 -> μ≈0.27 (k=100, clear transition below)")
        void justBelow() {
            var closer = new FeedbackLoopCloser(null, null, null, null, null);
            double mu = closer.cascadeMembership(0.04);
            assertTrue(mu < 0.3, "k=100, 0.01 below threshold should give μ<0.3, got " + mu);
            assertTrue(mu > 0.24, "Expected μ≈0.27, got " + mu);
        }
    }

    @Nested
    @DisplayName("Phase 2: RerankerTrainingScheduler — minSamples=50")
    class RerankerSamples {

        @Test
        @DisplayName("count=50 -> μ=0.5 (boundary)")
        void atBoundary() {
            double mu = MembershipFunctions.sigmoid(50, 50, 1.0);
            assertEquals(0.5, mu, 0.001);
        }

        @Test
        @DisplayName("count=45 -> μ≈0.18 (below, skip training)")
        void belowBoundary() {
            double mu = MembershipFunctions.sigmoid(45, 50, 1.0);
            assertTrue(mu < 0.2, "Expected μ < 0.2, got " + mu);
        }

        @Test
        @DisplayName("count=55 -> μ≈0.82 (above, proceed training)")
        void aboveBoundary() {
            double mu = MembershipFunctions.sigmoid(55, 50, 1.0);
            assertTrue(mu > 0.8, "Expected μ > 0.8, got " + mu);
        }
    }

    @Nested
    @DisplayName("P2: decayFactor 隶属度边界（模糊 sigmoid，与 memory tiering 无关）")
    class ReflectionDecay {

        @Test
        @DisplayName("decayFactor=0.3 -> μ=0.5 (boundary, included)")
        void atBoundary() {
            double mu = MembershipFunctions.sigmoid(0.3, 0.3, 4.0);
            assertEquals(0.5, mu, 0.001);
            assertTrue(mu >= 0.5);
        }

        @Test
        @DisplayName("decayFactor=0.29 -> μ≈0.49 (just below, filtered)")
        void justBelow() {
            double mu = MembershipFunctions.sigmoid(0.29, 0.3, 4.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5, got " + mu);
            assertTrue(mu > 0.47, "Expected μ > 0.47 (close to boundary), got " + mu);
        }

        @Test
        @DisplayName("decayFactor=0.25 -> μ≈0.45 (below but smooth, k=4.0)")
        void wellBelow() {
            double mu = MembershipFunctions.sigmoid(0.25, 0.3, 4.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5, got " + mu);
            assertTrue(mu > 0.4, "Expected μ > 0.4 (smooth transition), got " + mu);
        }
    }

    @Nested
    @DisplayName("P2: DefaultCognitionEngine — relation classification")
    class RelationClassification {

        @Test
        @DisplayName("similarity=0.8 -> equivMu=0.5 (equivalent boundary)")
        void equivalentBoundary() {
            double equivMu = MembershipFunctions.sigmoid(0.8, 0.8, 8.0);
            assertEquals(0.5, equivMu, 0.001);
            assertTrue(equivMu >= 0.5, "At 0.8, should classify as 'equivalent'");
        }

        @Test
        @DisplayName("similarity=0.79 -> equivMu≈0.48, relatedMu≈0.97 -> 'related'")
        void justBelowEquivalent() {
            double equivMu = MembershipFunctions.sigmoid(0.79, 0.8, 8.0);
            double relatedMu = MembershipFunctions.sigmoid(0.79, 0.5, 8.0);
            assertTrue(equivMu < 0.5, "equivMu should be < 0.5, got " + equivMu);
            assertTrue(relatedMu >= 0.5, "relatedMu should be >= 0.5, got " + relatedMu);
        }

        @Test
        @DisplayName("similarity=0.5 -> equivMu≈0.002, relatedMu=0.5 -> 'related'")
        void relatedBoundary() {
            double equivMu = MembershipFunctions.sigmoid(0.5, 0.8, 8.0);
            double relatedMu = MembershipFunctions.sigmoid(0.5, 0.5, 8.0);
            assertTrue(equivMu < 0.5, "equivMu should be < 0.5, got " + equivMu);
            assertEquals(0.5, relatedMu, 0.001, "relatedMu should be 0.5 at boundary");
        }

        @Test
        @DisplayName("similarity=0.49 -> equivMu≈0.002, relatedMu≈0.48 -> 'weakly_related'")
        void justBelowRelated() {
            double equivMu = MembershipFunctions.sigmoid(0.49, 0.8, 8.0);
            double relatedMu = MembershipFunctions.sigmoid(0.49, 0.5, 8.0);
            assertTrue(equivMu < 0.5, "equivMu should be < 0.5, got " + equivMu);
            assertTrue(relatedMu < 0.5, "relatedMu should be < 0.5, got " + relatedMu);
        }
    }

    @Nested
    @DisplayName("P2: DefaultCognitionEngine — preference > 0.5")
    class PreferenceClassification {

        @Test
        @DisplayName("avgScore=0.5 -> μ=0.5 (boundary, 'high')")
        void atBoundary() {
            double mu = MembershipFunctions.sigmoid(0.5, 0.5, 4.0);
            assertEquals(0.5, mu, 0.001);
            assertTrue(mu >= 0.5, "At 0.5, should be 'high'");
        }

        @Test
        @DisplayName("avgScore=0.49 -> μ≈0.49 (just below, 'low')")
        void justBelow() {
            double mu = MembershipFunctions.sigmoid(0.49, 0.5, 4.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5, got " + mu);
        }
    }

    @Nested
    @DisplayName("P2: DefaultCognitionEngine — relation build > 0.1")
    class RelationBuild {

        @Test
        @DisplayName("similarity=0.1 -> μ=0.5 (boundary, relation built)")
        void atBoundary() {
            double mu = MembershipFunctions.sigmoid(0.1, 0.1, 4.0);
            assertEquals(0.5, mu, 0.001);
            assertTrue(mu >= 0.5);
        }

        @Test
        @DisplayName("similarity=0.09 -> μ≈0.49 (just below, no relation)")
        void justBelow() {
            double mu = MembershipFunctions.sigmoid(0.09, 0.1, 4.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5, got " + mu);
            assertTrue(mu > 0.47, "Expected μ > 0.47 (very close), got " + mu);
        }
    }

    @Nested
    @DisplayName("P2: RerankerAbSwitcher — bucket threshold")
    class AbSwitcher {

        @Test
        @DisplayName("10% traffic: bucket=10 -> μ≈0.5 (boundary)")
        void atBoundary() {
            // targetBucket = 10% * 100 = 10
            double mu = MembershipFunctions.sigmoid(10, 10, 2.0);
            assertEquals(0.5, mu, 0.001);
            // switchMu < 0.5 -> use new model
            assertTrue(mu < 0.5 + 0.001);
        }

        @Test
        @DisplayName("10% traffic: bucket=9 -> μ≈0.12 (use new model)")
        void belowBoundary() {
            double mu = MembershipFunctions.sigmoid(9, 10, 2.0);
            assertTrue(mu < 0.5, "Expected μ < 0.5 (use new), got " + mu);
        }

        @Test
        @DisplayName("10% traffic: bucket=11 -> μ≈0.88 (use current model)")
        void aboveBoundary() {
            double mu = MembershipFunctions.sigmoid(11, 10, 2.0);
            assertTrue(mu > 0.5, "Expected μ > 0.5 (use current), got " + mu);
        }

        @Test
        @DisplayName("Actual switcher: 10% traffic, requestId hashing")
        void actualSwitcher() {
            var switcher = new RerankerAbSwitcher(0.10);
            int newModelCount = 0;
            int totalRequests = 1000;
            for (int i = 0; i < totalRequests; i++) {
                if (switcher.useNewModel("req-" + i)) {
                    newModelCount++;
                }
            }
            // With 10% traffic, expect ~100 requests (±15% tolerance for sigmoid smoothing)
            double ratio = (double) newModelCount / totalRequests;
            assertTrue(
                    ratio > 0.08 && ratio < 0.13,
                    "Expected ~10% new model usage, got " + (ratio * 100) + "%");
        }
    }

    @Nested
    @DisplayName("Cross-cutting: Old vs New behavior comparison")
    class OldVsNewComparison {

        @Test
        @DisplayName("SNR boundary: old has cliff at 2.0, new is smooth")
        void snrCliffEliminated() {
            double oldJustBelow = 0.0; // hard threshold: snr=1.99 -> false -> 0.0
            double oldJustAbove = 1.0; // hard threshold: snr=2.01 -> true -> 1.0
            double newJustBelow = MembershipFunctions.sigmoid(1.99, 2.0, 2.0);
            double newJustAbove = MembershipFunctions.sigmoid(2.01, 2.0, 2.0);

            double oldGap = oldJustAbove - oldJustBelow;
            double newGap = newJustAbove - newJustBelow;

            assertTrue(
                    newGap < oldGap,
                    "Sigmoid gap ("
                            + newGap
                            + ") should be smaller than hard threshold gap ("
                            + oldGap
                            + ")");
            assertTrue(
                    newGap < 0.01,
                    "Sigmoid gap at ±0.01 from boundary should be < 0.01, got " + newGap);
        }

        @Test
        @DisplayName("confidence boundary: old has cliff at 0.3, new is smooth")
        void confidenceCliffEliminated() {
            double oldGap = 1.0; // old: 0.29->false, 0.31->true, gap=1.0
            double newGap =
                    MembershipFunctions.sigmoid(0.31, 0.3, 4.0)
                            - MembershipFunctions.sigmoid(0.29, 0.3, 4.0);
            assertTrue(newGap < oldGap, "Sigmoid gap should be smaller than hard threshold gap");
        }
    }
}
