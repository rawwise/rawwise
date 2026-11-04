package io.github.rawwise.axiom.pipeline.state;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GuardExpressionEvaluatorE2ETest {

    @Test
    void invalidNumericMetadataFailsGuardInsteadOfThrowing() {
        GuardExpressionEvaluator evaluator = new GuardExpressionEvaluator();

        assertDoesNotThrow(
                () -> {
                    boolean passed =
                            evaluator.evaluate("similarity > 0.9", Map.of("similarity", "unknown"));
                    assertFalse(passed);
                });
    }

    @Test
    void nonFiniteNumericMetadataFailsGuard() {
        GuardExpressionEvaluator evaluator = new GuardExpressionEvaluator();

        assertFalse(evaluator.evaluate("similarity > 0.9", Map.of("similarity", "Infinity")));
        assertFalse(evaluator.evaluate("similarity > 0.9", Map.of("similarity", "NaN")));
        assertFalse(evaluator.evaluate("similarity > NaN", Map.of("similarity", "0.95")));
    }

    @Test
    void stateContractReturnsViolationForInvalidNumericMetadata() {
        DefaultStateContract contract = new DefaultStateContract(new GuardExpressionEvaluator());
        StateSpace space =
                new StateSpace(
                        "space:video",
                        "agent:video",
                        "appearance",
                        "baseline",
                        List.of("baseline", "consistent"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of())),
                        Map.of(),
                        0L,
                        0L);
        StateEntry entry =
                new StateEntry(
                        "character_A",
                        "value",
                        "baseline",
                        1,
                        Map.of("similarity", "unknown"),
                        0L,
                        0L);

        var violation = assertDoesNotThrow(() -> contract.validate(space, entry, "consistent"));

        assertTrue(violation.isPresent());
        assertTrue(violation.get().reason().contains("Guard failed"));
    }
}
