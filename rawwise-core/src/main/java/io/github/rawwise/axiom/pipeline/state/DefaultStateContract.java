package io.github.rawwise.axiom.pipeline.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DefaultStateContract implements StateContract {

    private final GuardExpressionEvaluator guardEvaluator;

    public DefaultStateContract() {
        this(new GuardExpressionEvaluator());
    }

    public DefaultStateContract(GuardExpressionEvaluator guardEvaluator) {
        this.guardEvaluator = guardEvaluator;
    }

    @Override
    public Optional<ContractViolation> validate(
            StateSpace space, StateEntry entry, String toState) {
        StateTransition transition =
                space.transitions().stream()
                        .filter(
                                t ->
                                        t.fromState().equals(entry.stateName())
                                                && t.toState().equals(toState))
                        .findFirst()
                        .orElse(null);

        if (transition == null) {
            return Optional.of(
                    new ContractViolation(
                            space.spaceId(),
                            entry.key(),
                            entry.stateName(),
                            toState,
                            null,
                            "Transition not defined: " + entry.stateName() + " -> " + toState));
        }

        if (!space.states().contains(toState)) {
            return Optional.of(
                    new ContractViolation(
                            space.spaceId(),
                            entry.key(),
                            entry.stateName(),
                            toState,
                            null,
                            "Target state not defined: " + toState));
        }

        if (transition.guardExpression() != null && !transition.guardExpression().isBlank()) {
            if (!guardEvaluator.evaluate(transition.guardExpression(), entry.metadata())) {
                return Optional.of(
                        new ContractViolation(
                                space.spaceId(),
                                entry.key(),
                                entry.stateName(),
                                toState,
                                transition.guardExpression(),
                                "Guard failed: "
                                        + transition.guardExpression()
                                        + " (metadata="
                                        + entry.metadata()
                                        + ")"));
            }
        }

        return Optional.empty();
    }

    @Override
    public List<ContractViolation> audit(StateSpace space) {
        List<ContractViolation> violations = new ArrayList<>();
        for (StateTransition t : space.transitions()) {
            if (!space.states().contains(t.fromState())) {
                violations.add(
                        new ContractViolation(
                                space.spaceId(),
                                "",
                                t.fromState(),
                                t.toState(),
                                t.guardExpression(),
                                "Source state not defined: " + t.fromState()));
            }
            if (!space.states().contains(t.toState())) {
                violations.add(
                        new ContractViolation(
                                space.spaceId(),
                                "",
                                t.fromState(),
                                t.toState(),
                                t.guardExpression(),
                                "Target state not defined: " + t.toState()));
            }
        }
        return violations;
    }
}
