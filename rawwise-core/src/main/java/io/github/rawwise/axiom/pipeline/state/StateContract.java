package io.github.rawwise.axiom.pipeline.state;

import java.util.List;
import java.util.Optional;

public interface StateContract {

    Optional<ContractViolation> validate(StateSpace space, StateEntry entry, String toState);

    List<ContractViolation> audit(StateSpace space);
}
