package io.github.rawwise.axiom.orchestrator;

public enum ValidationErrorType {
    CYCLE,
    ORPHAN_STEP,
    UNREACHABLE_STEP,
    INVALID_EDGE_REF,
    SELF_LOOP,
    TOO_DEEP,
    TOO_MANY_FAN_OUT,
    DUPLICATE_STEP_ID,
    INVALID_STEP_ID_FORMAT,
    TOO_MANY_STEPS
}
