package io.github.rawwise.frontend.ir;

/**
 * UPDATE/DELETE execution mode.
 *
 * <ul>
 *   <li>{@link #POINT} — Primary key equality predicate, goes through FAST_PATH (single gRPC
 *       auto-commit transaction)
 *   <li>{@link #LAZY} — Non-primary-key predicate, goes through LOGICAL_PLAN → LazyUpdate
 * </ul>
 */
public enum UpdateMode {
    POINT,
    LAZY
}
