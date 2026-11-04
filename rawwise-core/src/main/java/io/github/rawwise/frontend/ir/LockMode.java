package io.github.rawwise.frontend.ir;

/**
 * Row lock mode.
 *
 * <ul>
 *   <li>{@link #NONE} — Normal SELECT, no locking (default)
 *   <li>{@link #FOR_UPDATE} — SELECT ... FOR UPDATE, pessimistic EXCLUSIVE row lock
 *   <li>{@link #FOR_SHARE} — SELECT ... FOR SHARE, pessimistic SHARED row lock (not yet
 *       implemented)
 * </ul>
 */
public enum LockMode {
    NONE,
    FOR_UPDATE,
    FOR_SHARE
}
