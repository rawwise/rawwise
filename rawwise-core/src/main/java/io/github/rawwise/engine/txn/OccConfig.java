package io.github.rawwise.engine.txn;

/**
 * Immutable OCC configuration loaded once at startup.
 *
 * <p>Per the review-convergence R2-L1 fix: uses a Java 21 record with
 * {@code @RequiredArgsConstructor} injection pattern. The flag is read-once at startup and never
 * changes during the process lifetime, eliminating runtime-flip race conditions.
 */
public record OccConfig(boolean perKeyEnabled) {

    public static final OccConfig DISABLED = new OccConfig(false);

    public boolean isPerKey() {
        return perKeyEnabled;
    }

    public boolean isTableLevel() {
        return !perKeyEnabled;
    }
}
