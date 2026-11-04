package io.github.rawwise.engine.txn;

/**
 * MVCC visibleitydetermineutilityclass, according
 * toisolationlevelandsnapshotdeterminedataversionwhethertotransactionvisible.
 *
 * <p>corevisibleityrule:
 *
 * <ul>
 *   <li>READ_COMMITTED: alldataallvisible(allow null snapshot)
 *   <li>REPEATABLE_READ / SERIALIZABLE: onlywhen commitTs <= snapshot.snapshotTs whenvisible,
 *       snapshotnotenergyas null
 *   <li>commitTs == 0(nottrackdata): notheoryisolationlevel, alwaysvisible
 * </ul>
 */
public final class VisibilityChecker {

    private VisibilityChecker() {}

    /**
     * determineingiven commitTs commitdataversionwhethertotransactionvisible.
     *
     * @param level isolationlevel, notallowas null
     * @param snapshot readsnapshot(READ_COMMITTED undermay be null)
     * @param commitTs datacommit timestamp; 0 indicates not trackeddata, alwaysvisible
     * @return true ifdatatotransactionvisible
     * @throws IllegalArgumentException if level as null, or REPEATABLE_READ/SERIALIZABLE under
     *     snapshot as null
     */
    public static boolean isVisible(IsolationLevel level, ReadSnapshot snapshot, long commitTs) {
        if (level == null) {
            throw new IllegalArgumentException("level must not be null");
        }

        // nottrackdata(commitTs == 0)alwaysvisible
        if (commitTs == 0) {
            return true;
        }

        return switch (level) {
            case READ_COMMITTED -> true;
            case REPEATABLE_READ, SERIALIZABLE -> {
                if (snapshot == null) {
                    throw new IllegalArgumentException("snapshot must not be null for " + level);
                }
                yield commitTs <= snapshot.snapshotTs();
            }
        };
    }
}
