package io.github.rawwise.engine.txn;

/**
 * UNIQUE constraint OCC-based conflictdetection.
 *
 * <p>Design principleslogic:
 *
 * <ol>
 *   <li>INSERT beforetune {@link #checkInsertUnique(ShardVersionTracker, byte[], long)} — query
 *       {@code tracker.getCommitTs(key)}, expectsreturn 0(tableshowthe key not yet by
 *       anytransactioncommit).
 *   <li>ifcurrently commitTs &gt; 0, explanationexistingtransactionwritethevalue → return conflict.
 *   <li>concurrent INSERT same unique key: secondtransactionin ValidateStage OCC write-write check
 *       inbecause baseVersion notconsistentbereject(becauseas tracker intwotransaction begin after
 *       only see 0, but commit stage firstsuccesstransactionalreadywill commitTs advance, causing
 *       latter's OCC checkfail).
 * </ol>
 *
 * <p>and {@link ShardVersionTracker#holdsExclusiveLock} interaction(H-8): iftransactionalreadyto
 * unique key hold FOR UPDATE lock, OCC write-writecheckwillbeskip, thiswhen UNIQUE protect
 * byrowlockprovide(othertransactioncannot insert same key).
 */
public final class UniqueConstraintChecker {

    private UniqueConstraintChecker() {}

    /**
     * check INSERT UNIQUE key whetheralreadyexist.
     *
     * @param tracker ShardVersionTracker(include per-key commitTs info)
     * @param uniqueKey uniquekey encoded key
     * @param txnId currentlytransaction ID
     * @return conflictreport(hasConflict=true tableshowalreadyexist)
     */
    public static boolean isDuplicate(ShardVersionTracker tracker, byte[] uniqueKey) {
        return tracker.getCommitTs(uniqueKey) > 0;
    }
}
