package io.github.rawwise.engine.txn.isolate;

import io.github.rawwise.engine.txn.TxnReadCache;
import io.github.rawwise.engine.txn.WriteBuffer;
import io.github.rawwise.engine.txn.state.TxnContext;
import java.util.Optional;

/**
 * visibleitydetermine, by 3 levelreadcachepriority (INV-008)
 * andisolationleveldeterminedatavisibleity.
 *
 * <h3>readpriority (INV-008)</h3>
 *
 * <ol>
 *   <li>Tier 1 — WriteBuffer: thistransactionnotcommitwriteuniquecantrusted source
 *   <li>Tier 2 — TxnReadCache: committeddatacache
 *   <li>Tier 3 — LMDB/Parquet: diskabovecommitteddata(bycallpartyhandle)
 * </ol>
 *
 * <h3>visibleityrule</h3>
 *
 * <ul>
 *   <li>READ_COMMITTED: WriteBuffer → finalreturn LMDB latest version
 *   <li>REPEATABLE_READ: WriteBuffer → TxnReadCache → LMDB floor seek: returns the latest version
 *       with commitTs &le; readSnapshot.snapshotTs from the version chain
 *   <li>SERIALIZABLE: and REPEATABLE_READ phasesame, additionallyvia {@link PhantomDetector}
 *       detectionphantom read
 * </ul>
 */
public final class VisibilityChecker {

    private VisibilityChecker() {
        /* utility class */
    }

    /**
     * in WriteBuffer and TxnReadCache infindspecified key visiblevalue.
     *
     * <p>notaccess LMDB--callpartyshouldinthismethodreturn Optional.empty() afterexecute Tier 3
     * find.
     *
     * @param ctx transaction context
     * @param tableId table ID
     * @param key keybytes
     * @return ifin Tier1/Tier2 infindtovisiblevalue, returnthevalue; otherwisereturn
     *     Optional.empty()
     */
    public static Optional<byte[]> check(TxnContext ctx, String tableId, byte[] key) {
        // Tier 1: WriteBuffer (thistransactionnotcommitwrite)
        Optional<byte[]> writeBufferValue = checkWriteBuffer(ctx, tableId, key);
        if (writeBufferValue.isPresent()) {
            return writeBufferValue;
        }

        // Tier 2: TxnReadCache (committeddatacache)
        Optional<byte[]> cachedValue = checkReadCache(ctx, tableId, key);
        if (cachedValue.isPresent()) {
            return cachedValue;
        }

        // Tier 3: LMDB (bycallpartyhandle)
        return Optional.empty();
    }

    /** Tier 1: check WriteBuffer. */
    private static Optional<byte[]> checkWriteBuffer(TxnContext ctx, String tableId, byte[] key) {
        WriteBuffer.BufferResult result = ctx.writeBuffer().get(tableId, key);
        if (!result.exists()) {
            return Optional.empty();
        }
        WriteBuffer.BufferEntry entry = result.entry();
        if (entry == null) {
            return Optional.empty();
        }
        return switch (entry.type()) {
            case NEW_VALUE -> Optional.ofNullable(entry.getValue());
            case DELETED -> Optional.empty(); // alreadydeletemark
        };
    }

    /**
     * Tier 2: check TxnReadCache.
     *
     * <p>cachecommandinwhen, alsorequiresverifycachein commitTs whetherin readSnapshot range.
     * TxnReadCache instorageiscommitteddata, key formatas tableId|pk|commitTs.
     */
    private static Optional<byte[]> checkReadCache(TxnContext ctx, String tableId, byte[] key) {
        TxnReadCache.CachedRead cached = ctx.readCache().get(tableId, key);
        if (cached == null) {
            return Optional.empty();
        }
        if (!cached.exists()) {
            return Optional.empty(); // cache"datadoes not exist"
        }
        return Optional.ofNullable(cached.getValue());
    }

    /**
     * determinegiven commitTs inthistransaction readSnapshot underwhethervisible.
     *
     * <p>toin READ_COMMITTED(readSnapshot as null), allcommittedversionallvisible. toin
     * REPEATABLE_READ/SERIALIZABLE, onlyhas commitTs &le; snapshotTs are visible.
     *
     * @param ctx transaction context
     * @param commitTs pendingcheck commitTs
     * @return true ifvisible
     */
    public static boolean isVersionVisible(TxnContext ctx, long commitTs) {
        ReadSnapshot snapshot = ctx.readSnapshot();
        if (snapshot == null) {
            return true; // READ_COMMITTED: allcommittedversionvisible
        }
        return snapshot.isVisible(commitTs);
    }
}
