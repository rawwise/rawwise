package io.github.rawwise.engine.txn.store;

import io.github.rawwise.engine.txn.isolate.ReadSnapshot;
import io.github.rawwise.engine.txn.state.TxnContext;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Point-In-Time reader, implements floor seek semantics.
 *
 * <p>core semantics: given tableId + key + snapshotTs, returns commitTs &le; snapshotTs the latest
 * data version of.
 *
 * <h3>Read flow</h3>
 *
 * <ol>
 *   <li>Locate the version chain for (tableId, pk) in VersionChainStore
 *   <li>Find the latest version with commitTs &le; snapshotTs from the version chain
 *   <li>If no visible version in the chain, return Optional.empty()
 * </ol>
 *
 * <h3>Version chain Key format</h3>
 *
 * <pre>tableId|pk|invertedCommitTs → value</pre>
 *
 * invertedCommitTs = Long.MAX_VALUE - commitTs(workaround for descending order arrangement)
 */
@Slf4j
public final class PITReader {

    private final VersionChainStore versionChainStore;

    public PITReader(VersionChainStore versionChainStore) {
        this.versionChainStore = versionChainStore;
    }

    /**
     * PIT point read: returns the latest version at the specified snapshot.
     *
     * @param tableId table ID
     * @param key key
     * @param snapshotTs snapshot timestamp
     * @return visible version's value, or Optional.empty() if none
     */
    public Optional<byte[]> pointRead(String tableId, byte[] key, long snapshotTs) {
        return versionChainStore
                .getAtSnapshot(tableId, key, snapshotTs)
                .map(VersionChainStore.VersionEntry::valueCopy);
    }

    /**
     * PIT point read (using TxnContext's readSnapshot).
     *
     * @param ctx transaction context
     * @param tableId table ID
     * @param key key
     * @return visible version's value, or Optional.empty() if none
     */
    public Optional<byte[]> pointRead(TxnContext ctx, String tableId, byte[] key) {
        ReadSnapshot snapshot = ctx.readSnapshot();
        if (snapshot == null) {
            // READ_COMMITTED: use latest version
            return versionChainStore
                    .latest(tableId, key)
                    .map(VersionChainStore.VersionEntry::valueCopy);
        }
        return pointRead(tableId, key, snapshot.snapshotTs());
    }

    /**
     * PIT point read and return complete VersionEntry (including commitTs). Used to capture
     * baseVersion for OCC conflict detection.
     *
     * @param ctx transaction context
     * @param tableId table ID
     * @param key key
     * @return visible VersionEntry (including commitTs), or Optional.empty() if none
     */
    public Optional<VersionChainStore.VersionEntry> pointReadWithVersion(
            TxnContext ctx, String tableId, byte[] key) {
        ReadSnapshot snapshot = ctx.readSnapshot();
        if (snapshot == null) {
            return versionChainStore.latest(tableId, key);
        }
        return versionChainStore.getAtSnapshot(tableId, key, snapshot.snapshotTs());
    }

    /** Check whether the specified key has data at the specified snapshot. */
    public boolean exists(String tableId, byte[] key, long snapshotTs) {
        return versionChainStore.getAtSnapshot(tableId, key, snapshotTs).isPresent();
    }
}
