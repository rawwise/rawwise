package io.github.rawwise.engine.txn;

import java.util.OptionalLong;

/**
 * Probes the LMDB record header for the commitTimestamp of a given key.
 *
 * <p>Used by {@link ShardVersionTracker} to backfill the version cache on miss. Implementations
 * read the raw LMDB bytes and extract {@code Record.Header.commitTimestamp()} without decoding the
 * full record body.
 */
@FunctionalInterface
public interface RecordHeaderProbe {

    /**
     * A no-op probe that always returns empty. Useful for testing and for {@link
     * ShardVersionTracker} instances that have not yet been connected to a storage backend.
     */
    RecordHeaderProbe NOOP = key -> OptionalLong.empty();

    /**
     * Returns the commitTimestamp from the LMDB record header.
     *
     * @param key the record key in raw bytes
     * @return OptionalLong.empty() when the key is not found; a value when found
     * @throws io.github.rawwise.engine.storage.StorageException when read fails (corruption,
     *     checksum mismatch, IO error)
     */
    OptionalLong probeCommitTs(byte[] key);
}
