package io.github.rawwise.engine.txn.ingest;

import java.util.Objects;

/**
 * Batch commitdigestinfoset.
 *
 * <p>for {@link CommitLedger} digests field, storagebatchdatacompleteityvalidatedigest, forcrash
 * recoverywhenverifydatacompleteity(CRC32 validate).
 *
 * @param manifestDigest Manifest digest(Hex format CRC32C / SHA-256)
 * @param dataDigest datadigest(Hex format, overrideall shard data)
 * @param sealedDigest sealdigest(Hex format, WriteSet CRC32 digest, corresponding WAL
 *     TXN_PREPARE_AND_DURABLE sealedDigest)
 */
public record Digests(String manifestDigest, String dataDigest, String sealedDigest) {

    public Digests {
        Objects.requireNonNull(manifestDigest, "manifestDigest must not be null");
        Objects.requireNonNull(dataDigest, "dataDigest must not be null");
        Objects.requireNonNull(sealedDigest, "sealedDigest must not be null");
    }

    /** emptydigest(fornot yetcomputedigest initialstate) */
    public static Digests empty() {
        return new Digests("", "", "");
    }
}
