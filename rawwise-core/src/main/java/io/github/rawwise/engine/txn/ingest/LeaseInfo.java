package io.github.rawwise.engine.txn.ingest;

import java.util.Objects;

/**
 * Batch commitleaseinfo.
 *
 * <p>for {@link CommitLedger} lease field, implementationBatch commitlease mechanism,
 * preventmultipleitem Driver whileoperationsameonebatch(persistencestate machine).
 *
 * <p>leasesemantics:
 *
 * <ul>
 *   <li>Driver getonly after leaseoperation CommitLedger
 *   <li>after lease expires, other Driver canpreempt(CAS update leaseVersion)
 *   <li>leaserenew: sameone ownerId canupdate expireAtMs
 * </ul>
 *
 * <p>note: thistypeand {@code io.github.rawwise.engine.storage.LmdbInstance.LeaseInfo} notsame.
 * afteroneis LMDB Instance levelfilelocklease; thistypeisBatch commitlevel Metastore lease.
 *
 * @param ownerId leaseholderidentifier(Driver ID)
 * @param expireAtMs lease expiration time(epoch millis)
 * @param leaseVersion leaseversion number(CAS optimisticlock, eachtimerenew/preemptincrementing)
 */
public record LeaseInfo(String ownerId, long expireAtMs, long leaseVersion) {

    public LeaseInfo {
        Objects.requireNonNull(ownerId, "ownerId must not be null");
    }

    /**
     * determinewhether lease expired.
     *
     * @param nowMs currentlywhenbetween(epoch millis)
     * @return true iflease expired
     */
    public boolean isExpired(long nowMs) {
        return nowMs >= expireAtMs;
    }
}
