package io.github.rawwise.common.catalog;

import java.io.IOException;
import java.util.List;

/**
 * Functional interface for rebuilding commit operations on OCC conflict retry. Allows orchestrators
 * (SnapshotManager) to merge their operations with concurrent commits discovered on conflict.
 *
 * @param freshBaseSnapshotId the latest snapshotId after the conflicting commit
 * @return rebuilt list of TableOperations for the retry attempt
 */
@FunctionalInterface
public interface OperationRebuilder {
    List<CommitTransaction.TableOperation> rebuild(long freshBaseSnapshotId) throws IOException;
}
