package io.github.rawwise.model.op.commit;

import io.github.rawwise.metastore.CommitResult;
import java.io.IOException;

public interface CommitStrategy {
    CommitResult commit(CommitRequest request) throws IOException;

    CommitResult commitSnapshotUpdate(String tableId, CommitContext context) throws IOException;
}
