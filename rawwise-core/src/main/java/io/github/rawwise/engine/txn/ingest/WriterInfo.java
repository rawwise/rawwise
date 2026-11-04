package io.github.rawwise.engine.txn.ingest;

import java.util.Objects;

/**
 * writeridentifierinfo.
 *
 * <p>for {@link GlobalCommitMarker} writer field, recordcreatethe marker writeridentity, forcrash
 * recoverywhenlocatewriterandenterrowidempotentvalidate.
 *
 * @param writerId writeruniqueidentifier(such as Spark Driver ID / Flink JobManager ID)
 * @param nodeId Backend Node identifier(Server sideas Driver nodeId)
 * @param attemptId task attempt ID(Spark taskAttemptId / Flink checkpointId)
 */
public record WriterInfo(String writerId, String nodeId, String attemptId) {

    public WriterInfo {
        Objects.requireNonNull(writerId, "writerId must not be null");
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(attemptId, "attemptId must not be null");
    }
}
