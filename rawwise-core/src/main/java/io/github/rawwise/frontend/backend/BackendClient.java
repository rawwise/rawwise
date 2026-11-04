package io.github.rawwise.frontend.backend;

import io.github.rawwise.backend.server.txn.TxnHandle;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.server.ClusterStatus;
import io.github.rawwise.server.JobState;
import io.github.rawwise.server.QueryType;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BackendClient {

    SubmitResult submitQuery(String sql, QueryType type, Map<String, String> options);

    SubmitResult submitQuery(
            String sql,
            QueryType type,
            Map<String, String> options,
            long txnId,
            String frontendSessionId);

    SubmitIRResult submitIR(QueryIR ir, long timeoutMs);

    JobState getJobStatus(String jobId);

    boolean cancelJob(String jobId);

    ClusterStatus getClusterStatus();

    QueryResult getQueryResult(String jobId);

    QueryResult getQueryResult(String jobId, int offset, int limit, int maxBytes);

    TxnHandle beginTxn(Set<String> tableIds, IsolationLevel level);

    TxnResult commitTxn(long txnId);

    TxnResult abortTxn(long txnId);

    record SubmitResult(String jobId, boolean success, String errorMessage) {}

    record SubmitIRResult(String jobId, boolean accepted, String error) {}

    record ColumnInfo(String name, String type) {}

    record QueryResult(
            List<ColumnInfo> columns,
            List<List<Object>> rows,
            int rowCount,
            boolean truncated,
            String nextPageToken) {}
}
