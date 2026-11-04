package io.github.rawwise.backend.flink;

import io.github.rawwise.backend.flink.schema.FlinkSchemaConverter;
import io.github.rawwise.engine.storage.ingestion.IngestionDataSink;
import io.github.rawwise.engine.storage.ingestion.SinkFatalException;
import io.github.rawwise.engine.storage.ingestion.SinkRetryableException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

@Slf4j
public class RawWiseFlinkWriter
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<RowData, RawWiseCommittable> {

    private final String tableId;
    private final RowType rowType;
    private final int[] pkIndices;
    private final IngestionDataSink sink;
    private final int maxBatchRows;
    private final long maxBatchBytes;
    private final AtomicLong seqGenerator;
    private final boolean autoFlush;

    private final int subTaskIndex;
    private final long writerInstanceId;
    private final AtomicLong checkpointCounter = new AtomicLong(1);

    private List<byte[]> bufferedKeys = new ArrayList<>();
    private List<byte[]> bufferedValues = new ArrayList<>();
    private long bufferedBytes = 0;

    private final List<IngestionDataSink.FlushResult> flushResults = new ArrayList<>();
    private long currentTxnId;
    private long schemaVersion;

    private long batchMinSeq = Long.MAX_VALUE;
    private long batchMaxSeq = Long.MIN_VALUE;
    private long checkpointMinSeq = Long.MAX_VALUE;
    private long checkpointMaxSeq = Long.MIN_VALUE;

    public RawWiseFlinkWriter(
            String tableId,
            RowType rowType,
            int[] pkIndices,
            IngestionDataSink sink,
            int maxBatchRows,
            long maxBatchBytes,
            AtomicLong seqGenerator,
            boolean autoFlush,
            int subTaskIndex) {
        this(
                tableId,
                rowType,
                pkIndices,
                sink,
                maxBatchRows,
                maxBatchBytes,
                seqGenerator,
                autoFlush,
                subTaskIndex,
                subTaskIndex);
    }

    public RawWiseFlinkWriter(
            String tableId,
            RowType rowType,
            int[] pkIndices,
            IngestionDataSink sink,
            int maxBatchRows,
            long maxBatchBytes,
            AtomicLong seqGenerator,
            boolean autoFlush,
            int subTaskIndex,
            long writerInstanceId) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.pkIndices = pkIndices;
        this.sink = sink;
        this.maxBatchRows = maxBatchRows;
        this.maxBatchBytes = maxBatchBytes;
        this.seqGenerator = seqGenerator;
        this.autoFlush = autoFlush;
        this.subTaskIndex = subTaskIndex;
        this.writerInstanceId = writerInstanceId;
        this.currentTxnId = seqGenerator.getAndIncrement();
        this.schemaVersion = 0;
    }

    @Override
    public void write(RowData row, Context context) throws IOException {
        byte[] key = FlinkSchemaConverter.encodePrimaryKey(row, pkIndices, rowType);
        byte[] value = FlinkSchemaConverter.encodeRowValue(row, rowType);

        bufferedKeys.add(key);
        bufferedValues.add(value);
        bufferedBytes += key.length + value.length;

        long seq = seqGenerator.getAndIncrement();
        batchMinSeq = Math.min(batchMinSeq, seq);
        batchMaxSeq = Math.max(batchMaxSeq, seq);
        checkpointMinSeq = Math.min(checkpointMinSeq, seq);
        checkpointMaxSeq = Math.max(checkpointMaxSeq, seq);

        if (autoFlush && (bufferedKeys.size() >= maxBatchRows || bufferedBytes >= maxBatchBytes)) {
            flushBuffer();
        }
    }

    private void flushBuffer() throws IOException {
        if (bufferedKeys.isEmpty()) return;

        List<byte[]> keysToFlush = this.bufferedKeys;
        List<byte[]> valuesToFlush = this.bufferedValues;
        long currentBatchMinSeq = this.batchMinSeq;
        long currentBatchMaxSeq = this.batchMaxSeq;

        try {
            IngestionDataSink.FlushResult result =
                    sink.flushBatch(
                            tableId,
                            currentTxnId,
                            keysToFlush,
                            valuesToFlush,
                            schemaVersion,
                            currentBatchMinSeq,
                            currentBatchMaxSeq,
                            checkpointCounter.get(),
                            subTaskIndex,
                            writerInstanceId);
            flushResults.add(result);
        } catch (SinkRetryableException e) {
            throw new IOException("Retryable flush error", e);
        } catch (SinkFatalException e) {
            log.error("Fatal flush error for tableId={}, txnId={}", tableId, currentTxnId, e);
            throw e;
        }

        this.bufferedKeys = new ArrayList<>();
        this.bufferedValues = new ArrayList<>();
        this.bufferedBytes = 0;
        this.batchMinSeq = Long.MAX_VALUE;
        this.batchMaxSeq = Long.MIN_VALUE;
    }

    @Override
    public void flush(boolean checkpointBarrier) throws IOException {
        flushBuffer();
    }

    @Override
    public Collection<RawWiseCommittable> prepareCommit() throws IOException {
        flushBuffer();

        List<String> allPendingIds =
                flushResults.stream().flatMap(r -> r.pendingIds().stream()).toList();
        flushResults.clear();

        if (allPendingIds.isEmpty()) {
            long currentCkpt = checkpointCounter.getAndIncrement();
            return Collections.singletonList(
                    RawWiseCommittable.empty(tableId, currentTxnId, currentCkpt, schemaVersion));
        }

        try {
            sink.prepare(tableId, allPendingIds, currentTxnId);
        } catch (SinkRetryableException e) {
            throw new IOException("Retryable prepare error", e);
        } finally {
            sink.releasePrepared(tableId, allPendingIds, currentTxnId);
        }

        long currentCkpt = checkpointCounter.getAndIncrement();

        RawWiseCommittable committable =
                new RawWiseCommittable(
                        tableId,
                        allPendingIds,
                        currentTxnId,
                        currentCkpt,
                        schemaVersion,
                        checkpointMinSeq,
                        checkpointMaxSeq,
                        System.currentTimeMillis());

        currentTxnId = seqGenerator.getAndIncrement();
        checkpointMinSeq = Long.MAX_VALUE;
        checkpointMaxSeq = Long.MIN_VALUE;

        return Collections.singletonList(committable);
    }

    int bufferedCount() {
        return bufferedKeys.size();
    }

    @Override
    public void close() throws IOException {
        try {
            flushBuffer();
        } catch (IOException e) {
            log.warn("Flush on close failed", e);
        } finally {
            try {
                sink.close();
            } catch (Exception e) {
                log.warn("Failed to close ingestion data sink", e);
            }
        }
    }
}
