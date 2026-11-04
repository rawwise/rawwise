package io.github.rawwise.backend.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

@Slf4j
public final class RawWiseRowLevelOperation implements RowLevelOperation {

    private final Command command;

    RawWiseRowLevelOperation(Command command) {
        this.command = command;
    }

    @Override
    public Command command() {
        return command;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        throw new UnsupportedOperationException(
                "RawWiseRowLevelOperation.newScanBuilder() is not yet implemented. "
                        + "UPDATE interception is handled by LazyUpdateRewriteRule.");
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new RawWiseRowLevelWriteBuilder();
    }

    private static final class RawWiseRowLevelWriteBuilder implements WriteBuilder {
        @Override
        public BatchWrite buildForBatch() {
            return new RawWiseRowLevelBatchWrite();
        }
    }

    private static final class RawWiseRowLevelBatchWrite implements BatchWrite {
        @Override
        public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
            return (partitionId, taskId) -> new RawWiseRowLevelDataWriter();
        }

        @Override
        public void commit(WriterCommitMessage[] messages) {
            log.info(
                    "RawWiseRowLevelBatchWrite commit: {} messages",
                    messages != null ? messages.length : 0);
        }

        @Override
        public void abort(WriterCommitMessage[] messages) {
            log.warn(
                    "RawWiseRowLevelBatchWrite abort: {} messages",
                    messages != null ? messages.length : 0);
        }
    }

    static final class RawWiseRowLevelDataWriter implements DataWriter<InternalRow> {
        private long count = 0;

        @Override
        public void write(InternalRow record) {
            count++;
            throw new UnsupportedOperationException(
                    "RawWiseRowLevelDataWriter.write() is not yet implemented. "
                            + "UPDATE interception is handled by LazyUpdateRewriteRule. "
                            + "Row "
                            + count
                            + " rejected to prevent silent data loss.");
        }

        @Override
        public WriterCommitMessage commit() {
            log.info("RawWiseRowLevelDataWriter commit: {} rows processed", count);
            return new RowLevelUpdateWriterCommitMessage(count);
        }

        @Override
        public void abort() {
            log.warn("RawWiseRowLevelDataWriter abort: {} rows discarded", count);
        }

        @Override
        public void close() {}
    }

    private record RowLevelUpdateWriterCommitMessage(long count) implements WriterCommitMessage {}
}
