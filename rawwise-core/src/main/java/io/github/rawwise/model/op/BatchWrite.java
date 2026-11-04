package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.writer.BatchWriter;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.model.record.WriteEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchWrite extends AbstractWrite {
    private final List<WriteEntry> records;

    public BatchWrite(List<WriteEntry> records, RecordWriter<?> writer, Properties config) {
        super(writer, config);
        this.records = records;
    }

    @Override
    protected void doWrite() throws Exception {
        if (writer instanceof BatchWriter<?> batchWriter) {
            @SuppressWarnings("unchecked")
            BatchWriter<WriteEntry> typed = (BatchWriter<WriteEntry>) batchWriter;
            typed.batchWrite(records);
        } else if (writer instanceof RecordWriter<?> recordWriter) {
            @SuppressWarnings("unchecked")
            RecordWriter<WriteEntry> typed = (RecordWriter<WriteEntry>) recordWriter;
            for (WriteEntry record : records) {
                typed.write(record);
            }
        }
    }

    @Override
    public OpType type() {
        return OpType.BATCH_WRITE;
    }

    public List<WriteEntry> records() {
        return records;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements WriteBuilder<BatchWrite, Builder> {
        private io.github.rawwise.model.record.RawWiseTable table;
        private List<WriteEntry> records = new ArrayList<>();
        private RecordWriter<?> writer;
        private Properties config = new Properties();

        @Override
        public Builder withTable(io.github.rawwise.model.record.RawWiseTable table) {
            this.table = table;
            return this;
        }

        public Builder withRecords(List<WriteEntry> records) {
            this.records = records;
            return this;
        }

        public Builder addRecord(WriteEntry record) {
            this.records.add(record);
            return this;
        }

        @Override
        public Builder withWriter(RecordWriter<?> writer) {
            this.writer = writer;
            return this;
        }

        @Override
        public Builder withConfig(Properties config) {
            this.config = config;
            return this;
        }

        @Override
        public io.github.rawwise.model.record.RawWiseTable table() {
            return table;
        }

        @Override
        public BatchWrite build() {
            if (records.isEmpty()) {
                throw new IllegalStateException("records must not be empty");
            }
            if (writer == null) {
                throw new IllegalStateException("writer must be set");
            }
            return new BatchWrite(records, writer, config);
        }
    }
}
