package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.writer.Initializable;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.model.record.RawWiseTable;
import java.util.Map;
import java.util.Properties;

public class Overwrite extends AbstractWrite {
    private final Map<String, Comparable<?>> keyValues;

    public Overwrite(
            RawWiseTable table,
            Map<String, Comparable<?>> keyValues,
            RecordWriter<?> writer,
            Properties config) {
        super(writer, config);
        this.keyValues = keyValues;
    }

    @Override
    protected void doWrite() throws Exception {
        if (writer instanceof Initializable initable) {
            initable.init(config);
        }
    }

    @Override
    public OpType type() {
        return OpType.OVERWRITE;
    }

    public Map<String, Comparable<?>> keyValues() {
        return keyValues;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements WriteBuilder<Overwrite, Builder> {
        private RawWiseTable table;
        private Map<String, Comparable<?>> keyValues;
        private RecordWriter<?> writer;
        private Properties config = new Properties();

        @Override
        public Builder withTable(RawWiseTable table) {
            this.table = table;
            return this;
        }

        public Builder withKeyValues(Map<String, Comparable<?>> keyValues) {
            this.keyValues = keyValues;
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
        public RawWiseTable table() {
            return table;
        }

        @Override
        public Overwrite build() {
            if (keyValues == null || keyValues.isEmpty()) {
                throw new IllegalStateException("keyValues must be set");
            }
            if (writer == null) {
                throw new IllegalStateException("writer must be set");
            }
            return new Overwrite(table, keyValues, writer, config);
        }
    }
}
