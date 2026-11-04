package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.writer.Initializable;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionUtils;
import io.github.rawwise.model.record.PrimaryKey;
import io.github.rawwise.model.record.RawWiseTable;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;

public class Delete implements Write {
    private static final OpType TYPE = OpType.DELETE;

    private final RawWiseTable table;
    private final Expression condition;
    private final RecordWriter<?> writer;
    private final Properties config;
    private final BiConsumer<String, Map<String, Comparable<?>>> deleteCallback;

    public Delete(
            RawWiseTable table, Expression condition, RecordWriter<?> writer, Properties config) {
        this(table, condition, writer, config, null);
    }

    public Delete(
            RawWiseTable table,
            Expression condition,
            RecordWriter<?> writer,
            Properties config,
            BiConsumer<String, Map<String, Comparable<?>>> deleteCallback) {
        this.table = table;
        this.condition = condition;
        this.writer = writer;
        this.config = config;
        this.deleteCallback = deleteCallback;
    }

    @Override
    public Write.Result exec() {
        try {
            if (writer instanceof Initializable initable) {
                initable.init(config);
            }

            long deletedCount = doDelete();

            writer.flush();

            return Result.ok("Deleted " + deletedCount + " records", deletedCount);
        } catch (Exception e) {
            return Result.error("Delete failed: " + e.getMessage(), e);
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }
    }

    private long doDelete() throws Exception {
        PrimaryKey pk = table.meta().primaryKey();
        if (pk == null || pk.isEmpty()) {
            return 0L;
        }

        Set<String> pkColumns = new HashSet<>(pk.fields());

        if (ExpressionUtils.isPrimaryKeyEquals(condition, pkColumns)) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Map<String, Comparable<?>> pkValues =
                    (Map) ExpressionUtils.extractPrimaryKeyValues(condition, pkColumns);

            if (pkValues.size() != pkColumns.size()) {
                return 0L;
            }

            if (deleteCallback == null) {
                throw new IllegalStateException(
                        "deleteCallback is not set for Delete operation on table "
                                + table.uniqId());
            }

            deleteCallback.accept(table.uniqId(), pkValues);
            return 1L;
        }

        return 0L;
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    @Override
    public long writeSize() {
        return 0;
    }

    public Expression condition() {
        return condition;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements WriteBuilder<Delete, Builder> {
        private RawWiseTable table;
        private Expression condition;
        private RecordWriter<?> writer;
        private Properties config = new Properties();
        private BiConsumer<String, Map<String, Comparable<?>>> deleteCallback;

        @Override
        public Builder withTable(RawWiseTable table) {
            this.table = table;
            return this;
        }

        public Builder withCondition(Expression condition) {
            this.condition = condition;
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

        public Builder withConfig(String key, String value) {
            this.config.setProperty(key, value);
            return this;
        }

        public Builder withDeleteCallback(BiConsumer<String, Map<String, Comparable<?>>> callback) {
            this.deleteCallback = callback;
            return this;
        }

        @Override
        public RawWiseTable table() {
            return table;
        }

        @Override
        public Delete build() {
            if (condition == null) {
                throw new IllegalStateException("condition must be set");
            }
            if (writer == null) {
                throw new IllegalStateException("writer must be set");
            }
            return new Delete(table, condition, writer, config, deleteCallback);
        }
    }
}
