package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.storage.writer.Initializable;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionUtils;
import io.github.rawwise.model.record.PrimaryKey;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.WriteEntry;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Update implements Write {
    private static final OpType TYPE = OpType.UPDATE;

    private final RawWiseTable table;
    private final Expression condition;
    private final Map<String, Object> updates;
    private final RecordWriter<WriteEntry> writer;
    private final Properties config;

    @SuppressWarnings("unchecked")
    public Update(
            RawWiseTable table,
            Expression condition,
            Map<String, Object> updates,
            RecordWriter<?> writer,
            Properties config) {
        this.table = table;
        this.condition = condition;
        this.updates = updates;
        this.writer = (RecordWriter<WriteEntry>) writer;
        this.config = config;
    }

    @Override
    public Write.Result exec() {
        try {
            if (writer instanceof Initializable initable) {
                initable.init(config);
            }

            long updatedCount = doUpdate();

            writer.flush();

            return Write.Result.ok(updatedCount);
        } catch (Exception e) {
            return Write.Result.error("Update failed: " + e.getMessage(), e);
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }
    }

    private long doUpdate() throws Exception {
        PrimaryKey pk = table.meta().primaryKey();
        if (pk == null || pk.isEmpty()) {
            log.debug(
                    "Table {} has no primary key, update is a no-op at executor level",
                    table.name());
            return 0L;
        }

        Set<String> pkColumns = new HashSet<>(pk.fields());

        if (ExpressionUtils.isPrimaryKeyEquals(condition, pkColumns)) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Map<String, Comparable> pkValues =
                    (Map) ExpressionUtils.extractPrimaryKeyValues(condition, pkColumns);

            if (pkValues.size() != pkColumns.size()) {
                log.warn(
                        "PK equality detection succeeded but extracted only {}/{} PK values for table {}",
                        pkValues.size(),
                        pkColumns.size(),
                        table.name());
            }

            log.debug(
                    "PK point update on table {}: pkValues={}, updates={}",
                    table.name(),
                    pkValues,
                    updates);

            DefaultKeyCodec keyCodec = new DefaultKeyCodec();
            byte[] keyBytes = keyCodec.encodePrimaryKey(pkValues);
            byte[] valueBytes = NullableValueCodec.encodeUpdateValues(updates);
            writer.write(new WriteEntry(keyBytes, valueBytes, true));
            return 1L;
        }

        log.debug(
                "Range update on table {} deferred to LazyUpdate mechanism: condition={}",
                table.name(),
                condition);
        return 0L;
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    @Override
    public long writeSize() {
        return updates != null ? updates.size() : 0;
    }

    public Expression condition() {
        return condition;
    }

    public Map<String, Object> updates() {
        return updates;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements WriteBuilder<Update, Builder> {
        private RawWiseTable table;
        private Expression condition;
        private Map<String, Object> updates;
        private RecordWriter<?> writer;
        private Properties config = new Properties();

        @Override
        public Builder withTable(RawWiseTable table) {
            this.table = table;
            return this;
        }

        public Builder withCondition(Expression condition) {
            this.condition = condition;
            return this;
        }

        public Builder withUpdates(Map<String, Object> updates) {
            this.updates = updates;
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

        @Override
        public RawWiseTable table() {
            return table;
        }

        @Override
        public Update build() {
            if (condition == null) {
                throw new IllegalStateException("condition must be set");
            }
            if (updates == null || updates.isEmpty()) {
                throw new IllegalStateException("updates must be set");
            }
            if (writer == null) {
                throw new IllegalStateException("writer must be set");
            }
            return new Update(table, condition, updates, writer, config);
        }
    }
}
