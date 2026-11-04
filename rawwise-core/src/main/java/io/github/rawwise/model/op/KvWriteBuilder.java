package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.WriteEntry;
import java.util.List;
import java.util.Map;

public class KvWriteBuilder implements WriteBuilder<KvWrite, KvWriteBuilder> {
    private RawWiseTable table;
    private List<Map<String, Object>> data;
    private byte[] key;
    private byte[] value;
    private boolean valuePreEncoded;
    private boolean tombstone;
    private Integer partitionHint;
    private RecordWriter<WriteEntry> writer;

    public static KvWriteBuilder builder() {
        return new KvWriteBuilder();
    }

    public KvWriteBuilder table(RawWiseTable table) {
        this.table = table;
        return this;
    }

    @Override
    public KvWriteBuilder withTable(RawWiseTable table) {
        this.table = table;
        return this;
    }

    public KvWriteBuilder withData(List<Map<String, Object>> data) {
        this.data = data;
        return this;
    }

    public KvWriteBuilder key(byte[] key) {
        this.key = key != null ? key.clone() : null;
        return this;
    }

    public KvWriteBuilder value(byte[] value) {
        if (this.tombstone) {
            throw new IllegalStateException("Cannot set value after tombstone() has been called");
        }
        this.value = value != null ? value.clone() : null;
        return this;
    }

    public KvWriteBuilder valuePreEncoded(boolean valuePreEncoded) {
        this.valuePreEncoded = valuePreEncoded;
        return this;
    }

    public boolean valuePreEncoded() {
        return valuePreEncoded;
    }

    /**
     * Marks this write as a tombstone (logical delete) with the given commit timestamp.
     *
     * <p>commit Ts must come from {@link io.github.rawwise.engine.txn.state.Txn Context
     * Holder#current()} or a {@link io.github.rawwise.engine.txn.clock.Logical Clock}, never from
     * wall-clock. This ensures tombstone commit Ts is in the same domain as OCC validation.
     */
    public KvWriteBuilder tombstone(long commitTs) {
        if (this.value != null) {
            throw new IllegalStateException("Cannot call tombstone() after value() has been set");
        }
        this.tombstone = true;
        this.value = NullableValueCodec.encodeTombstone(commitTs);
        this.valuePreEncoded = true;
        return this;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    public KvWriteBuilder partitionHint(Integer partitionHint) {
        this.partitionHint = partitionHint;
        return this;
    }

    public byte[] key() {
        return key != null ? key.clone() : null;
    }

    public byte[] value() {
        return value != null ? value.clone() : null;
    }

    public List<Map<String, Object>> data() {
        return data;
    }

    @Override
    @SuppressWarnings("unchecked")
    public KvWriteBuilder withWriter(RecordWriter<?> writer) {
        this.writer = (RecordWriter<WriteEntry>) writer;
        return this;
    }

    @Override
    public KvWrite build() {
        if (key != null && value != null) {
            return new KvWrite(table, key, value, valuePreEncoded, partitionHint, writer);
        }
        if (partitionHint != null) {
            return new KvWrite(table, data, partitionHint, writer);
        }
        return new KvWrite(table, data, writer);
    }

    @Override
    public RawWiseTable table() {
        return table;
    }
}
