package io.github.rawwise.engine.storage.ingestion;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class IngestionKvWriteSupport extends WriteSupport<IngestionKvRecord> {
    private RecordConsumer recordConsumer;
    private final MessageType schema;
    private final int keyFieldIndex;
    private final int valueFieldIndex;
    private final int sequenceNumberFieldIndex;
    private final int commitTimestampFieldIndex;
    private final int tombstoneFieldIndex;

    public IngestionKvWriteSupport(MessageType schema) {
        this.schema = schema;
        this.keyFieldIndex = requireFieldIndex(schema, "key");
        this.valueFieldIndex = requireFieldIndex(schema, "value");
        this.sequenceNumberFieldIndex = requireFieldIndex(schema, "sequence_number");
        this.commitTimestampFieldIndex = requireFieldIndex(schema, "commit_timestamp");
        this.tombstoneFieldIndex = requireFieldIndex(schema, "is_tombstone");
    }

    private static int requireFieldIndex(MessageType schema, String fieldName) {
        int index = schema.getFieldIndex(fieldName);
        if (index < 0) {
            throw new IllegalArgumentException(
                    "Required field '" + fieldName + "' not found in Parquet schema: " + schema);
        }
        return index;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        Map<String, String> extraMetadata = new HashMap<>();
        String schemaVersion = configuration.get("rawwise.schema.version");
        if (schemaVersion != null) {
            extraMetadata.put("schemaVersion", schemaVersion);
        }
        return new WriteContext(schema, extraMetadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(IngestionKvRecord record) {
        if (recordConsumer == null) {
            throw new IllegalStateException("prepareForWrite() must be called before write()");
        }
        recordConsumer.startMessage();
        recordConsumer.startField("key", keyFieldIndex);
        recordConsumer.addBinary(Binary.fromByteArray(record.key()));
        recordConsumer.endField("key", keyFieldIndex);
        recordConsumer.startField("value", valueFieldIndex);
        recordConsumer.addBinary(Binary.fromByteArray(record.value()));
        recordConsumer.endField("value", valueFieldIndex);
        recordConsumer.startField("sequence_number", sequenceNumberFieldIndex);
        recordConsumer.addLong(record.sequenceNumber());
        recordConsumer.endField("sequence_number", sequenceNumberFieldIndex);
        recordConsumer.startField("commit_timestamp", commitTimestampFieldIndex);
        recordConsumer.addLong(record.commitTimestamp());
        recordConsumer.endField("commit_timestamp", commitTimestampFieldIndex);
        recordConsumer.startField("is_tombstone", tombstoneFieldIndex);
        recordConsumer.addBoolean(record.tombstone());
        recordConsumer.endField("is_tombstone", tombstoneFieldIndex);
        recordConsumer.endMessage();
    }
}
