package io.github.rawwise.engine.storage.writer;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public final class StorageSchemas {

    public static final Schema INGESTION_KV_SCHEMA =
            SchemaBuilder.record("IngestionKV")
                    .fields()
                    .name("key")
                    .type()
                    .bytesType()
                    .noDefault()
                    .name("value")
                    .type()
                    .bytesType()
                    .noDefault()
                    .name("sequence_number")
                    .type()
                    .longType()
                    .longDefault(0L)
                    .name("commit_timestamp")
                    .type()
                    .longType()
                    .longDefault(0L)
                    .name("is_tombstone")
                    .type()
                    .booleanType()
                    .booleanDefault(false)
                    .endRecord();

    private StorageSchemas() {}
}
