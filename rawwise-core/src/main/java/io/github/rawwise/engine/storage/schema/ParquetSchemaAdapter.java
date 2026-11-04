package io.github.rawwise.engine.storage.schema;

import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableMeta;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.SchemaBuilder;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

@Slf4j
public class ParquetSchemaAdapter {

    private static final String META_BODY_CODEC = "rawwise.bodyCodec";
    private static final String META_SCHEMA_VERSION = "rawwise.schemaVersion";
    private static final String META_COLUMN_GROUPS = "rawwise.columnGroups";
    private static final String PROP_BODY_CODEC = "body.codec";
    private static final String BODY_COLUMN_NAME = "body";

    private static final org.apache.avro.Schema KEY_VALUE_SCHEMA =
            new org.apache.avro.Schema.Parser()
                    .parse(
                            """
            {
              "type": "record",
              "name": "RawWiseKeyValueRecord",
              "fields": [
                {"name": "key", "type": "bytes"},
                {"name": "value", "type": "bytes"}
              ]
            }
            """);

    public record ReadSchemaInfo(
            org.apache.avro.Schema readerSchema,
            boolean isBodyCodec,
            String bodyCodecName,
            long schemaVersion,
            List<String> pkFieldNames) {}

    public record WriteSchemaInfo(
            org.apache.avro.Schema writerSchema,
            boolean isBodyCodec,
            String bodyCodecName,
            List<ColumnGroup> columnGroups) {}

    public ReadSchemaInfo readSchema(org.apache.hadoop.fs.Path parquetPath) throws IOException {
        ParquetMetadata metadata =
                ParquetFileReader.readFooter(
                        new org.apache.hadoop.conf.Configuration(), parquetPath);
        Map<String, String> kvMeta = metadata.getFileMetaData().getKeyValueMetaData();

        String bodyCodecName = kvMeta.get(META_BODY_CODEC);
        if (bodyCodecName == null || bodyCodecName.isEmpty()) {
            return new ReadSchemaInfo(KEY_VALUE_SCHEMA, false, null, 0, List.of());
        }

        String schemaVerStr = kvMeta.get(META_SCHEMA_VERSION);
        long schemaVersion = schemaVerStr != null ? Long.parseLong(schemaVerStr) : 0;

        org.apache.parquet.schema.MessageType parquetSchema =
                metadata.getFileMetaData().getSchema();
        List<String> pkNames =
                parquetSchema.getFields().stream()
                        .filter(f -> !BODY_COLUMN_NAME.equals(f.getName()))
                        .map(org.apache.parquet.schema.Type::getName)
                        .toList();

        return new ReadSchemaInfo(null, true, bodyCodecName, schemaVersion, pkNames);
    }

    public WriteSchemaInfo writeSchema(TableMeta meta) {
        if (meta == null) {
            return new WriteSchemaInfo(KEY_VALUE_SCHEMA, false, null, null);
        }

        String bodyCodecName =
                meta.properties() != null ? meta.properties().get(PROP_BODY_CODEC) : null;
        if (bodyCodecName == null || bodyCodecName.isEmpty()) {
            bodyCodecName = "fixed-offset";
        }

        Schema schema = meta.schema();
        if (schema == null) {
            return new WriteSchemaInfo(KEY_VALUE_SCHEMA, false, null, null);
        }

        List<String> pkNames = meta.primaryKey() != null ? meta.primaryKey().fields() : List.of();

        List<ColumnGroup> columnGroups = schema.columnGroups();

        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> assembler =
                SchemaBuilder.record("RawWiseBodyCodecRecord").fields();
        for (String pkName : pkNames) {
            assembler = assembler.name(pkName).type().bytesType().noDefault();
        }
        assembler = assembler.name("body").type().bytesType().noDefault();
        org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) assembler.endRecord();

        return new WriteSchemaInfo(avroSchema, true, bodyCodecName, columnGroups);
    }

    public static org.apache.avro.Schema keyValueSchema() {
        return KEY_VALUE_SCHEMA;
    }
}
