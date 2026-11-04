package io.github.rawwise.engine.storage.manifest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.github.rawwise.backend.lazyupdate.UpdateLog;
import io.github.rawwise.engine.storage.*;
import java.io.IOException;
import java.io.Serial;
import java.util.Base64;

/**
 * Jackson serializers and deserializers for ManifestFile JSON format. Handles Base64
 * encoding/decoding of byte[] fields and polymorphic DataFile/LogFile serialization.
 */
public final class ManifestJsonCodecs {

    private ManifestJsonCodecs() {}

    /** Shared ObjectMapper with all manifest-related codecs registered. */
    private static final ObjectMapper SHARED_MAPPER = createObjectMapper();

    /**
     * Creates a new ObjectMapper with ALL codecs registered (ManifestFile, ManifestList, DataFile,
     * LogFile, KeyRange, ColumnStatistics, ColumnStat).
     *
     * @return a fully-configured ObjectMapper
     */
    public static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(
                com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        SimpleModule module = new SimpleModule("RawWiseManifest");
        // ManifestFile
        module.addSerializer(ManifestFile.class, new ManifestFileSerializer());
        module.addDeserializer(ManifestFile.class, new ManifestFileDeserializer());
        // ManifestList
        module.addSerializer(ManifestList.class, new ManifestListSerializer());
        module.addDeserializer(ManifestList.class, new ManifestListDeserializer());
        // DataFile (polymorphic)
        module.addSerializer(DataFile.class, new DataFileSerializer());
        module.addDeserializer(DataFile.class, new DataFileDeserializer());
        // LogFile (polymorphic)
        module.addSerializer(LogFile.class, new LogFileSerializer());
        module.addDeserializer(LogFile.class, new LogFileDeserializer());
        // KeyRange
        module.addSerializer(KeyRange.class, new KeyRangeSerializer());
        module.addDeserializer(KeyRange.class, new KeyRangeDeserializer());
        // ColumnStatistics
        module.addSerializer(ColumnStatistics.class, new ColumnStatisticsSerializer());
        module.addDeserializer(ColumnStatistics.class, new ColumnStatisticsDeserializer());
        // ColumnStat
        module.addSerializer(ColumnStatistics.ColumnStat.class, new ColumnStatSerializer());
        module.addDeserializer(ColumnStatistics.ColumnStat.class, new ColumnStatDeserializer());
        mapper.registerModule(module);
        return mapper;
    }

    /** Returns the shared ObjectMapper instance. */
    public static ObjectMapper sharedObjectMapper() {
        return SHARED_MAPPER;
    }

    // ── Base64 helpers ──────────────────────────────────────────────

    static String toBase64(byte[] data) {
        return data == null ? "" : Base64.getEncoder().encodeToString(data);
    }

    static byte[] fromBase64(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return new byte[0];
        }
        return Base64.getDecoder().decode(encoded);
    }

    // ── KeyRange ────────────────────────────────────────────────────

    static final class KeyRangeSerializer extends StdSerializer<KeyRange> {
        @Serial private static final long serialVersionUID = 1L;

        KeyRangeSerializer() {
            super(KeyRange.class);
        }

        @Override
        public void serialize(KeyRange value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("startKey", toBase64(value.startKey()));
            gen.writeStringField("endKey", toBase64(value.endKey()));
            gen.writeBooleanField("startInclusive", value.startInclusive());
            gen.writeBooleanField("endInclusive", value.endInclusive());
            gen.writeEndObject();
        }
    }

    static final class KeyRangeDeserializer extends StdDeserializer<KeyRange> {
        private static final long serialVersionUID = 1L;

        KeyRangeDeserializer() {
            super(KeyRange.class);
        }

        @Override
        public KeyRange deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            return new KeyRange(
                    fromBase64(node.path("startKey").asText("")),
                    node.path("startInclusive").asBoolean(true),
                    fromBase64(node.path("endKey").asText("")),
                    node.path("endInclusive").asBoolean(true));
        }
    }

    // ── ColumnStat ──────────────────────────────────────────────────

    static final class ColumnStatSerializer extends StdSerializer<ColumnStatistics.ColumnStat> {
        @Serial private static final long serialVersionUID = 1L;

        ColumnStatSerializer() {
            super(ColumnStatistics.ColumnStat.class);
        }

        @Override
        public void serialize(
                ColumnStatistics.ColumnStat value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeStartObject();
            gen.writeNumberField("nullCount", value.nullCount());
            gen.writeNumberField("valueCount", value.valueCount());
            gen.writeStringField("minBytes", toBase64(value.minBytes()));
            gen.writeStringField("maxBytes", toBase64(value.maxBytes()));
            gen.writeStringField("logicalType", value.logicalType());
            gen.writeNumberField("sizeBytes", value.sizeBytes());
            gen.writeEndObject();
        }
    }

    static final class ColumnStatDeserializer extends StdDeserializer<ColumnStatistics.ColumnStat> {
        @Serial private static final long serialVersionUID = 1L;

        ColumnStatDeserializer() {
            super(ColumnStatistics.ColumnStat.class);
        }

        @Override
        public ColumnStatistics.ColumnStat deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            return new ColumnStatistics.ColumnStat(
                    node.path("nullCount").asLong(0),
                    node.path("valueCount").asLong(0),
                    fromBase64(node.path("minBytes").asText("")),
                    fromBase64(node.path("maxBytes").asText("")),
                    node.path("logicalType").asText("UNKNOWN"),
                    node.path("sizeBytes").asLong(0));
        }
    }

    // ── ColumnStatistics ────────────────────────────────────────────

    static final class ColumnStatisticsSerializer extends StdSerializer<ColumnStatistics> {
        private static final long serialVersionUID = 1L;

        ColumnStatisticsSerializer() {
            super(ColumnStatistics.class);
        }

        @Override
        public void serialize(
                ColumnStatistics value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeStartObject();
            gen.writeObjectFieldStart("columns");
            for (var entry : value.columns().entrySet()) {
                gen.writeFieldName(entry.getKey());
                provider.defaultSerializeValue(entry.getValue(), gen);
            }
            gen.writeEndObject();
            gen.writeEndObject();
        }
    }

    static final class ColumnStatisticsDeserializer extends StdDeserializer<ColumnStatistics> {
        @Serial private static final long serialVersionUID = 1L;

        ColumnStatisticsDeserializer() {
            super(ColumnStatistics.class);
        }

        @Override
        public ColumnStatistics deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            JsonNode columnsNode = node.path("columns");
            java.util.Map<String, ColumnStatistics.ColumnStat> columns =
                    new java.util.LinkedHashMap<>();
            if (columnsNode.isObject()) {
                var fieldIter = columnsNode.fields();
                while (fieldIter.hasNext()) {
                    var entry = fieldIter.next();
                    JsonNode colNode = entry.getValue();
                    ColumnStatistics.ColumnStat stat =
                            new ColumnStatistics.ColumnStat(
                                    colNode.path("nullCount").asLong(0),
                                    colNode.path("valueCount").asLong(0),
                                    fromBase64(colNode.path("minBytes").asText("")),
                                    fromBase64(colNode.path("maxBytes").asText("")),
                                    colNode.path("logicalType").asText("UNKNOWN"),
                                    colNode.path("sizeBytes").asLong(0));
                    columns.put(entry.getKey(), stat);
                }
            }
            return new ColumnStatistics(columns);
        }
    }

    // ── DataFile (polymorphic: ParquetDataFile) ─────────────────────

    static final class DataFileSerializer extends StdSerializer<DataFile> {
        @Serial private static final long serialVersionUID = 1L;

        DataFileSerializer() {
            super(DataFile.class);
        }

        @Override
        public void serialize(DataFile value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("type", value.getClass().getSimpleName());
            gen.writeStringField("filePath", value.filePath());
            gen.writeStringField("format", value.format().name());
            gen.writeNumberField("fileSizeBytes", value.fileSizeBytes());
            gen.writeNumberField("createdAtMillis", value.createdAtMillis());
            gen.writeStringField("tableId", value.tableId());
            gen.writeNumberField("snapshotId", value.snapshotId());
            gen.writeObjectField("keyRange", value.keyRange());
            gen.writeNumberField("rowCount", value.rowCount());
            gen.writeNumberField("schemaVersion", value.schemaVersion());
            gen.writeNumberField("minSequenceNumber", value.minSequenceNumber());
            gen.writeNumberField("maxSequenceNumber", value.maxSequenceNumber());
            // partitionValues
            gen.writeObjectFieldStart("partitionValues");
            if (value.partitionValues() != null) {
                for (var entry : value.partitionValues().entrySet()) {
                    gen.writeStringField(entry.getKey(), entry.getValue());
                }
            }
            gen.writeEndObject();
            gen.writeObjectField("columnStatistics", value.columnStatistics());
            gen.writeNumberField("compactionLevel", value.compactionLevel());
            gen.writeEndObject();
        }
    }

    static final class DataFileDeserializer extends StdDeserializer<DataFile> {
        @Serial private static final long serialVersionUID = 1L;

        DataFileDeserializer() {
            super(DataFile.class);
        }

        @Override
        public DataFile deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            String type = node.path("type").asText("ParquetDataFile");
            RawWiseFile.FileFormat format =
                    RawWiseFile.FileFormat.valueOf(node.path("format").asText("PARQUET"));

            // Parse keyRange
            KeyRange keyRange = null;
            JsonNode krNode = node.path("keyRange");
            if (!krNode.isMissingNode() && !krNode.isNull()) {
                keyRange =
                        new KeyRange(
                                fromBase64(krNode.path("startKey").asText("")),
                                krNode.path("startInclusive").asBoolean(true),
                                fromBase64(krNode.path("endKey").asText("")),
                                krNode.path("endInclusive").asBoolean(true));
            }

            // Parse partitionValues
            java.util.Map<String, String> partitionValues = new java.util.LinkedHashMap<>();
            JsonNode pvNode = node.path("partitionValues");
            if (pvNode.isObject()) {
                var fieldIter = pvNode.fields();
                while (fieldIter.hasNext()) {
                    var entry = fieldIter.next();
                    partitionValues.put(entry.getKey(), entry.getValue().asText(""));
                }
            }

            // Parse columnStatistics
            ColumnStatistics columnStats = null;
            JsonNode csNode = node.path("columnStatistics");
            if (!csNode.isMissingNode() && !csNode.isNull()) {
                columnStats = ctxt.readValue(csNode.traverse(p.getCodec()), ColumnStatistics.class);
            }

            // Dispatch based on format field
            if (format == RawWiseFile.FileFormat.ORC) {
                return new OrcDataFile(
                        node.path("filePath").asText(""),
                        node.path("fileSizeBytes").asLong(0),
                        node.path("createdAtMillis").asLong(0),
                        node.path("tableId").asText(""),
                        node.path("snapshotId").asLong(0),
                        keyRange,
                        node.path("rowCount").asLong(0),
                        node.path("schemaVersion").asLong(0),
                        node.path("minSequenceNumber").asLong(0),
                        node.path("maxSequenceNumber").asLong(0),
                        partitionValues,
                        columnStats,
                        node.path("compactionLevel").asInt(DataFile.UNASSIGNED_LEVEL));
            }

            // Default: ParquetDataFile
            return new ParquetDataFile(
                    node.path("filePath").asText(""),
                    node.path("fileSizeBytes").asLong(0),
                    node.path("createdAtMillis").asLong(0),
                    node.path("tableId").asText(""),
                    node.path("snapshotId").asLong(0),
                    keyRange,
                    node.path("rowCount").asLong(0),
                    node.path("schemaVersion").asLong(0),
                    node.path("minSequenceNumber").asLong(0),
                    node.path("maxSequenceNumber").asLong(0),
                    partitionValues,
                    columnStats,
                    node.path("compactionLevel").asInt(DataFile.UNASSIGNED_LEVEL));
        }
    }

    // ── LogFile (polymorphic: UpdateLog / DeleteVector) ─────────────

    static final class LogFileSerializer extends StdSerializer<LogFile> {
        @Serial private static final long serialVersionUID = 1L;

        LogFileSerializer() {
            super(LogFile.class);
        }

        @Override
        public void serialize(LogFile value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("type", value.getClass().getSimpleName());
            gen.writeStringField("filePath", value.filePath());
            gen.writeStringField("format", value.format().name());
            gen.writeNumberField("fileSizeBytes", value.fileSizeBytes());
            gen.writeNumberField("createdAtMillis", value.createdAtMillis());
            gen.writeStringField("logType", value.logType().name());
            gen.writeStringField("tableId", value.tableId());
            gen.writeNumberField("snapshotId", value.snapshotId());
            gen.writeNumberField("fromSequence", value.fromSequence());
            gen.writeNumberField("toSequence", value.toSequence());
            if (value instanceof UpdateLog ul) {
                if (ul.serializedPredicate() != null) {
                    gen.writeStringField(
                            "serializedPredicate",
                            java.util.Base64.getEncoder().encodeToString(ul.serializedPredicate()));
                }
                if (ul.serializedUpdates() != null) {
                    gen.writeStringField(
                            "serializedUpdates",
                            java.util.Base64.getEncoder().encodeToString(ul.serializedUpdates()));
                }
                gen.writeNumberField("transactionId", ul.transactionId());
                if (ul.entryId() != null) {
                    gen.writeStringField("entryId", ul.entryId());
                }
            }
            gen.writeEndObject();
        }
    }

    static final class LogFileDeserializer extends StdDeserializer<LogFile> {
        @Serial private static final long serialVersionUID = 1L;

        LogFileDeserializer() {
            super(LogFile.class);
        }

        @Override
        public LogFile deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            String type = node.path("type").asText("");
            String logTypeName = node.path("logType").asText("UPDATE_LOG");
            LogFile.LogType logType = LogFile.LogType.valueOf(logTypeName);
            RawWiseFile.FileFormat format =
                    RawWiseFile.FileFormat.valueOf(node.path("format").asText("NWL"));

            String filePath = node.path("filePath").asText("");
            long fileSizeBytes = node.path("fileSizeBytes").asLong(0);
            long createdAtMillis = node.path("createdAtMillis").asLong(0);
            String tableId = node.path("tableId").asText("");
            long snapshotId = node.path("snapshotId").asLong(0);
            long fromSequence = node.path("fromSequence").asLong(0);
            long toSequence = node.path("toSequence").asLong(0);

            if (logType == LogFile.LogType.DELETE_VECTOR) {
                return new DeleteVector(
                        filePath,
                        format,
                        fileSizeBytes,
                        createdAtMillis,
                        tableId,
                        snapshotId,
                        fromSequence,
                        toSequence);
            } else {
                byte[] serializedPredicate = null;
                String predStr = node.path("serializedPredicate").asText(null);
                if (predStr != null && !predStr.isEmpty()) {
                    serializedPredicate = java.util.Base64.getDecoder().decode(predStr);
                }
                byte[] serializedUpdates = null;
                String updStr = node.path("serializedUpdates").asText(null);
                if (updStr != null && !updStr.isEmpty()) {
                    serializedUpdates = java.util.Base64.getDecoder().decode(updStr);
                }
                long transactionId = node.path("transactionId").asLong(0L);
                String entryId = node.path("entryId").asText(null);
                return new UpdateLog(
                        filePath,
                        format,
                        fileSizeBytes,
                        createdAtMillis,
                        tableId,
                        snapshotId,
                        fromSequence,
                        toSequence,
                        serializedPredicate,
                        serializedUpdates,
                        transactionId,
                        entryId);
            }
        }
    }

    // ── ManifestFile ────────────────────────────────────────────────

    static final class ManifestFileSerializer extends StdSerializer<ManifestFile> {
        @Serial private static final long serialVersionUID = 1L;

        ManifestFileSerializer() {
            super(ManifestFile.class);
        }

        @Override
        public void serialize(ManifestFile value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("manifestId", value.manifestId());
            gen.writeStringField("format", value.format().name());
            gen.writeNumberField("fileSizeBytes", value.fileSizeBytes());
            gen.writeNumberField("createdAtMillis", value.createdAtMillis());
            gen.writeNumberField("snapshotId", value.snapshotId());

            gen.writeArrayFieldStart("dataFiles");
            for (DataFile df : value.dataFiles()) {
                provider.defaultSerializeValue(df, gen);
            }
            gen.writeEndArray();

            gen.writeArrayFieldStart("logFiles");
            for (LogFile lf : value.logFiles()) {
                provider.defaultSerializeValue(lf, gen);
            }
            gen.writeEndArray();

            gen.writeNumberField("minSequenceNumber", value.minSequenceNumber());
            gen.writeNumberField("maxSequenceNumber", value.maxSequenceNumber());
            if (value.bloomFilter() != null) {
                gen.writeStringField("bloomFilter", toBase64(value.bloomFilter()));
            }
            gen.writeEndObject();
        }
    }

    static final class ManifestFileDeserializer extends StdDeserializer<ManifestFile> {
        @Serial private static final long serialVersionUID = 1L;

        ManifestFileDeserializer() {
            super(ManifestFile.class);
        }

        @Override
        public ManifestFile deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException {
            JsonNode node = p.getCodec().readTree(p);

            java.util.List<DataFile> dataFiles = new java.util.ArrayList<>();
            JsonNode dfArray = node.path("dataFiles");
            if (dfArray.isArray()) {
                for (JsonNode dfNode : dfArray) {
                    dataFiles.add(ctxt.readValue(dfNode.traverse(p.getCodec()), DataFile.class));
                }
            }

            java.util.List<LogFile> logFiles = new java.util.ArrayList<>();
            JsonNode lfArray = node.path("logFiles");
            if (lfArray.isArray()) {
                for (JsonNode lfNode : lfArray) {
                    logFiles.add(ctxt.readValue(lfNode.traverse(p.getCodec()), LogFile.class));
                }
            }

            return new ManifestFile(
                    node.has("manifestId")
                            ? node.path("manifestId").asText("")
                            : node.path("filePath").asText(""),
                    RawWiseFile.FileFormat.valueOf(node.path("format").asText("PARQUET")),
                    node.path("fileSizeBytes").asLong(0),
                    node.path("createdAtMillis").asLong(0),
                    node.path("snapshotId").asLong(0),
                    dataFiles,
                    logFiles,
                    node.path("minSequenceNumber").asLong(0),
                    node.path("maxSequenceNumber").asLong(0),
                    fromBase64(node.path("bloomFilter").asText(null)));
        }
    }

    // ── ManifestList ────────────────────────────────────────────────

    static final class ManifestListSerializer extends StdSerializer<ManifestList> {
        @Serial private static final long serialVersionUID = 1L;

        ManifestListSerializer() {
            super(ManifestList.class);
        }

        @Override
        public void serialize(ManifestList value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("filePath", value.filePath());
            gen.writeNumberField("snapshotId", value.snapshotId());

            gen.writeArrayFieldStart("manifests");
            for (ManifestFile mf : value.manifests()) {
                provider.defaultSerializeValue(mf, gen);
            }
            gen.writeEndArray();

            gen.writeNumberField("createdAtMillis", value.createdAtMillis());
            gen.writeEndObject();
        }
    }

    static final class ManifestListDeserializer extends StdDeserializer<ManifestList> {
        @Serial private static final long serialVersionUID = 1L;

        ManifestListDeserializer() {
            super(ManifestList.class);
        }

        @Override
        public ManifestList deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException {
            JsonNode node = p.getCodec().readTree(p);

            java.util.List<ManifestFile> manifests = new java.util.ArrayList<>();
            JsonNode mfArray = node.path("manifests");
            if (mfArray.isArray()) {
                for (JsonNode mfNode : mfArray) {
                    manifests.add(
                            ctxt.readValue(mfNode.traverse(p.getCodec()), ManifestFile.class));
                }
            }

            return new ManifestList(
                    node.path("filePath").asText(""),
                    node.path("snapshotId").asLong(0),
                    manifests,
                    node.path("createdAtMillis").asLong(0));
        }
    }
}
