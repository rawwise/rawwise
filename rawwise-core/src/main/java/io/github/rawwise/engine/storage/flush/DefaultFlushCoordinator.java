package io.github.rawwise.engine.storage.flush;

import io.github.rawwise.backend.lazyupdate.CompactionTrigger;
import io.github.rawwise.backend.lazyupdate.LazyUpdateCompactor;
import io.github.rawwise.engine.storage.ColumnStatistics;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.ParquetDataFile;
import io.github.rawwise.engine.storage.ParquetFormatConstants;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.index.SparseIndexBuilder;
import io.github.rawwise.engine.storage.bloom.BloomFilterBuilder;
import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.codec.BodyCodecRegistry;
import io.github.rawwise.engine.storage.codec.RecordDecoder;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.vector.VectorFlushListener;
import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.ColumnGroupCodec;
import io.github.rawwise.model.record.ColumnGroupCodecFactory;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.TableMeta;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

@Slf4j
public class DefaultFlushCoordinator implements FlushCoordinator {
    private static final Configuration SHARED_CONF = new Configuration();

    private static final org.apache.avro.Schema KEY_VALUE_SCHEMA =
            new org.apache.avro.Schema.Parser()
                    .parse(
                            """
            {
              "type": "record",
              "name": "RawWiseKeyValueRecord",
              "fields": [
                {"name": "key", "type": "bytes"},
                {"name": "value", "type": "bytes"},
                {"name": "__commit_ts__", "type": "long"}
              ]
            }
            """);

    private static final String PROP_BODY_CODEC = "body.codec";

    private final Function<String, TableStore> tableStoreProvider;

    private volatile LazyUpdateCompactor lazyUpdateCompactor;

    private volatile CompactionTrigger compactionTrigger;

    private volatile SnapshotManager snapshotManager;

    private final ThreadLocal<byte[]> currentSparseIndex = new ThreadLocal<>();

    public DefaultFlushCoordinator(Function<String, TableStore> tableStoreProvider) {
        this.tableStoreProvider = tableStoreProvider;
    }

    public void setCompactionComponents(LazyUpdateCompactor compactor, CompactionTrigger trigger) {
        this.lazyUpdateCompactor = compactor;
        this.compactionTrigger = trigger;
    }

    @Override
    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    @Override
    public void addFlushListener(VectorFlushListener listener) {}

    @Override
    public FlushPlan plan(String tableId, LmdbInstance instance, String remoteBasePath) {
        Storage.StoragePath targetPath =
                resolveStoragePath(remoteBasePath, instance.instanceId() + ".parquet");
        return new FlushPlan(
                "flush-" + instance.instanceId(),
                tableId,
                List.of(instance.instanceId()),
                new DataBlock.Location("remote", targetPath, 0L, instance.getTotalUsageSize()),
                DataBlock.Format.PARQUET,
                new KeyRange(instance.minKey(), true, instance.maxKey(), true),
                instance.getTotalRecords(),
                resolveSchemaVersion(tableId),
                System.currentTimeMillis());
    }

    @Override
    public void beforeFlush(String tableId, LmdbInstance instance) {
        instance.markAsReadOnly();
        tableStoreProvider
                .apply(tableId)
                .markBlockVisibility(instance.instanceId(), DataBlock.Visibility.FLUSHING);
    }

    @Override
    public void afterFlush(String tableId, LmdbInstance instance, FlushPlan plan) {
        Storage.StoragePath storagePath = plan.targetLocation().storagePath();
        String targetUri = toUri(storagePath);

        byte[] sparseIndex = writeParquet(instance, storagePath, targetUri, tableId);
        currentSparseIndex.set(sparseIndex);
        validateAndGetSize(storagePath, targetUri);
    }

    @Override
    public void commitFlush(String tableId, LmdbInstance instance, FlushPlan plan) {
        TableStore tableStore = tableStoreProvider.apply(tableId);
        Storage.StoragePath storagePath = plan.targetLocation().storagePath();
        String targetUri = toUri(storagePath);

        byte[] bloomFilter = buildBloomFilter(instance);

        long fileSize = validateAndGetSize(storagePath, targetUri);

        long minSeq = instance.minSequenceNumber();
        long maxSeq = instance.maxSequenceNumber();

        SnapshotManager sm = this.snapshotManager;
        if (sm == null) {
            deleteParquetFile(storagePath, targetUri);
            throw new IllegalStateException(
                    "SnapshotManager not configured for table "
                            + tableId
                            + ". Flush data at "
                            + targetUri
                            + " will not be discoverable.");
        }

        ParquetDataFile dataFile =
                new ParquetDataFile(
                        targetUri,
                        fileSize,
                        System.currentTimeMillis(),
                        tableId,
                        0L,
                        plan.targetKeyRange(),
                        plan.estimatedRows(),
                        plan.schemaVersion(),
                        minSeq,
                        maxSeq,
                        Map.of(),
                        ColumnStatistics.EMPTY,
                        0);

        try {
            sm.commitSnapshot(tableId, List.of(dataFile), List.of(), minSeq, maxSeq, bloomFilter);
        } catch (Exception e) {
            deleteParquetFile(storagePath, targetUri);
            throw new RuntimeException(
                    "Failed to commit snapshot for table "
                            + tableId
                            + ". Orphan parquet at "
                            + targetUri
                            + " has been cleaned up.",
                    e);
        }

        byte[] sparseIndex = currentSparseIndex.get();
        currentSparseIndex.remove();

        ColdDataBlock coldBlock =
                new ColdDataBlock(
                        plan.planId(),
                        plan.tableId(),
                        DataBlock.Kind.IMMUTABLE_COLD,
                        plan.targetFormat(),
                        plan.targetFormat(),
                        plan.targetLocation(),
                        plan.targetKeyRange(),
                        plan.estimatedRows(),
                        fileSize,
                        plan.schemaVersion(),
                        -1L,
                        Map.of(),
                        new ColdDataBlock.Statistics(
                                plan.estimatedRows(), fileSize, minSeq, maxSeq),
                        DataBlock.Visibility.ARCHIVED,
                        minSeq,
                        maxSeq,
                        0,
                        bloomFilter,
                        sparseIndex);

        tableStore.registerColdBlock(coldBlock);
        instance.markForDeletion();
        tableStore.markBlockVisibility(instance.instanceId(), DataBlock.Visibility.DELETED);

        postFlushCompactionCheck(tableId);
    }

    @Override
    public void onFlushFailure(String tableId, LmdbInstance instance, Throwable cause) {
        instance.unmarkForDeletion();
        tableStoreProvider
                .apply(tableId)
                .markBlockVisibility(instance.instanceId(), DataBlock.Visibility.ACTIVE);
    }

    private byte[] writeParquet(
            LmdbInstance instance,
            Storage.StoragePath storagePath,
            String targetUri,
            String tableId) {
        try {
            if ("file".equals(storagePath.scheme())) {
                Path outputPath = Paths.get(URI.create(targetUri));
                if (outputPath.getParent() != null) {
                    Files.createDirectories(outputPath.getParent());
                }
            }

            FlushSchema flushSchema = resolveFlushSchemaForTable(tableId);
            org.apache.avro.Schema avroSchema =
                    (flushSchema != null) ? flushSchema.avroSchema() : KEY_VALUE_SCHEMA;

            SparseIndexBuilder indexBuilder = new SparseIndexBuilder();
            try (var iterator = instance.closeableIterator()) {
                while (iterator.hasNext()) {
                    Pair<byte[], byte[]> pair = iterator.next();
                    indexBuilder.accept(pair.getLeft());
                }
            }
            byte[] sparseIndex = indexBuilder.build();

            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(targetUri);

            Configuration conf = new Configuration(SHARED_CONF);
            Map<String, String> extraMeta = new LinkedHashMap<>();
            if (flushSchema != null) {
                extraMeta.put(ParquetFormatConstants.META_FORMAT_VERSION, "2");
                extraMeta.put(
                        ParquetFormatConstants.META_SCHEMA_VERSION,
                        String.valueOf(flushSchema.schemaVersion()));
                extraMeta.put(ParquetFormatConstants.META_BODY_CODEC, flushSchema.bodyCodecName());
                extraMeta.put(
                        ParquetFormatConstants.META_NUMERIC_TABLE_ID,
                        String.valueOf(flushSchema.numericTableId()));

                List<String> pkNames = flushSchema.pkFieldNames();
                if (pkNames != null && !pkNames.isEmpty()) {
                    extraMeta.put(
                            ParquetFormatConstants.META_PK_FIELD_NAMES, String.join(",", pkNames));
                }

                List<ColumnGroup> cgs = flushSchema.columnGroups();
                if (cgs != null && !cgs.isEmpty()) {
                    extraMeta.put(
                            ParquetFormatConstants.META_COLUMN_GROUPS, serializeColumnGroups(cgs));
                }
            }
            extraMeta.put(
                    ParquetFormatConstants.META_SPARSE_INDEX,
                    Base64.getEncoder().encodeToString(sparseIndex));

            AvroParquetWriter.Builder<GenericRecord> parquetBuilder =
                    AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                            .withSchema(avroSchema)
                            .withConf(conf)
                            .withExtraMetaData(extraMeta)
                            .withRowGroupSize(10L * 1024 * 1024)
                            .withWriterVersion(
                                    org.apache.parquet.column.ParquetProperties.WriterVersion
                                            .PARQUET_2_0);

            try {
                org.apache.hadoop.fs.FileSystem fs = hadoopPath.getFileSystem(conf);
                if (fs.exists(hadoopPath)) {
                    log.warn(
                            "Flush {}: parquet file already exists, deleting: {}",
                            instance.instanceId(),
                            hadoopPath);
                    fs.delete(hadoopPath, false);
                }
            } catch (Exception deleteErr) {
                log.warn(
                        "Flush {}: failed to delete existing parquet file: {}",
                        instance.instanceId(),
                        deleteErr.getMessage());
            }

            try (ParquetWriter<GenericRecord> writer = parquetBuilder.build()) {
                long skipped = 0;
                try (var iterator = instance.closeableIterator()) {
                    while (iterator.hasNext()) {
                        Pair<byte[], byte[]> pair = iterator.next();
                        GenericRecord record;
                        try {
                            record = buildParquetRecord(avroSchema, flushSchema, pair, tableId);
                        } catch (Exception decodeErr) {
                            skipped++;
                            record = buildRawFallbackRecord(avroSchema, pair, flushSchema);
                        }
                        writer.write(record);
                    }
                }
                if (skipped > 0) {
                    log.warn(
                            "Flush {}: {} records degraded to raw fallback (decode failed)",
                            instance.instanceId(),
                            skipped);
                }
            }

            return sparseIndex;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to write parquet for instance " + instance.instanceId(), e);
        }
    }

    private byte[] buildBloomFilter(LmdbInstance instance) {
        try {
            List<byte[]> keys = new ArrayList<>();
            try (var iterator = instance.closeableIterator()) {
                while (iterator.hasNext()) {
                    Pair<byte[], byte[]> pair = iterator.next();
                    keys.add(pair.getLeft());
                }
            }
            return BloomFilterBuilder.build(keys, keys.size());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to build bloom filter for instance " + instance.instanceId(), e);
        }
    }

    private GenericRecord buildParquetRecord(
            org.apache.avro.Schema avroSchema,
            FlushSchema flushSchema,
            Pair<byte[], byte[]> pair,
            String tableId) {
        GenericRecord record = new GenericData.Record(avroSchema);
        long commitTs = extractCommitTs(pair.getRight(), tableId);
        record.put(ParquetFormatConstants.COMMIT_TIMESTAMP_COLUMN_NAME, commitTs);
        if (flushSchema == null) {
            record.put("key", ByteBuffer.wrap(pair.getLeft()));
            record.put("value", ByteBuffer.wrap(pair.getRight()));
            return record;
        }

        byte[] keyBytes = pair.getLeft();
        byte[] valueBytes = pair.getRight();

        record.put(ParquetFormatConstants.PK_COLUMN_NAME, ByteBuffer.wrap(keyBytes));

        Map<String, Object> allFields =
                flushSchema.bodyCodec().decode(flushSchema.schema(), valueBytes);

        List<ColumnGroup> cgs = flushSchema.columnGroups();
        if (cgs != null) {
            for (ColumnGroup cg : cgs) {
                Map<String, Object> cgValues = new LinkedHashMap<>();
                for (String colName : cg.columnNames()) {
                    cgValues.put(colName, allFields.get(colName));
                }
                ColumnGroupCodec cgCodec = ColumnGroupCodecFactory.getCodec(cg);
                byte[] cgBytes = cgCodec.encode(cg, cgValues);
                record.put(cg.name(), ByteBuffer.wrap(cgBytes));
            }
        }

        for (String fieldName : flushSchema.nonCgFieldNames()) {
            Object value = allFields.get(fieldName);
            if (value != null) {
                byte[] encoded = encodeFieldValue(value);
                record.put(fieldName, ByteBuffer.wrap(encoded));
            }
        }

        return record;
    }

    private GenericRecord buildRawFallbackRecord(
            org.apache.avro.Schema avroSchema, Pair<byte[], byte[]> pair, FlushSchema flushSchema) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put(ParquetFormatConstants.COMMIT_TIMESTAMP_COLUMN_NAME, Long.MIN_VALUE);
        record.put(ParquetFormatConstants.PK_COLUMN_NAME, ByteBuffer.wrap(pair.getLeft()));
        if (flushSchema != null && flushSchema.nonCgFieldNames() != null) {
            for (String fieldName : flushSchema.nonCgFieldNames()) {
                record.put(fieldName, null);
            }
        }
        return record;
    }

    /**
     * Uses the TableStore's RecordCodec to peek at the Record header. When the record cannot be
     * decoded (legacy non-RecordCodec format), returns {@link Long#MIN_VALUE} as the "unknown
     * commitTs" sentinel.
     */
    private long extractCommitTs(byte[] valueBytes, String tableId) {
        if (valueBytes == null) return Long.MIN_VALUE;
        try {
            TableStore ts = tableStoreProvider.apply(tableId);
            if (ts == null) return Long.MIN_VALUE;
            RecordCodec recordCodec = ts.getRecordCodec();
            if (recordCodec == null) return Long.MIN_VALUE;
            RecordCodec.SchemaResolver schemaResolver = ts.getSchemaResolver();
            RecordDecoder.DecodeResult dr =
                    RecordDecoder.decodeNullableWrapped(valueBytes, recordCodec, schemaResolver);
            if (dr instanceof RecordDecoder.DecodeResult.Found f) {
                long tsVal = f.record().header().commitTimestamp();
                if (tsVal <= 0 && tsVal != Long.MIN_VALUE) {
                    return Long.MIN_VALUE;
                }
                return tsVal;
            }
            return Long.MIN_VALUE;
        } catch (Exception e) {
            log.debug(
                    "Failed to extract commitTs from value bytes for table {}: {}",
                    tableId,
                    e.getMessage());
            return Long.MIN_VALUE;
        }
    }

    private static byte[] encodeFieldValue(Object value) {
        if (value == null) return new byte[0];
        if (value instanceof byte[] bytes) return bytes;
        if (value instanceof String s) return s.getBytes(StandardCharsets.UTF_8);
        if (value instanceof Long l) {
            ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            buf.putLong(l);
            return buf.array();
        }
        if (value instanceof Integer i) {
            ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
            buf.putInt(i);
            return buf.array();
        }
        if (value instanceof Double d) {
            ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            buf.putDouble(d);
            return buf.array();
        }
        if (value instanceof Float f) {
            ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
            buf.putFloat(f);
            return buf.array();
        }
        if (value instanceof Boolean b) return new byte[] {(byte) (b ? 1 : 0)};
        if (value instanceof Byte b) return new byte[] {b};
        throw new IllegalArgumentException(
                "Unsupported field value type for Parquet encoding: " + value.getClass().getName());
    }

    private FlushSchema resolveFlushSchemaForTable(String tableId) {
        SnapshotManager sm = this.snapshotManager;
        if (sm == null) return null;
        try {
            RawWiseTable table = sm.getCatalog().loadTable(TableId.parseLenient(tableId));
            if (table == null || table.meta() == null) return null;
            TableMeta meta = table.meta();

            String bodyCodecName = meta.properties().get(PROP_BODY_CODEC);
            if (bodyCodecName == null || bodyCodecName.isEmpty()) {
                bodyCodecName = "fixed-offset";
            }
            if (!BodyCodecRegistry.isRegistered(bodyCodecName)) {
                return null;
            }

            Schema schema = meta.schema();
            if (schema == null) return null;

            long numericTableId = meta.tableId();

            List<String> pkNames =
                    meta.primaryKey() != null ? meta.primaryKey().fields() : List.of();

            List<ColumnGroup> columnGroups = schema.columnGroups();
            BodyCodec bodyCodec = BodyCodecRegistry.create(bodyCodecName, schema);
            List<String> nonCgFieldNames =
                    ParquetFormatConstants.computeNonCgFieldNames(schema, columnGroups);
            org.apache.avro.Schema avroSchema =
                    buildMultiColumnAvroSchema(pkNames, columnGroups, nonCgFieldNames);

            return new FlushSchema(
                    avroSchema,
                    pkNames,
                    "body",
                    schema.version(),
                    bodyCodecName,
                    columnGroups,
                    tableId,
                    numericTableId,
                    bodyCodec,
                    schema);
        } catch (Exception e) {
            log.warn("Failed to resolve flush schema for table {}: {}", tableId, e.getMessage());
            return null;
        }
    }

    private org.apache.avro.Schema buildMultiColumnAvroSchema(
            List<String> pkNames, List<ColumnGroup> columnGroups, List<String> nonCgFieldNames) {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> assembler =
                SchemaBuilder.record("RawWiseSchemaAwareRecord").fields();

        assembler =
                assembler
                        .name(ParquetFormatConstants.PK_COLUMN_NAME)
                        .type()
                        .bytesType()
                        .noDefault();

        if (columnGroups != null) {
            for (ColumnGroup cg : columnGroups) {
                assembler = assembler.name(cg.name()).type().bytesType().noDefault();
            }
        }

        for (String fieldName : nonCgFieldNames) {
            assembler =
                    assembler
                            .name(fieldName)
                            .type()
                            .unionOf()
                            .nullType()
                            .and()
                            .bytesType()
                            .endUnion()
                            .nullDefault();
        }

        assembler =
                assembler
                        .name(ParquetFormatConstants.COMMIT_TIMESTAMP_COLUMN_NAME)
                        .type()
                        .longType()
                        .noDefault();

        return assembler.endRecord();
    }

    private static String serializeColumnGroups(List<ColumnGroup> columnGroups) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnGroups.size(); i++) {
            if (i > 0) sb.append(';');
            ColumnGroup cg = columnGroups.get(i);
            sb.append(cg.name())
                    .append(':')
                    .append(cg.encoding().name())
                    .append(':')
                    .append(cg.schemaVersion());
            for (int j = 0; j < cg.columnNames().size(); j++) {
                String colName = cg.columnNames().get(j);
                int fieldId = cg.columnFieldId(j);
                sb.append(',').append(fieldId).append(':').append(colName);
                if (!cg.columnTypes().isEmpty()) {
                    sb.append(':').append(cg.columnTypes().get(j).name());
                    sb.append(':').append(cg.columnTypes().get(j).isNullable() ? "1" : "0");
                }
            }
        }
        return sb.toString();
    }

    record FlushSchema(
            org.apache.avro.Schema avroSchema,
            List<String> pkFieldNames,
            String bodyFieldName,
            long schemaVersion,
            String bodyCodecName,
            List<ColumnGroup> columnGroups,
            String tableId,
            long numericTableId,
            BodyCodec bodyCodec,
            Schema schema,
            List<String> nonCgFieldNames) {

        FlushSchema(
                org.apache.avro.Schema avroSchema,
                List<String> pkFieldNames,
                String bodyFieldName,
                long schemaVersion,
                String bodyCodecName,
                List<ColumnGroup> columnGroups,
                String tableId,
                long numericTableId,
                BodyCodec bodyCodec,
                Schema schema) {
            this(
                    avroSchema,
                    pkFieldNames,
                    bodyFieldName,
                    schemaVersion,
                    bodyCodecName,
                    columnGroups,
                    tableId,
                    numericTableId,
                    bodyCodec,
                    schema,
                    ParquetFormatConstants.computeNonCgFieldNames(schema, columnGroups));
        }
    }

    private long validateAndGetSize(Storage.StoragePath storagePath, String targetUri) {
        try {
            if ("file".equals(storagePath.scheme())) {
                Path outputPath = Paths.get(URI.create(targetUri));
                if (!Files.exists(outputPath)) {
                    throw new IllegalStateException("Parquet file was not written: " + targetUri);
                }
                long size = Files.size(outputPath);
                if (size == 0) {
                    throw new IllegalStateException(
                            "Parquet file is empty (0 bytes): " + targetUri);
                }
                byte[] magic = new byte[4];
                try (var is = Files.newInputStream(outputPath)) {
                    if (is.readNBytes(magic, 0, magic.length) != magic.length) {
                        throw new IllegalStateException("Parquet file is corrupted: " + targetUri);
                    }
                }
                if (magic[0] != 'P' || magic[1] != 'A' || magic[2] != 'R' || magic[3] != '1') {
                    throw new IllegalStateException(
                            "Parquet file missing PAR1 header: " + targetUri);
                }
                return size;
            } else {
                org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(targetUri);
                org.apache.hadoop.fs.FileSystem fs = hdfsPath.getFileSystem(SHARED_CONF);
                if (!fs.exists(hdfsPath)) {
                    throw new IllegalStateException(
                            "Parquet file was not written to HDFS: " + targetUri);
                }
                long size = fs.getFileStatus(hdfsPath).getLen();
                if (size == 0) {
                    throw new IllegalStateException(
                            "Parquet file is empty (0 bytes) on HDFS: " + targetUri);
                }
                return size;
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to validate parquet file: " + targetUri, e);
        }
    }

    private static Storage.StoragePath resolveStoragePath(String base, String relative) {
        if (base == null || base.isBlank()) {
            throw new IllegalArgumentException("remoteBasePath is required");
        }
        URI uri;
        try {
            uri = URI.create(base);
        } catch (IllegalArgumentException ignored) {
            uri = null;
        }
        if (uri == null || uri.getScheme() == null) {
            String normalizedBase =
                    base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
            return new Storage.StoragePath("file", normalizedBase + "/" + relative);
        }

        String scheme = uri.getScheme();
        String locationPrefix;
        if ("file".equals(scheme)) {
            locationPrefix = uri.getPath();
        } else {
            String authority = uri.getRawAuthority();
            String path = uri.getRawPath();
            String prefix = authority != null ? "//" + authority : "";
            locationPrefix = prefix + (path != null ? path : "");
        }
        if (locationPrefix == null || locationPrefix.isEmpty()) {
            locationPrefix = "/";
        }
        String normalizedPrefix =
                locationPrefix.endsWith("/")
                        ? locationPrefix.substring(0, locationPrefix.length() - 1)
                        : locationPrefix;
        return new Storage.StoragePath(scheme, normalizedPrefix + "/" + relative);
    }

    private static String toUri(Storage.StoragePath path) {
        return path.scheme() + ":" + path.location();
    }

    private void deleteParquetFile(Storage.StoragePath storagePath, String targetUri) {
        try {
            if ("file".equals(storagePath.scheme())) {
                Path outputPath = Paths.get(URI.create(targetUri));
                Files.deleteIfExists(outputPath);
            } else {
                org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(targetUri);
                org.apache.hadoop.fs.FileSystem fs = hdfsPath.getFileSystem(SHARED_CONF);
                if (fs.exists(hdfsPath)) {
                    fs.delete(hdfsPath, false);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to delete orphan parquet file at {}: {}", targetUri, e.getMessage());
        }
    }

    private void postFlushCompactionCheck(String tableId) {
        LazyUpdateCompactor compactor = this.lazyUpdateCompactor;
        CompactionTrigger trigger = this.compactionTrigger;

        if (compactor == null || trigger == null) {
            return;
        }

        try {
            Optional<Snapshot> latestSnapshot =
                    compactor.getSnapshotManager().latestSnapshot(tableId);
            if (latestSnapshot.isEmpty()) {
                return;
            }
            long snapshotId = latestSnapshot.get().snapshotId();

            if (trigger.shouldCompact(tableId, snapshotId)) {
                compactor
                        .compactAsync(tableId, snapshotId)
                        .whenComplete(
                                (result, error) -> {
                                    if (error != null) {
                                        log.warn(
                                                "Compaction async failed for table={}: {}",
                                                tableId,
                                                error.getMessage());
                                    } else {
                                        trigger.recordCompactionCompleted();
                                        log.info(
                                                "Compaction completed for table={}: {}",
                                                tableId,
                                                result);
                                    }
                                });
            }
        } catch (Exception e) {
            log.warn(
                    "Post-flush compaction check failed for table={}: {}", tableId, e.getMessage());
        }
    }

    private long resolveSchemaVersion(String tableId) {
        SnapshotManager sm = this.snapshotManager;
        if (sm != null) {
            try {
                TableId id = new TableId("default", tableId);
                RawWiseTable table = sm.getCatalog().loadTable(id);
                if (table != null && table.meta() != null && table.meta().schema() != null) {
                    return table.meta().schema().version();
                }
            } catch (Exception e) {
                log.warn(
                        "Failed to resolve schema version for table {}, defaulting to 0",
                        tableId,
                        e);
            }
        }
        return 0L;
    }
}
