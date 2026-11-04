package io.github.rawwise.engine.storage.block.read;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.engine.storage.ByteArrays;
import io.github.rawwise.engine.storage.ParquetFormatConstants;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.index.SparseIndex;
import io.github.rawwise.engine.storage.bloom.BloomFilterReader;
import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.codec.BodyCodecRegistry;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.metrics.ReaderMetrics;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Scan;
import io.github.rawwise.model.record.*;
import io.github.rawwise.model.record.Record;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.impl.RowSkipReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetPointReader;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

@Slf4j
public class ParquetBlockReader extends AbstractColdFormatBlockReader {
    private final KeyCodec keyCodec;
    private RecordCodec recordCodec;
    private RecordCodec.SchemaResolver schemaResolver;
    private volatile SnapshotManager snapshotManager;
    private volatile TableStore tableStore;

    /** Footer metadata keys used by the schema-aware flush path. */
    private static final Cache<String, ParquetMetadata> FOOTER_CACHE =
            Caffeine.newBuilder().maximumSize(512).build();

    /** Body codec column name used by schema-aware flush. */
    private static final String BODY_COLUMN_NAME = "body";

    public ParquetBlockReader() {
        this(new DefaultKeyCodec());
    }

    public ParquetBlockReader(ReaderMetrics metrics) {
        this(new DefaultKeyCodec(), metrics);
    }

    public ParquetBlockReader(KeyCodec keyCodec) {
        super(DataBlock.Format.PARQUET);
        this.keyCodec = keyCodec;
    }

    public ParquetBlockReader(KeyCodec keyCodec, ReaderMetrics metrics) {
        super(DataBlock.Format.PARQUET, metrics);
        this.keyCodec = keyCodec;
    }

    public ParquetBlockReader withRecordCodec(
            RecordCodec recordCodec, RecordCodec.SchemaResolver schemaResolver) {
        this.recordCodec = recordCodec;
        this.schemaResolver = schemaResolver;
        return this;
    }

    private static SparseIndex validateAndDeserializeIndex(byte[] rowGroupIndex) {
        if (rowGroupIndex == null || rowGroupIndex.length < 9) {
            return null;
        }
        SparseIndex index = SparseIndex.deserialize(rowGroupIndex);
        if (index == null || index.entries().isEmpty()) {
            return null;
        }
        List<SparseIndex.SparseIndexEntry> entries = index.entries();
        if (entries.get(0).rowIndex() != 0) {
            return null;
        }
        for (int i = 1; i < entries.size(); i++) {
            if (ByteArrays.compareUnsigned(entries.get(i - 1).key(), entries.get(i).key()) >= 0) {
                return null;
            }
        }
        return index;
    }

    private static int floorIndex(List<SparseIndex.SparseIndexEntry> entries, byte[] target) {
        int lo = 0, hi = entries.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = ByteArrays.compareUnsigned(entries.get(mid).key(), target);
            if (cmp < 0) {
                lo = mid + 1;
            } else if (cmp > 0) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return Math.max(hi, 0);
    }

    /** Set the SnapshotManager for historical Schema resolution from footer metadata. */
    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    /** Set the TableStore for lazy SnapshotManager access at read time. */
    public void setTableStore(TableStore tableStore) {
        this.tableStore = tableStore;
    }

    public byte[] getRawValueFromColdBlocks(List<ColdDataBlock> blocks, byte[] encodedKey)
            throws IOException {
        for (ColdDataBlock block : blocks) {
            if (block.keyRange() != null && !block.keyRange().contains(encodedKey)) {
                continue;
            }
            if (block.bloomFilter() != null
                    && !BloomFilterReader.mightContain(block.bloomFilter(), encodedKey)) {
                continue;
            }
            Path path = new Path(block.location().storagePath().location());
            try (var reader =
                    AvroParquetReader.<GenericRecord>builder(path)
                            .withConf(new Configuration())
                            .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    byte[] key = toBytes(record.get("key"));
                    if (!java.util.Arrays.equals(encodedKey, key)) {
                        continue;
                    }
                    return toBytes(record.get("value"));
                }
            } catch (Exception e) {
                log.warn("Cold block scan failed for {}: {}", path, e.getMessage());
            }
        }
        return null;
    }

    /**
     * Parse footer metadata from a body-codec-aware Parquet file.
     *
     * <p>Returns a {@link ParquetFooterMeta} containing schema version, body codec name, and
     * reconstructed ColumnGroups from the persisted metadata. Returns {@code null} if no body codec
     * metadata is found (legacy format).
     */
    private ParquetFooterMeta parseFooterMeta(Path parquetPath) {
        try {
            String cacheKey = parquetPath.toString();
            ParquetMetadata metadata =
                    FOOTER_CACHE.get(
                            cacheKey,
                            k -> {
                                try {
                                    return ParquetFileReader.readFooter(
                                            new Configuration(), parquetPath);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            Map<String, String> kvMeta = metadata.getFileMetaData().getKeyValueMetaData();

            String schemaVersionStr = kvMeta.get(ParquetFormatConstants.META_SCHEMA_VERSION);
            String bodyCodecName = kvMeta.get(ParquetFormatConstants.META_BODY_CODEC);
            String numericTableIdStr = kvMeta.get(ParquetFormatConstants.META_NUMERIC_TABLE_ID);
            if (bodyCodecName == null || bodyCodecName.isEmpty()) {
                return null;
            }
            long schemaVersion = schemaVersionStr != null ? Long.parseLong(schemaVersionStr) : 0L;
            long numericTableId =
                    numericTableIdStr != null ? Long.parseLong(numericTableIdStr) : 0L;

            String formatVersionStr = kvMeta.get(ParquetFormatConstants.META_FORMAT_VERSION);
            int formatVersion = 1;
            if (formatVersionStr != null) {
                try {
                    formatVersion = Integer.parseInt(formatVersionStr);
                } catch (NumberFormatException e) {
                    formatVersion = 1;
                }
            }

            String pkFieldNamesStr = kvMeta.get(ParquetFormatConstants.META_PK_FIELD_NAMES);
            List<String> pkFieldNames =
                    (pkFieldNamesStr != null && !pkFieldNamesStr.isEmpty())
                            ? List.of(pkFieldNamesStr.split(","))
                            : List.of();

            String cgRaw = kvMeta.get(ParquetFormatConstants.META_COLUMN_GROUPS);
            List<ColumnGroup> columnGroups = parseColumnGroups(cgRaw);

            return new ParquetFooterMeta(
                    schemaVersion,
                    bodyCodecName,
                    numericTableId,
                    columnGroups,
                    formatVersion,
                    pkFieldNames);
        } catch (Exception e) {
            log.warn(
                    "Failed to parse Parquet footer metadata from {}: {}",
                    parquetPath,
                    e.getMessage());
            return null;
        }
    }

    /**
     * Parse column groups from the compact persisted format. Format:
     * "name1:encoding,col1,col2;name2:encoding,col3,col4"
     */
    private static List<ColumnGroup> parseColumnGroups(String cgRaw) {
        List<ColumnGroup> result = new ArrayList<>();
        if (cgRaw == null || cgRaw.isEmpty()) return result;
        String[] groups = cgRaw.split(";");
        for (String group : groups) {
            int colonIdx = group.indexOf(':');
            if (colonIdx < 0) continue;
            String name = group.substring(0, colonIdx);
            String rest = group.substring(colonIdx + 1);
            int firstComma = rest.indexOf(',');
            if (firstComma < 0) continue;
            String headerPart = rest.substring(0, firstComma);
            String columnsPart = rest.substring(firstComma + 1);

            String[] headerTokens = headerPart.split(":");
            String encodingStr = headerTokens[0];
            int schemaVersion = headerTokens.length > 1 ? Integer.parseInt(headerTokens[1]) : 1;

            ColumnGroup.Encoding encoding;
            try {
                encoding = ColumnGroup.Encoding.valueOf(encodingStr);
            } catch (IllegalArgumentException e) {
                encoding = ColumnGroup.Encoding.FIXED_OFFSET;
            }

            List<String> columnNames = new ArrayList<>();
            List<Integer> columnFieldIds = new ArrayList<>();
            for (String colEntry : columnsPart.split(",")) {
                if (colEntry.isEmpty()) continue;
                String[] parts = colEntry.split(":");
                if (parts.length >= 2 && parts[0].matches("\\d+")) {
                    columnFieldIds.add(Integer.parseInt(parts[0]));
                    columnNames.add(parts[1]);
                } else {
                    columnFieldIds.add(0);
                    columnNames.add(parts[0]);
                }
            }

            result.add(
                    new ColumnGroup(
                            name, columnNames, List.of(), encoding, schemaVersion, columnFieldIds));
        }
        return result;
    }

    /**
     * Resolve the historical Schema for a given table and schema version.
     *
     * <p>Traverses ALL Snapshots to find one whose Schema has the matching version. Returns the
     * current latest Schema only as a last-resort fallback with a warning. Using the wrong Schema
     * causes column misalignment/data corruption.
     */
    private TableId parseTableId(String tableId) {
        String[] parts = tableId.split("\\.", 3);
        return parts.length >= 2
                ? new TableId(parts.length >= 3 ? parts[1] : parts[0], parts[parts.length - 1])
                : new TableId("default", tableId);
    }

    private Schema resolveHistoricalSchema(String tableId, long schemaVersion) {
        SnapshotManager sm = this.snapshotManager;
        if (sm == null) {
            TableStore ts = this.tableStore;
            if (ts != null) {
                sm = ts.getSnapshotManager();
            }
        }
        if (sm == null) return null;
        try {
            Catalog catalog = sm.getCatalog();
            if (catalog != null) {
                TableId tid = parseTableId(tableId);
                try {
                    Optional<Schema> historical = catalog.loadSchemaVersion(tid, schemaVersion);
                    if (historical.isPresent()) {
                        return historical.get();
                    }
                } catch (Exception e) {
                    log.warn(
                            "Failed to load schema version {} from catalog for table {}: {}",
                            schemaVersion,
                            tableId,
                            e.getMessage());
                }
            }

            List<Snapshot> allSnapshots = sm.getSnapshots(tableId);
            for (Snapshot s : allSnapshots) {
                if (s.hasSchema() && s.schema().version() == schemaVersion) {
                    return s.schema();
                }
            }

            boolean strictMode = false;
            if (catalog != null) {
                try {
                    TableId tid = parseTableId(tableId);
                    RawWiseTable table = catalog.loadTable(tid);
                    if (table != null
                            && table.meta() != null
                            && table.meta().properties() != null) {
                        strictMode =
                                "true"
                                        .equalsIgnoreCase(
                                                table.meta()
                                                        .properties()
                                                        .getOrDefault(
                                                                "schema.history.strict", "false"));
                    }
                } catch (Exception e) {
                    log.debug(
                            "Failed to check schema.history.strict for table {}: {}",
                            tableId,
                            e.getMessage());
                }
            }

            if (strictMode) {
                throw new IllegalStateException(
                        "Historical schema version "
                                + schemaVersion
                                + " not found for table "
                                + tableId
                                + " and schema.history.strict=true — refusing to use current schema as fallback");
            }

            Optional<Snapshot> latestSnap = sm.latestSnapshot(tableId);
            if (latestSnap.isPresent() && latestSnap.get().hasSchema()) {
                Schema currentSchema = latestSnap.get().schema();
                log.warn(
                        "Historical schema version {} not found in any snapshot for table {}. "
                                + "Falling back to current schema version {}. Schema evolution may apply.",
                        schemaVersion,
                        tableId,
                        currentSchema.version());
                return currentSchema;
            }
        } catch (Exception e) {
            if (e instanceof IllegalStateException) throw (IllegalStateException) e;
            log.warn(
                    "Failed to resolve historical schema for table={}, version={}: {}",
                    tableId,
                    schemaVersion,
                    e.getMessage());
        }
        return null;
    }

    /**
     * Reconstruct ColumnGroups from footer metadata with resolved types from Schema.
     *
     * <p>The persisted ColumnGroups only contain column names. Types are resolved from the
     * historical Schema to handle schema evolution correctly.
     */
    private List<ColumnGroup> reconstructColumnGroups(
            List<ColumnGroup> persistedCGs, Schema schema) {
        if (persistedCGs == null || persistedCGs.isEmpty() || schema == null) {
            return persistedCGs;
        }
        List<ColumnGroup> result = new ArrayList<>();
        Map<String, StructField> fieldMap = new LinkedHashMap<>();
        for (StructField field : schema.fields()) {
            fieldMap.put(field.name(), field);
        }

        for (ColumnGroup cg : persistedCGs) {
            List<io.github.rawwise.model.record.RawWiseType> columnTypes = new ArrayList<>();
            List<Integer> columnFieldIds = new ArrayList<>();
            List<String> columnNames = new ArrayList<>(cg.columnNames());
            for (String colName : columnNames) {
                StructField field = null;
                int persistedId = cg.columnFieldId(colName);
                if (persistedId > 0) {
                    field = schema.structType().findFieldById(persistedId).orElse(null);
                }
                if (field == null) {
                    field = fieldMap.get(colName);
                }
                if (field != null) {
                    columnTypes.add(field.dataType());
                    columnFieldIds.add(field.hasAssignedFieldId() ? field.fieldId() : 0);
                } else {
                    columnTypes.add(null);
                    columnFieldIds.add(cg.columnFieldId(colName));
                }
            }
            result.add(
                    new ColumnGroup(
                            cg.name(),
                            columnNames,
                            columnTypes,
                            cg.encoding(),
                            cg.schemaVersion(),
                            columnFieldIds));
        }
        return result;
    }

    /** Immutable record holding parsed Parquet footer metadata. */
    private record ParquetFooterMeta(
            long schemaVersion,
            String bodyCodecName,
            long numericTableId,
            List<ColumnGroup> columnGroups,
            int formatVersion,
            List<String> pkFieldNames) {
        boolean isMultiColumn() {
            return formatVersion >= 2;
        }
    }

    @Override
    protected Find.Result findVisible(Find find, List<ColdDataBlock> blocks) throws IOException {
        byte[] encodedKey = keyCodec.encodePrimaryKey(find.primaryKey().values());
        for (ColdDataBlock block : blocks) {
            if (block.keyRange() != null && !block.keyRange().contains(encodedKey)) {
                continue;
            }
            if (block.bloomFilter() != null
                    && !BloomFilterReader.mightContain(block.bloomFilter(), encodedKey)) {
                continue;
            }

            byte[] rowGroupIndex = block.rowGroupIndex();
            SparseIndex sparseIndex = validateAndDeserializeIndex(rowGroupIndex);
            long skipRows = 0;
            if (sparseIndex != null) {
                int entryIdx = floorIndex(sparseIndex.entries(), encodedKey);
                skipRows = sparseIndex.entries().get(entryIdx).rowIndex();
            }

            Path path = new Path(block.location().storagePath().location());

            ParquetFooterMeta footerMeta = parseFooterMeta(path);
            if (footerMeta != null && footerMeta.isMultiColumn()) {
                Record result =
                        readMultiColumnRecord(
                                path,
                                footerMeta,
                                block.tableId(),
                                encodedKey,
                                find.filter(),
                                find.projection(),
                                sparseIndex,
                                skipRows);
                if (result != null) {
                    return Find.Result.found(result, null);
                }
                continue;
            }

            if (footerMeta != null) {
                Record result =
                        readBodyCodecRecord(
                                path,
                                footerMeta,
                                block.tableId(),
                                encodedKey,
                                find.filter(),
                                null,
                                sparseIndex,
                                skipRows);
                if (result != null) {
                    return Find.Result.found(result, null);
                }
                continue;
            }

            // Legacy Parquet path: "key" + "value" columns
            try (var reader =
                    AvroParquetReader.<GenericRecord>builder(path)
                            .withConf(new Configuration())
                            .build()) {
                GenericRecord record;
                long rowsRead = 0;
                while ((record = reader.read()) != null) {
                    if (rowsRead < skipRows) {
                        rowsRead++;
                        continue;
                    }
                    if (sparseIndex != null && rowsRead - skipRows > 2000) {
                        break;
                    }
                    rowsRead++;
                    byte[] key = toBytes(record.get("key"));
                    if (!java.util.Arrays.equals(encodedKey, key)) {
                        continue;
                    }
                    byte[] valueBytes = toBytes(record.get("value"));
                    if (valueBytes == null || valueBytes.length == 0) {
                        continue;
                    }
                    Record rec = recordCodec.decode(valueBytes, schemaResolver);
                    try {
                        if (find.filter() != null && !find.filter().evaluate(rec)) {
                            rec.body().evict();
                            continue;
                        }
                        return Find.Result.found(rec, null);
                    } catch (Exception e) {
                        rec.body().evict();
                        throw e;
                    }
                }
            } catch (Exception e) {
                return Find.Result.error(
                        "Parquet find failed for " + path + ": " + e.getMessage(), e);
            }
        }
        return Find.Result.notFound();
    }

    @Override
    protected Scan.Result scanVisible(Scan scan, List<ColdDataBlock> blocks) throws IOException {
        List<Record> rows = new ArrayList<>();
        for (ColdDataBlock block : blocks) {
            if (scan.keyRange() != null
                    && block.keyRange() != null
                    && !block.keyRange().overlaps(scan.keyRange())) {
                continue;
            }
            Path path = new Path(block.location().storagePath().location());

            ParquetFooterMeta footerMeta = parseFooterMeta(path);
            if (footerMeta != null && footerMeta.isMultiColumn()) {
                List<Record> multiColRows =
                        readMultiColumnRecordsForScan(path, footerMeta, block.tableId(), scan);
                if (multiColRows != null) {
                    rows.addAll(multiColRows);
                }
                continue;
            }

            if (footerMeta != null) {
                List<Record> bodyCodecRows =
                        readBodyCodecRecordsForScan(path, footerMeta, block.tableId(), scan);
                if (bodyCodecRows != null) {
                    rows.addAll(bodyCodecRows);
                }
                continue;
            }

            // Legacy Parquet path: "key" + "value" columns
            try (var reader =
                    AvroParquetReader.<GenericRecord>builder(path)
                            .withConf(new Configuration())
                            .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    byte[] key = toBytes(record.get("key"));
                    byte[] value = toBytes(record.get("value"));
                    if (!inRange(key, scan)) {
                        continue;
                    }
                    if (value == null || value.length == 0) {
                        continue;
                    }
                    Record rec = recordCodec.decode(value, schemaResolver);
                    try {
                        if (matchesRecordPredicate(rec, scan)) {
                            Record projected = Records.projectCopy(rec, scan.projections());
                            rec.body().evict();
                            rows.add(projected);
                        } else {
                            rec.body().evict();
                        }
                    } catch (Exception e) {
                        rec.body().evict();
                        throw e;
                    }
                }
            } catch (Exception e) {
                return Scan.Result.error(
                        "Parquet scan failed for " + path + ": " + e.getMessage(), e);
            }
        }
        return Scan.Result.ok(rows, false);
    }

    /**
     * Read a single body-codec Parquet record matching the given key.
     *
     * @param path Parquet file path
     * @param footerMeta parsed footer metadata
     * @param tableIdStr table identifier string
     * @param encodedKey the encoded primary key to match
     * @param filter optional filter to evaluate
     * @param projections optional projection list (null = all columns)
     * @param sparseIndex optional sparse index for row skip
     * @param skipRows rows to skip based on sparse index lookup
     * @return the decoded Record, or null if not found/filtered out
     */
    private Record readBodyCodecRecord(
            Path path,
            ParquetFooterMeta footerMeta,
            String tableIdStr,
            byte[] encodedKey,
            Expression filter,
            List<String> projections,
            SparseIndex sparseIndex,
            long skipRows) {
        try {
            Schema schema = resolveHistoricalSchema(tableIdStr, footerMeta.schemaVersion());
            if (schema == null) return null;
            List<ColumnGroup> cgs = reconstructColumnGroups(footerMeta.columnGroups(), schema);
            Schema bodySchema =
                    new Schema(
                            schema.structType(),
                            Map.of(),
                            schema.version(),
                            schema.constraints(),
                            cgs);
            BodyCodec bc = BodyCodecRegistry.create(footerMeta.bodyCodecName(), bodySchema);

            try (var reader =
                    AvroParquetReader.<GenericRecord>builder(path)
                            .withConf(new Configuration())
                            .build()) {
                GenericRecord record;
                org.apache.avro.Schema avroSchema = null;
                long rowsRead = 0;
                while ((record = reader.read()) != null) {
                    if (rowsRead < skipRows) {
                        rowsRead++;
                        continue;
                    }
                    if (sparseIndex != null && rowsRead - skipRows > 2000) {
                        break;
                    }
                    rowsRead++;
                    if (avroSchema == null) {
                        avroSchema = record.getSchema();
                    }
                    byte[] pkBytes = extractPkFromBodyCodecRecord(record, avroSchema);
                    if (!java.util.Arrays.equals(encodedKey, pkBytes)) {
                        continue;
                    }
                    byte[] bodyBytes = toBytes(record.get(BODY_COLUMN_NAME));
                    if (bodyBytes == null || bodyBytes.length == 0) {
                        continue;
                    }

                    Record rec =
                            buildRecordFromBodyCodec(
                                    footerMeta.numericTableId(),
                                    schema,
                                    bc,
                                    pkBytes,
                                    bodyBytes,
                                    footerMeta.schemaVersion(),
                                    0L);
                    if (filter != null && !filter.evaluate(rec)) {
                        rec.body().evict();
                        continue;
                    }
                    return rec;
                }
            }
        } catch (Exception e) {
            log.warn("Body-codec Parquet find failed for {}: {}", path, e.getMessage());
        }
        return null;
    }

    /** Read body-codec Parquet records matching the scan criteria. */
    private List<Record> readBodyCodecRecordsForScan(
            Path path, ParquetFooterMeta footerMeta, String tableIdStr, Scan scan) {
        try {
            Schema schema = resolveHistoricalSchema(tableIdStr, footerMeta.schemaVersion());
            if (schema == null) return List.of();
            List<ColumnGroup> cgs = reconstructColumnGroups(footerMeta.columnGroups(), schema);
            Schema bodySchema =
                    new Schema(
                            schema.structType(),
                            Map.of(),
                            schema.version(),
                            schema.constraints(),
                            cgs);
            BodyCodec bc = BodyCodecRegistry.create(footerMeta.bodyCodecName(), bodySchema);

            List<Record> rows = new ArrayList<>();
            try (var reader =
                    AvroParquetReader.<GenericRecord>builder(path)
                            .withConf(new Configuration())
                            .build()) {
                GenericRecord record;
                org.apache.avro.Schema avroSchema = null;
                while ((record = reader.read()) != null) {
                    if (avroSchema == null) {
                        avroSchema = record.getSchema();
                    }
                    byte[] pkBytes = extractPkFromBodyCodecRecord(record, avroSchema);
                    if (!inRange(pkBytes, scan)) {
                        continue;
                    }
                    byte[] bodyBytes = toBytes(record.get(BODY_COLUMN_NAME));
                    if (bodyBytes == null || bodyBytes.length == 0) {
                        continue;
                    }

                    Record rec =
                            buildRecordFromBodyCodec(
                                    footerMeta.numericTableId(),
                                    schema,
                                    bc,
                                    pkBytes,
                                    bodyBytes,
                                    footerMeta.schemaVersion(),
                                    0L);

                    if (matchesRecordPredicate(rec, scan)) {
                        if (scan.projections() != null && !scan.projections().isEmpty()) {
                            Record projected = Records.projectCopy(rec, scan.projections());
                            rec.body().evict();
                            rows.add(projected);
                        } else {
                            rows.add(rec);
                        }
                    } else {
                        rec.body().evict();
                    }
                }
            }
            return rows;
        } catch (Exception e) {
            log.warn("Body-codec Parquet scan failed for {}: {}", path, e.getMessage());
            return List.of();
        }
    }

    /**
     * Extract the encoded primary key bytes from a body-codec Parquet record. PK columns are all
     * fields except the "body" column.
     */
    private byte[] extractPkFromBodyCodecRecord(
            GenericRecord record, org.apache.avro.Schema avroSchema) {
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            if (!BODY_COLUMN_NAME.equals(field.name())) {
                return toBytes(record.get(field.name()));
            }
        }
        return new byte[0];
    }

    /** Build a Record from body-codec Parquet data using CgEncodedBody for lazy decode. */
    private Record buildRecordFromBodyCodec(
            long numericTableId,
            Schema schema,
            BodyCodec bc,
            byte[] pkBytes,
            byte[] bodyBytes,
            long schemaVersion,
            long commitTs) {
        Record.Key key = Records.keyOf(pkBytes);
        CgEncodedBody body = new CgEncodedBody(schema, bc, bodyBytes);
        Record.Header header =
                Record.Header.of(
                        key, numericTableId, schemaVersion, 0L, commitTs, Record.Kind.INSERT, true);
        return new DefaultRecord(key, body, header);
    }

    private Record readMultiColumnRecord(
            Path path,
            ParquetFooterMeta footerMeta,
            String tableIdStr,
            byte[] encodedKey,
            Expression filter,
            List<String> projections,
            SparseIndex sparseIndex,
            long skipRows) {
        try {
            Schema schema = resolveHistoricalSchema(tableIdStr, footerMeta.schemaVersion());
            if (schema == null) return null;
            List<ColumnGroup> cgs = reconstructColumnGroups(footerMeta.columnGroups(), schema);
            List<String> nonCgFieldNames =
                    ParquetFormatConstants.computeNonCgFieldNames(schema, cgs);

            Record result =
                    readMultiColumnViaRowGroup(
                            path,
                            footerMeta,
                            schema,
                            cgs,
                            nonCgFieldNames,
                            encodedKey,
                            filter,
                            projections);
            if (result != null) return result;

            result =
                    readMultiColumnViaPointReader(
                            path,
                            footerMeta,
                            schema,
                            cgs,
                            nonCgFieldNames,
                            encodedKey,
                            filter,
                            projections);
            if (result != null) return result;

            return readMultiColumnFullScan(
                    path,
                    footerMeta,
                    schema,
                    cgs,
                    nonCgFieldNames,
                    encodedKey,
                    filter,
                    projections,
                    sparseIndex,
                    skipRows);
        } catch (Exception e) {
            log.warn("Multi-column Parquet find failed for {}: {}", path, e.getMessage());
        }
        return null;
    }

    private Record readMultiColumnViaRowGroup(
            Path path,
            ParquetFooterMeta footerMeta,
            Schema schema,
            List<ColumnGroup> cgs,
            List<String> nonCgFieldNames,
            byte[] encodedKey,
            Expression filter,
            List<String> projections) {
        try {
            Configuration conf = new Configuration();
            ParquetMetadata footer =
                    FOOTER_CACHE.get(
                            path.toString(),
                            k -> {
                                try {
                                    return ParquetFileReader.readFooter(conf, path);
                                } catch (Exception e) {
                                    return null;
                                }
                            });
            if (footer == null) return null;

            List<BlockMetaData> allBlocks = footer.getBlocks();
            if (allBlocks.isEmpty()) return null;

            List<BlockMetaData> matchingBlocks =
                    ParquetRowGroupReader.findMatchingBlocks(
                            allBlocks, ParquetFormatConstants.PK_COLUMN_NAME, encodedKey);
            if (matchingBlocks.isEmpty()) return null;

            MessageType fileSchema = footer.getFileMetaData().getSchema();
            Map<String, String> fileMeta = footer.getFileMetaData().getKeyValueMetaData();

            for (BlockMetaData block : matchingBlocks) {
                int blockIndex = allBlocks.indexOf(block);
                try {
                    ParquetRowGroupReader.RowGroupReadResult rgResult =
                            ParquetRowGroupReader.readSingleRowGroup(
                                    conf,
                                    path,
                                    blockIndex,
                                    fileSchema,
                                    fileMeta,
                                    ParquetFormatConstants.PK_COLUMN_NAME,
                                    encodedKey);

                    long skipRows = rgResult.columnIndexSkipRows();
                    long rowCount = rgResult.rowCount();
                    RecordReader<Group> recordReader = rgResult.recordReader();

                    try {
                        for (long r = 0; r < rowCount; r++) {
                            Group group = recordReader.read();
                            if (group == null) break;
                            if (r < skipRows) continue;

                            byte[] pkBytes =
                                    group.getBinary(ParquetFormatConstants.PK_COLUMN_NAME, 0)
                                            .getBytes();
                            if (!java.util.Arrays.equals(encodedKey, pkBytes)) continue;

                            Record rec =
                                    buildRecordFromMultiColumnGroup(
                                            footerMeta.numericTableId(),
                                            schema,
                                            cgs,
                                            nonCgFieldNames,
                                            group,
                                            footerMeta.schemaVersion());

                            if (filter != null && !filter.evaluate(rec)) {
                                rec.body().evict();
                                continue;
                            }

                            if (projections != null && !projections.isEmpty()) {
                                Record projected = Records.projectCopy(rec, projections);
                                rec.body().evict();
                                return projected;
                            }
                            return rec;
                        }
                    } finally {
                        rgResult.fileReader().close();
                    }
                } catch (Exception e) {
                    log.debug("Row Group read failed for block {}: {}", blockIndex, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.debug(
                    "Row Group optimization failed, will fallback to full scan: {}",
                    e.getMessage());
        }
        return null;
    }

    private Record readMultiColumnViaPointReader(
            Path path,
            ParquetFooterMeta footerMeta,
            Schema schema,
            List<ColumnGroup> cgs,
            List<String> nonCgFieldNames,
            byte[] encodedKey,
            Expression filter,
            List<String> projections) {
        try {
            Configuration conf = new Configuration();
            org.apache.parquet.hadoop.util.HadoopInputFile inputFile =
                    org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, conf);
            ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
            try {
                List<BlockMetaData> allBlocks = fileReader.getFooter().getBlocks();
                if (allBlocks.isEmpty()) return null;

                List<BlockMetaData> matchingBlocks =
                        ParquetRowGroupReader.findMatchingBlocks(
                                allBlocks, ParquetFormatConstants.PK_COLUMN_NAME, encodedKey);
                if (matchingBlocks.isEmpty()) return null;

                MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
                String createdBy = fileReader.getFooter().getFileMetaData().getCreatedBy();

                for (BlockMetaData block : matchingBlocks) {
                    PageReadStore pageStore = fileReader.readRowGroup(allBlocks.indexOf(block));
                    org.apache.parquet.hadoop.example.GroupReadSupport readSupport =
                            new org.apache.parquet.hadoop.example.GroupReadSupport();
                    org.apache.parquet.io.api.RecordMaterializer<Group> materializer =
                            readSupport.prepareForRead(
                                    conf,
                                    fileReader.getFooter().getFileMetaData().getKeyValueMetaData(),
                                    fileSchema,
                                    readSupport.init(
                                            conf,
                                            fileReader
                                                    .getFooter()
                                                    .getFileMetaData()
                                                    .getKeyValueMetaData(),
                                            fileSchema));

                    org.apache.parquet.io.ColumnIOFactory columnIOFactory =
                            new org.apache.parquet.io.ColumnIOFactory(true);
                    org.apache.parquet.io.MessageColumnIO columnIO =
                            columnIOFactory.getColumnIO(fileSchema, fileSchema);

                    RowSkipReadStore skipStore =
                            new RowSkipReadStore(
                                    pageStore,
                                    materializer.getRootConverter(),
                                    fileSchema,
                                    createdBy);

                    ParquetPointReader<Group> pointReader =
                            new ParquetPointReader<>(
                                    columnIO,
                                    materializer,
                                    false,
                                    skipStore,
                                    ColumnPath.fromDotString(ParquetFormatConstants.PK_COLUMN_NAME),
                                    block.getRowCount());

                    int seekResult =
                            pointReader.seekTo(
                                    org.apache.parquet.io.api.Binary.fromReusedByteArray(
                                            encodedKey));
                    if (seekResult != 0) continue;

                    Group group = pointReader.read();
                    if (group == null) continue;

                    Record rec =
                            buildRecordFromMultiColumnGroup(
                                    footerMeta.numericTableId(),
                                    schema,
                                    cgs,
                                    nonCgFieldNames,
                                    group,
                                    footerMeta.schemaVersion());

                    if (filter != null && !filter.evaluate(rec)) {
                        rec.body().evict();
                        continue;
                    }

                    if (projections != null && !projections.isEmpty()) {
                        Record projected = Records.projectCopy(rec, projections);
                        rec.body().evict();
                        return projected;
                    }
                    return rec;
                }
            } finally {
                fileReader.close();
            }
        } catch (Exception e) {
            log.debug("Point reader optimization failed, will fallback: {}", e.getMessage());
        }
        return null;
    }

    private Record buildRecordFromMultiColumnGroup(
            long numericTableId,
            Schema schema,
            List<ColumnGroup> cgs,
            List<String> nonCgFieldNames,
            Group group,
            long schemaVersion) {
        byte[] pkBytes = group.getBinary(ParquetFormatConstants.PK_COLUMN_NAME, 0).getBytes();
        long commitTs = readCommitTsFromGroup(group);
        return assembleMultiColumnRecord(
                numericTableId,
                schema,
                cgs,
                nonCgFieldNames,
                pkBytes,
                schemaVersion,
                name -> {
                    if (group.getFieldRepetitionCount(name) > 0) {
                        return group.getBinary(name, 0).getBytes();
                    }
                    return null;
                },
                commitTs);
    }

    private Record readMultiColumnFullScan(
            Path path,
            ParquetFooterMeta footerMeta,
            Schema schema,
            List<ColumnGroup> cgs,
            List<String> nonCgFieldNames,
            byte[] encodedKey,
            Expression filter,
            List<String> projections,
            SparseIndex sparseIndex,
            long skipRows) {
        try {
            org.apache.avro.Schema readSchema =
                    buildProjectedAvroSchema(cgs, nonCgFieldNames, projections);

            Configuration conf = new Configuration();
            if (projections != null && !projections.isEmpty()) {
                AvroReadSupport.setRequestedProjection(conf, readSchema);
            }

            try (var reader =
                    AvroParquetReader.<GenericRecord>builder(path).withConf(conf).build()) {
                GenericRecord record;
                long rowsRead = 0;
                while ((record = reader.read()) != null) {
                    if (rowsRead < skipRows) {
                        rowsRead++;
                        continue;
                    }
                    if (sparseIndex != null && rowsRead - skipRows > 2000) {
                        break;
                    }
                    rowsRead++;

                    byte[] pkBytes = extractPkFromMultiColumnRecord(record);
                    if (!java.util.Arrays.equals(encodedKey, pkBytes)) {
                        continue;
                    }

                    Record rec =
                            buildRecordFromMultiColumn(
                                    footerMeta.numericTableId(),
                                    schema,
                                    cgs,
                                    nonCgFieldNames,
                                    record,
                                    footerMeta.schemaVersion());

                    if (filter != null && !filter.evaluate(rec)) {
                        rec.body().evict();
                        continue;
                    }

                    if (projections != null && !projections.isEmpty()) {
                        Record projected = Records.projectCopy(rec, projections);
                        rec.body().evict();
                        return projected;
                    }
                    return rec;
                }
            }
        } catch (Exception e) {
            log.warn("Multi-column full scan failed for {}: {}", path, e.getMessage());
        }
        return null;
    }

    private List<Record> readMultiColumnRecordsForScan(
            Path path, ParquetFooterMeta footerMeta, String tableIdStr, Scan scan) {
        try {
            Schema schema = resolveHistoricalSchema(tableIdStr, footerMeta.schemaVersion());
            if (schema == null) return List.of();
            List<ColumnGroup> cgs = reconstructColumnGroups(footerMeta.columnGroups(), schema);
            List<String> nonCgFieldNames =
                    ParquetFormatConstants.computeNonCgFieldNames(schema, cgs);

            List<Record> rows = new ArrayList<>();
            try (var reader =
                    AvroParquetReader.<GenericRecord>builder(path)
                            .withConf(new Configuration())
                            .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    byte[] pkBytes = extractPkFromMultiColumnRecord(record);
                    if (!inRange(pkBytes, scan)) {
                        continue;
                    }

                    Record rec =
                            buildRecordFromMultiColumn(
                                    footerMeta.numericTableId(),
                                    schema,
                                    cgs,
                                    nonCgFieldNames,
                                    record,
                                    footerMeta.schemaVersion());

                    if (matchesRecordPredicate(rec, scan)) {
                        if (scan.projections() != null && !scan.projections().isEmpty()) {
                            Record projected = Records.projectCopy(rec, scan.projections());
                            rec.body().evict();
                            rows.add(projected);
                        } else {
                            rows.add(rec);
                        }
                    } else {
                        rec.body().evict();
                    }
                }
            }
            return rows;
        } catch (Exception e) {
            log.warn("Multi-column Parquet scan failed for {}: {}", path, e.getMessage());
            return List.of();
        }
    }

    private byte[] extractPkFromMultiColumnRecord(GenericRecord record) {
        return toBytes(record.get(ParquetFormatConstants.PK_COLUMN_NAME));
    }

    private Record buildRecordFromMultiColumn(
            long numericTableId,
            Schema schema,
            List<ColumnGroup> cgs,
            List<String> nonCgFieldNames,
            GenericRecord record,
            long schemaVersion) {
        byte[] pkBytes = extractPkFromMultiColumnRecord(record);
        long commitTs = readCommitTsFromGenericRecord(record);
        return assembleMultiColumnRecord(
                numericTableId,
                schema,
                cgs,
                nonCgFieldNames,
                pkBytes,
                schemaVersion,
                name -> {
                    Object raw = record.get(name);
                    return raw != null ? toBytes(raw) : null;
                },
                commitTs);
    }

    private Record assembleMultiColumnRecord(
            long numericTableId,
            Schema schema,
            List<ColumnGroup> cgs,
            List<String> nonCgFieldNames,
            byte[] pkBytes,
            long schemaVersion,
            java.util.function.Function<String, byte[]> fieldBytesProvider,
            long commitTs) {
        Record.Key key = Records.keyOf(pkBytes);

        Map<String, Object> allFields = new LinkedHashMap<>();

        if (cgs != null) {
            for (ColumnGroup cg : cgs) {
                byte[] cgBytes = fieldBytesProvider.apply(cg.name());
                if (cgBytes != null && cgBytes.length > 0) {
                    ColumnGroupCodec cgCodec = ColumnGroupCodecFactory.getCodec(cg);
                    allFields.putAll(cgCodec.decode(cg, cgBytes));
                }
            }
        }

        for (String fieldName : nonCgFieldNames) {
            byte[] rawBytes = fieldBytesProvider.apply(fieldName);
            if (rawBytes != null && rawBytes.length > 0) {
                Optional<StructField> sfOpt = schema.structType().field(fieldName);
                if (sfOpt.isPresent()) {
                    allFields.put(
                            fieldName, decodeNonCgFieldValue(rawBytes, sfOpt.get().dataType()));
                } else {
                    allFields.put(fieldName, rawBytes);
                }
            }
        }

        Object[] values = new Object[schema.fields().size()];
        List<StructField> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            values[i] = allFields.get(fields.get(i).name());
        }

        DefaultBody body = new DefaultBody(schema, values);
        Record.Header header =
                Record.Header.of(
                        key, numericTableId, schemaVersion, 0L, commitTs, Record.Kind.INSERT, true);
        return new DefaultRecord(key, body, header);
    }

    private static long readCommitTsFromGroup(Group group) {
        try {
            return group.getLong(ParquetFormatConstants.COMMIT_TIMESTAMP_COLUMN_NAME, 0);
        } catch (RuntimeException e) {
            return Long.MIN_VALUE;
        }
    }

    private static long readCommitTsFromGenericRecord(GenericRecord record) {
        try {
            Object val = record.get(ParquetFormatConstants.COMMIT_TIMESTAMP_COLUMN_NAME);
            if (val instanceof Long l) return l;
            return Long.MIN_VALUE;
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }

    private static Object decodeNonCgFieldValue(byte[] bytes, RawWiseType type) {
        return switch (type.name()) {
            case "int64" -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong();
            case "int32" -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
            case "float64" -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
            case "float32" -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getFloat();
            case "boolean" -> bytes.length > 0 && bytes[0] != 0;
            case "string" -> new String(bytes, StandardCharsets.UTF_8);
            case "binary" -> bytes;
            default -> bytes;
        };
    }

    private static org.apache.avro.Schema buildProjectedAvroSchema(
            List<ColumnGroup> cgs, List<String> nonCgFieldNames, List<String> projections) {
        if (projections == null || projections.isEmpty()) {
            return buildFullAvroSchema(cgs, nonCgFieldNames);
        }

        Set<String> needed = new HashSet<>(projections);
        needed.add(ParquetFormatConstants.PK_COLUMN_NAME);

        Set<String> neededCgs = new HashSet<>();
        if (cgs != null) {
            for (ColumnGroup cg : cgs) {
                for (String col : cg.columnNames()) {
                    if (needed.contains(col)) {
                        neededCgs.add(cg.name());
                        break;
                    }
                }
            }
        }

        Set<String> neededNonCg = new HashSet<>();
        for (String fieldName : nonCgFieldNames) {
            if (needed.contains(fieldName)) {
                neededNonCg.add(fieldName);
            }
        }

        org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> assembler =
                org.apache.avro.SchemaBuilder.record("ProjectedRead").fields();
        assembler =
                assembler
                        .name(ParquetFormatConstants.PK_COLUMN_NAME)
                        .type()
                        .bytesType()
                        .noDefault();

        if (cgs != null) {
            for (ColumnGroup cg : cgs) {
                if (neededCgs.contains(cg.name())) {
                    assembler = assembler.name(cg.name()).type().bytesType().noDefault();
                }
            }
        }

        for (String fieldName : nonCgFieldNames) {
            if (neededNonCg.contains(fieldName)) {
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
        }

        return assembler.endRecord();
    }

    private static org.apache.avro.Schema buildFullAvroSchema(
            List<ColumnGroup> cgs, List<String> nonCgFieldNames) {
        org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> assembler =
                org.apache.avro.SchemaBuilder.record("FullRead").fields();
        assembler =
                assembler
                        .name(ParquetFormatConstants.PK_COLUMN_NAME)
                        .type()
                        .bytesType()
                        .noDefault();

        if (cgs != null) {
            for (ColumnGroup cg : cgs) {
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

        return assembler.endRecord();
    }

    private boolean matchesRecordPredicate(Record record, Scan scan) {
        if (scan.getPushedPredicate() == null) {
            return true;
        }
        return scan.getPushedPredicate().evaluate(record);
    }

    private boolean inRange(byte[] key, Scan scan) {
        return scan.keyRange() == null || scan.keyRange().contains(key);
    }

    private byte[] toBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer byteBuffer) {
            ByteBuffer duplicate = byteBuffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }
        if (value instanceof CharSequence sequence) {
            return sequence.toString().getBytes(StandardCharsets.UTF_8);
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }
}
