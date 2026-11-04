package io.github.rawwise.axiom.pipeline.ingestion;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.StorageWriteFacade;
import io.github.rawwise.axiom.pipeline.feedback.FeedbackSignal;
import io.github.rawwise.engine.storage.ImmutablePromotionService;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import io.github.rawwise.model.op.KvWriteBuilder;
import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.DefaultFeedbackAnnotation;
import io.github.rawwise.model.record.DefaultRecord;
import io.github.rawwise.model.record.FeedbackAnnotation;
import io.github.rawwise.model.record.ImmutabilitySource;
import io.github.rawwise.model.record.NullType;
import io.github.rawwise.model.record.Raw;
import io.github.rawwise.model.record.RawWiseType;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.SemanticRecord;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultIngestionEngine implements IngestionEngine {

    private static final long SYNTHETIC_SCHEMA_VERSION = 0L;
    private static final long SYNTHETIC_SEQUENCE_NUMBER = 0L;

    private final StorageWriteFacade store;
    private final ImmutablePromotionService promotionService;
    private final ImplicitViewGenerator viewGenerator;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, Object> viewAnnotationLocks = new ConcurrentHashMap<>();

    public DefaultIngestionEngine(
            StorageWriteFacade store,
            ImmutablePromotionService promotionService,
            ImplicitViewGenerator viewGenerator) {
        this.store = store;
        this.promotionService = promotionService;
        this.viewGenerator = viewGenerator;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public SemanticRecord ingest(IngestionRequest request) {
        Record.Key key = request.key();
        Record.Body body = request.body();
        long tableId = request.tableId();
        boolean immutable = request.immutable();

        String tableIdentifier = request.tableIdentifier();
        if (tableIdentifier == null) {
            tableIdentifier = resolveTableIdentifier(tableId);
        }
        if (tableIdentifier == null) {
            throw new IllegalStateException(
                    "Cannot resolve tableIdentifier for tableId="
                            + tableId
                            + ". Ensure the table has been registered via DynamicMmapStore.tableStore().");
        }

        byte[] encodedKey = key.encoded();
        Map<String, String> props = request.properties();
        byte[] bodyJson;
        if (props != null && !props.isEmpty()) {
            try {
                Map<String, Object> enriched = new LinkedHashMap<>(body.toMap());
                enriched.put("properties", objectMapper.writeValueAsString(props));
                bodyJson = objectMapper.writeValueAsBytes(enriched);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize body with properties to JSON", e);
            }
        } else {
            bodyJson = serializeBody(body);
        }
        byte[] wrappedValue = NullableValueCodec.encode(bodyJson);

        KvWriteBuilder kvBuilder =
                KvWriteBuilder.builder()
                        .table(new SimpleTableRef(tableIdentifier))
                        .key(encodedKey)
                        .value(wrappedValue)
                        .valuePreEncoded(true);

        store.write(kvBuilder);

        ImmutabilitySource immutableSource =
                immutable ? ImmutabilitySource.CREATED : ImmutabilitySource.DEFAULT;

        long commitTimestamp = resolveCommitTs();

        Record.Header header =
                new Record.Header(
                        key,
                        tableId,
                        SYNTHETIC_SCHEMA_VERSION,
                        SYNTHETIC_SEQUENCE_NUMBER,
                        commitTimestamp,
                        Record.Kind.INSERT,
                        true,
                        immutable,
                        immutableSource);

        DefaultRecord record = new DefaultRecord(key, body, header);

        if (immutable) {
            try {
                promotionService.markImmutable(key, tableId);
            } catch (Exception e) {
                log.warn(
                        "Failed to mark ingested record as immutable for tableId={}, key={}: {}",
                        tableId,
                        key,
                        e.getMessage(),
                        e);
            }
        }

        log.debug("Ingested record for tableId={}, key={}, immutable={}", tableId, key, immutable);
        return record;
    }

    @Override
    public Optional<Raw> readRaw(Record.Key key, long tableId, long snapshotId) {
        String tableIdentifier = resolveTableIdentifier(tableId);
        if (tableIdentifier == null) {
            log.warn("Cannot resolve tableIdentifier for tableId={} in readRaw", tableId);
            return Optional.empty();
        }

        try {
            TableStore ts = store.tableStore(tableIdentifier);
            byte[] encodedKey = key.encoded();
            byte[] rawValue = ts.getRawValue(encodedKey);

            if (rawValue == null) {
                log.debug("readRaw: no value found for tableId={}, key={}", tableId, key);
                return Optional.empty();
            }

            if (NullableValueCodec.isTombstone(rawValue)) {
                log.debug("readRaw: tombstone for tableId={}, key={}", tableId, key);
                return Optional.empty();
            }

            byte[] decoded = NullableValueCodec.decode(rawValue);
            if (decoded == null) {
                log.debug("readRaw: null decoded value for tableId={}, key={}", tableId, key);
                return Optional.empty();
            }

            Record record = tryDecodeAsRecord(ts, decoded);
            if (record == null) {
                record = decodeFromJson(key, tableId, decoded);
            }

            if (record == null) {
                log.warn("readRaw: unable to decode value for tableId={}, key={}", tableId, key);
                return Optional.empty();
            }

            Raw raw = Raw.of(record.key(), record.header(), record.body());

            triggerImplicitViewGeneration(key, tableId, snapshotId);

            tryMarkImmutableOnRead(key, tableId, record.header());

            return Optional.of(raw);
        } catch (RuntimeException e) {
            log.error("readRaw: unexpected error for tableId={}, key={}", tableId, key, e);
            return Optional.empty();
        }
    }

    @Override
    public void markImmutable(Record.Key key, long tableId) {
        promotionService.markImmutable(key, tableId);
    }

    @Override
    public SemanticRecord ingestFeedback(FeedbackSignal signal, long tableId) {
        String tableIdentifier = resolveTableIdentifier(tableId);
        if (tableIdentifier == null) {
            throw new IllegalStateException(
                    "Cannot resolve tableIdentifier for tableId="
                            + tableId
                            + ". Ensure the table has been registered via DynamicMmapStore.tableStore().");
        }

        Record.Key feedbackKey = buildFeedbackKey(signal);
        Record.Body body = buildFeedbackBody(signal);
        byte[] encodedKey = feedbackKey.encoded();
        byte[] bodyJson = serializeBody(body);
        byte[] wrappedValue = NullableValueCodec.encode(bodyJson);

        KvWriteBuilder kvBuilder =
                KvWriteBuilder.builder()
                        .table(new SimpleTableRef(tableIdentifier))
                        .key(encodedKey)
                        .value(wrappedValue)
                        .valuePreEncoded(true);

        store.write(kvBuilder);

        long commitTimestamp = resolveCommitTs();

        Record.Header header =
                new Record.Header(
                        feedbackKey,
                        tableId,
                        SYNTHETIC_SCHEMA_VERSION,
                        SYNTHETIC_SEQUENCE_NUMBER,
                        commitTimestamp,
                        Record.Kind.INSERT,
                        true,
                        true,
                        ImmutabilitySource.FEEDBACK);

        DefaultRecord record = new DefaultRecord(feedbackKey, body, header);

        try {
            promotionService.markImmutable(feedbackKey, tableId);
        } catch (Exception e) {
            log.warn(
                    "Failed to mark feedback record as immutable for tableId={}, key={}: {}",
                    tableId,
                    feedbackKey,
                    e.getMessage(),
                    e);
        }

        log.debug("Ingested feedback {} for tableId={}", signal.feedbackId(), tableId);
        return record;
    }

    @Override
    public boolean annotateView(String viewId, FeedbackAnnotation annotation) {
        return annotateView(viewId, 0, annotation);
    }

    public boolean annotateView(String viewId, int viewVersion, FeedbackAnnotation annotation) {
        try {
            Object lock = viewAnnotationLocks.computeIfAbsent(viewId, k -> new Object());
            synchronized (lock) {
                byte[] metaKey =
                        viewVersion > 0
                                ? viewAnnotationKey(viewId, viewVersion)
                                : viewAnnotationKeyLegacy(viewId);
                List<FeedbackAnnotation> existing = getViewAnnotations(viewId, viewVersion);
                List<FeedbackAnnotation> updated = new ArrayList<>(existing);
                updated.add(annotation);

                byte[] value =
                        objectMapper.writeValueAsBytes(
                                updated.stream().map(this::annotationToMap).toList());
                viewGenerator.metastore().put(metaKey, value);
                log.debug(
                        "Annotated view {} (v{}) with annotation {}",
                        viewId,
                        viewVersion,
                        annotation.annotationId());
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to annotate view {}: {}", viewId, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public List<FeedbackAnnotation> getViewAnnotations(String viewId) {
        return getViewAnnotations(viewId, 0);
    }

    public List<FeedbackAnnotation> getViewAnnotations(String viewId, int version) {
        try {
            if (version > 0) {
                byte[] metaKey = viewAnnotationKey(viewId, version);
                Optional<byte[]> raw = viewGenerator.metastore().get(metaKey);
                if (raw.isPresent()) {
                    List<Map<String, Object>> maps =
                            objectMapper.readValue(
                                    raw.get(), new TypeReference<List<Map<String, Object>>>() {});
                    return maps.stream().map(this::mapToAnnotation).toList();
                }
            }

            byte[] legacyKey = viewAnnotationKeyLegacy(viewId);
            Optional<byte[]> raw = viewGenerator.metastore().get(legacyKey);
            if (raw.isPresent()) {
                List<Map<String, Object>> maps =
                        objectMapper.readValue(
                                raw.get(), new TypeReference<List<Map<String, Object>>>() {});
                return maps.stream().map(this::mapToAnnotation).toList();
            }

            return List.of();
        } catch (Exception e) {
            log.warn("Failed to read annotations for view {}: {}", viewId, e.getMessage(), e);
            return List.of();
        }
    }

    private Record.Key buildFeedbackKey(FeedbackSignal signal) {
        byte[] keyBytes =
                ("feedback:" + signal.feedbackId())
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return Records.keyOf(keyBytes);
    }

    private Record.Body buildFeedbackBody(FeedbackSignal signal) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("feedbackId", signal.feedbackId());
        fields.put("source", signal.source().name());
        fields.put("path", signal.path().name());
        fields.put("signalType", signal.signalType().name());
        fields.put("targetEntityId", signal.targetEntityId());
        fields.put("targetLayer", signal.targetLayer().name());
        fields.put("score", signal.score());
        fields.put("timestamp", signal.timestamp());

        if (signal.payload() != null) {
            fields.putAll(signal.payload());
        }

        var builder = Schema.builder();
        Object[] values = new Object[fields.size()];
        int i = 0;
        for (var entry : fields.entrySet()) {
            builder.field(new StructField(entry.getKey(), inferType(entry.getValue())));
            values[i++] = entry.getValue();
        }

        return new DefaultBody(builder.build(), values);
    }

    private Map<String, Object> annotationToMap(FeedbackAnnotation ann) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("annotationId", ann.annotationId());
        map.put("viewId", ann.viewId());
        map.put("annotatorId", ann.annotatorId());
        map.put("type", ann.type().name());
        map.put("value", ann.value());
        map.put("confidence", ann.confidence());
        map.put("timestamp", ann.timestamp());
        return map;
    }

    private FeedbackAnnotation mapToAnnotation(Map<String, Object> map) {
        return new DefaultFeedbackAnnotation(
                (String) map.get("annotationId"),
                (String) map.get("viewId"),
                (String) map.get("annotatorId"),
                FeedbackAnnotation.AnnotationType.valueOf((String) map.get("type")),
                map.get("value"),
                ((Number) map.get("confidence")).doubleValue(),
                ((Number) map.get("timestamp")).longValue());
    }

    private static byte[] viewAnnotationKey(String viewId, int version) {
        return ("view_annotations:" + viewId + ":v" + version)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] viewAnnotationKeyLegacy(String viewId) {
        return ("view_annotations:" + viewId).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private Record tryDecodeAsRecord(TableStore ts, byte[] decoded) {
        try {
            var recordCodec = ts.getRecordCodec();
            if (recordCodec != null && recordCodec.isRecordCodecFormat(decoded)) {
                var schemaResolver = ts.getSchemaResolver();
                return recordCodec.decode(decoded, schemaResolver);
            }
        } catch (Exception e) {
            log.debug("RecordCodec decode failed, falling back to JSON: {}", e.getMessage());
        }
        return null;
    }

    private Record decodeFromJson(Record.Key key, long tableId, byte[] jsonBytes) {
        try {
            Map<String, Object> map =
                    objectMapper.readValue(jsonBytes, new TypeReference<Map<String, Object>>() {});

            if (map.isEmpty()) {
                return null;
            }

            Schema schema = inferSchema(map);
            Object[] values = new Object[map.size()];
            int i = 0;
            for (var field : schema.fields()) {
                values[i++] = map.get(field.name());
            }

            Record.Body body = new DefaultBody(schema, values);
            // Synthetic record reconstructed from JSON (no live txn context).
            // resolve Commit Ts() returns Long.MIN_VALUE sentinel - this record is for read-side
            // reconstruction only and must never be persisted to LMDB through this path.
            Record.Header header =
                    new Record.Header(
                            key,
                            tableId,
                            SYNTHETIC_SCHEMA_VERSION,
                            SYNTHETIC_SEQUENCE_NUMBER,
                            resolveCommitTs(),
                            Record.Kind.INSERT,
                            true,
                            false,
                            ImmutabilitySource.MUTABLE);

            return new DefaultRecord(key, body, header);
        } catch (Exception e) {
            log.debug("JSON decode failed: {}", e.getMessage());
            return null;
        }
    }

    private void triggerImplicitViewGeneration(Record.Key key, long tableId, long snapshotId) {
        try {
            if (!viewGenerator.isViewGenerated(key, tableId, snapshotId)) {
                viewGenerator.generateImplicitView(key, tableId, snapshotId);
            }
        } catch (Exception e) {
            log.warn(
                    "Implicit view generation failed for tableId={}, key={}: {}",
                    tableId,
                    key,
                    e.getMessage(),
                    e);
        }
    }

    /**
     * Resolves the commit Ts for ingestion records.
     *
     * <p>commit Ts is sourced from the active {@link Txn Context} (via {@link Txn Context Holder})
     * to ensure it stays within the same {@code Metastore Clock} domain as OCC validation. This
     * honors the unified strategy: {@code txn Id ≡ commit Ts}.
     *
     * <p>If no transaction context is active (e.g., synthetic record reconstruction in {@code parse
     * Json}), {@link Long#MIN_VALUE} is returned as the "unknown" sentinel - see plan Task 1.6 for
     * OCC/MVCC semantics. The caller is responsible for never persisting such records to LMDB
     * without subsequent overwrite via a real transaction.
     */
    private static long resolveCommitTs() {
        TxnContext ctx = TxnContextHolder.current();
        return ctx != null ? ctx.txnId() : Long.MIN_VALUE;
    }

    private void tryMarkImmutableOnRead(Record.Key key, long tableId, Record.Header header) {
        if (header.immutable()) {
            return;
        }
        if (!promotionService.canPromote(header.immutableSource())) {
            return;
        }
        try {
            promotionService.markImmutable(key, tableId);
        } catch (Exception e) {
            log.debug(
                    "markImmutable on read skipped for tableId={}, key={}: {}",
                    tableId,
                    key,
                    e.getMessage());
        }
    }

    private String resolveTableIdentifier(long tableId) {
        return store.findTableIdentifierByNumericId(tableId);
    }

    private byte[] serializeBody(Record.Body body) {
        try {
            Map<String, Object> map = body.toMap();
            return objectMapper.writeValueAsBytes(map);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize body to JSON", e);
        }
    }

    private static Schema inferSchema(Map<String, Object> map) {
        var builder = Schema.builder();
        for (var entry : map.entrySet()) {
            RawWiseType type = inferType(entry.getValue());
            builder.field(new StructField(entry.getKey(), type));
        }
        return builder.build();
    }

    private static RawWiseType inferType(Object value) {
        if (value == null) return NullType.INSTANCE;
        if (value instanceof Boolean) return Types.bool();
        if (value instanceof Integer) return Types.int32();
        if (value instanceof Long) return Types.int64();
        if (value instanceof Float) return Types.float32();
        if (value instanceof Double) return Types.float64();
        if (value instanceof String) return Types.string();
        if (value instanceof byte[]) return Types.binary();
        return Types.string();
    }
}
