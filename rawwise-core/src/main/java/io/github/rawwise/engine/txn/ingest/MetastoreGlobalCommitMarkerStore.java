package io.github.rawwise.engine.txn.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * based on Metastore {@link GlobalCommitMarkerStore} implementation.
 *
 * <p>Metastore key pattern: /commit_marker/{catalog}/{db}/{table}/{batchId}
 *
 * <p>serializationformat: JSON(Jackson ObjectMapper), support Java records and
 * List&lt;ShardRef&gt;.
 *
 * <p>CAS semantics:
 *
 * <ul>
 *   <li>{@link #create} — NotExists(key): ensure marker onlycreateonetime(notrollbackboundary)
 * </ul>
 *
 * <p>concurrentcontention(E2E-006b): two batch while CAS sameone marker → onlyonesuccess,
 * failpartyreturn false.
 */
@Slf4j
public class MetastoreGlobalCommitMarkerStore implements GlobalCommitMarkerStore {

    private static final String KEY_PREFIX = "/commit_marker/";

    private final Metastore metastore;
    private final ObjectMapper objectMapper;

    public MetastoreGlobalCommitMarkerStore(Metastore metastore) {
        this.metastore = Objects.requireNonNull(metastore, "metastore must not be null");
        this.objectMapper = createObjectMapper();
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(
                com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        return mapper;
    }

    @Override
    public boolean create(GlobalCommitMarker marker) throws IOException {
        Objects.requireNonNull(marker, "marker must not be null");
        byte[] key =
                buildKey(marker.catalog(), marker.database(), marker.table(), marker.batchId());
        byte[] value = serialize(marker);
        boolean success =
                metastore.compareAndSwap(
                        List.of(new Precondition.NotExists(key)),
                        List.of(new Mutation.Put(key, value)));
        if (success) {
            log.info(
                    "Created GlobalCommitMarker: batchId={} table={}.{}.{} commitTs={} state={}",
                    marker.batchId(),
                    marker.catalog(),
                    marker.database(),
                    marker.table(),
                    marker.commitTs(),
                    marker.state());
        } else {
            log.warn(
                    "GlobalCommitMarker already exists: batchId={} table={}.{}.{}",
                    marker.batchId(),
                    marker.catalog(),
                    marker.database(),
                    marker.table());
        }
        return success;
    }

    @Override
    public Optional<GlobalCommitMarker> load(String batchId) throws IOException {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        byte[] prefix = KEY_PREFIX.getBytes(StandardCharsets.UTF_8);
        String suffix = "/" + batchId;
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                String keyStr = new String(kv.key(), StandardCharsets.UTF_8);
                if (keyStr.endsWith(suffix)) {
                    return Optional.of(deserialize(kv.value()));
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public List<GlobalCommitMarker> loadByTable(String catalog, String database, String table)
            throws IOException {
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(database, "database must not be null");
        Objects.requireNonNull(table, "table must not be null");
        byte[] prefix = buildTablePrefix(catalog, database, table);
        List<GlobalCommitMarker> markers = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                markers.add(deserialize(kv.value()));
            }
        }
        return markers;
    }

    private byte[] buildKey(String catalog, String database, String table, String batchId) {
        return (KEY_PREFIX + catalog + "/" + database + "/" + table + "/" + batchId)
                .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] buildTablePrefix(String catalog, String database, String table) {
        return (KEY_PREFIX + catalog + "/" + database + "/" + table + "/")
                .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] serialize(GlobalCommitMarker marker) throws IOException {
        return objectMapper.writeValueAsBytes(marker);
    }

    private GlobalCommitMarker deserialize(byte[] data) throws IOException {
        return objectMapper.readValue(data, GlobalCommitMarker.class);
    }
}
