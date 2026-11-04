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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * based on Metastore {@link CommitLedgerStore} implementation.
 *
 * <p>Metastore key pattern: /commit_ledger/{catalog}/{db}/{table}/{idempotencyKey}
 *
 * <p>tableId parserule:
 *
 * <ul>
 *   <li>"database.table" (2-segment) → key: /commit_ledger/database/table/{idempotencyKey}
 *   <li>"catalog.database.table" (3-segment) → key:
 *       /commit_ledger/catalog/database/table/{idempotencyKey}
 * </ul>
 *
 * <p>serializationformat: JSON(Jackson ObjectMapper), support Java records and Map&lt;Integer,
 * ShardLedgerState&gt;.
 *
 * <p>CAS semantics:
 *
 * <ul>
 *   <li>{@link #save} — NotExists(key): firstcreate, ensureidempotent keyunique
 *   <li>{@link #updateState} — ValueEquals(key, oldSerialized): optimisticlockstatetransition
 * </ul>
 */
@Slf4j
public class MetastoreCommitLedgerStore implements CommitLedgerStore {

    private static final String KEY_PREFIX = "/commit_ledger/";

    private final Metastore metastore;
    private final ObjectMapper objectMapper;

    public MetastoreCommitLedgerStore(Metastore metastore) {
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
    public boolean save(CommitLedger ledger) throws IOException {
        Objects.requireNonNull(ledger, "ledger must not be null");
        byte[] key = buildKey(ledger.tableId(), ledger.idempotencyKey());
        byte[] value = serialize(ledger);
        boolean success =
                metastore.compareAndSwap(
                        List.of(new Precondition.NotExists(key)),
                        List.of(new Mutation.Put(key, value)));
        if (success) {
            log.info(
                    "Saved CommitLedger: idempotencyKey={} batchId={} state={}",
                    ledger.idempotencyKey(),
                    ledger.batchId(),
                    ledger.state());
        } else {
            log.warn("CommitLedger already exists: idempotencyKey={}", ledger.idempotencyKey());
        }
        return success;
    }

    @Override
    public Optional<CommitLedger> load(String idempotencyKey) throws IOException {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        byte[] prefix = KEY_PREFIX.getBytes(StandardCharsets.UTF_8);
        String suffix = "/" + idempotencyKey;
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
    public Optional<CommitLedger> loadByBatchId(String batchId) throws IOException {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        byte[] prefix = KEY_PREFIX.getBytes(StandardCharsets.UTF_8);
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                CommitLedger ledger = deserialize(kv.value());
                if (batchId.equals(ledger.batchId())) {
                    return Optional.of(ledger);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean delete(String idempotencyKey) throws IOException {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        Optional<CommitLedger> existing = load(idempotencyKey);
        if (existing.isEmpty()) {
            return false;
        }
        byte[] key = buildKey(existing.get().tableId(), idempotencyKey);
        metastore.delete(key);
        log.info("Deleted CommitLedger: idempotencyKey={}", idempotencyKey);
        return true;
    }

    @Override
    public CommitLedger updateState(String idempotencyKey, BatchState newState) throws IOException {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        Objects.requireNonNull(newState, "newState must not be null");

        CommitLedger current =
                load(idempotencyKey)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "CommitLedger not found for idempotencyKey="
                                                        + idempotencyKey));

        if (!current.state().canTransitionTo(newState)) {
            throw new IllegalStateException(
                    "Illegal batch state transition: "
                            + current.state()
                            + " → "
                            + newState
                            + " (idempotencyKey="
                            + idempotencyKey
                            + ")");
        }

        CommitLedger updated = current.withState(newState);
        byte[] key = buildKey(current.tableId(), idempotencyKey);
        byte[] oldValue = serialize(current);
        byte[] newValue = serialize(updated);

        boolean success =
                metastore.compareAndSwap(
                        List.of(new Precondition.ValueEquals(key, oldValue)),
                        List.of(new Mutation.Put(key, newValue)));

        if (!success) {
            throw new IllegalStateException(
                    "CAS failed for CommitLedger state transition: idempotencyKey="
                            + idempotencyKey
                            + ", concurrent modification detected");
        }

        log.info(
                "Updated CommitLedger state: idempotencyKey={} {} → {}",
                idempotencyKey,
                current.state(),
                newState);
        return updated;
    }

    @Override
    public java.util.List<CommitLedger> listIncomplete() throws IOException {
        java.util.List<CommitLedger> incomplete = new java.util.ArrayList<>();
        byte[] prefix = KEY_PREFIX.getBytes(StandardCharsets.UTF_8);
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                try {
                    CommitLedger ledger = deserialize(kv.value());
                    if (ledger.state() != BatchState.COMMITTED
                            && ledger.state() != BatchState.ABORTED) {
                        incomplete.add(ledger);
                    }
                } catch (Exception e) {
                    log.warn(
                            "Failed to deserialize CommitLedger during listIncomplete: {}",
                            e.getMessage());
                }
            }
        }
        return incomplete;
    }

    private byte[] buildKey(String tableId, String idempotencyKey) {
        String pathPart = tableId.replace('.', '/');
        return (KEY_PREFIX + pathPart + "/" + idempotencyKey).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] serialize(CommitLedger ledger) throws IOException {
        return objectMapper.writeValueAsBytes(ledger);
    }

    private CommitLedger deserialize(byte[] data) throws IOException {
        return objectMapper.readValue(data, CommitLedger.class);
    }
}
