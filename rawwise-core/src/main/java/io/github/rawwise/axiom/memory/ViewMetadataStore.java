package io.github.rawwise.axiom.memory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ViewMetadataStore {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final String PREFIX = "viewmeta";
    private static final byte SEPARATOR = 0x00;

    private final Metastore metastore;

    public ViewMetadataStore(Metastore metastore) {
        this.metastore = metastore;
    }

    public void put(ViewMetadata metadata) throws IOException {
        byte[] key = buildKey(metadata.tableId(), metadata.entryKey());
        byte[] value = MAPPER.writeValueAsBytes(metadata);
        metastore.put(key, value);
        log.debug(
                "ViewMetadata stored tableId={} entryKey={}",
                metadata.tableId(),
                metadata.entryKey());
    }

    public Optional<ViewMetadata> get(String tableId, String entryKey) throws IOException {
        byte[] key = buildKey(tableId, entryKey);
        Optional<byte[]> value = metastore.get(key);
        if (value.isEmpty()) return Optional.empty();
        return Optional.of(MAPPER.readValue(value.get(), ViewMetadata.class));
    }

    public boolean update(String tableId, String entryKey, ViewMetadata metadata)
            throws IOException {
        byte[] key = buildKey(tableId, entryKey);
        byte[] value = MAPPER.writeValueAsBytes(metadata);
        metastore.put(key, value);
        return true;
    }

    public List<ViewMetadata> scanByTable(String tableId, int limit) throws IOException {
        byte[] prefix = buildTablePrefix(tableId);
        List<KeyValue> kvs = metastore.scanAll(prefix);
        List<ViewMetadata> results = new ArrayList<>(Math.min(kvs.size(), limit));
        for (KeyValue kv : kvs) {
            if (results.size() >= limit) break;
            try {
                results.add(MAPPER.readValue(kv.value(), ViewMetadata.class));
            } catch (Exception e) {
                log.warn(
                        "Failed to deserialize ViewMetadata for key={}: {}",
                        new String(kv.key(), StandardCharsets.UTF_8),
                        e.getMessage());
            }
        }
        return results;
    }

    public boolean compareAndSet(
            String tableId, String entryKey, ViewMetadata expected, ViewMetadata newValue)
            throws IOException {
        byte[] key = buildKey(tableId, entryKey);
        byte[] newVal = MAPPER.writeValueAsBytes(newValue);
        List<Precondition> preconditions = new ArrayList<>();
        if (expected == null) {
            preconditions.add(new Precondition.NotExists(key));
        } else {
            byte[] expectedVal = MAPPER.writeValueAsBytes(expected);
            preconditions.add(new Precondition.ValueEquals(key, expectedVal));
        }
        boolean success =
                metastore.compareAndSwap(preconditions, List.of(new Mutation.Put(key, newVal)));
        if (success) {
            log.debug("ViewMetadata CAS succeeded tableId={} entryKey={}", tableId, entryKey);
        }
        return success;
    }

    public void delete(String tableId, String entryKey) throws IOException {
        byte[] key = buildKey(tableId, entryKey);
        metastore.delete(key);
    }

    private byte[] buildKey(String tableId, String entryKey) {
        byte[] prefixBytes = PREFIX.getBytes(StandardCharsets.UTF_8);
        byte[] tableBytes = tableId.getBytes(StandardCharsets.UTF_8);
        byte[] entryBytes = entryKey.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefixBytes.length + 1 + tableBytes.length + 1 + entryBytes.length];
        int pos = 0;
        System.arraycopy(prefixBytes, 0, key, pos, prefixBytes.length);
        pos += prefixBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(tableBytes, 0, key, pos, tableBytes.length);
        pos += tableBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(entryBytes, 0, key, pos, entryBytes.length);
        return key;
    }

    private byte[] buildTablePrefix(String tableId) {
        byte[] prefixBytes = PREFIX.getBytes(StandardCharsets.UTF_8);
        byte[] tableBytes = tableId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefixBytes.length + 1 + tableBytes.length + 1];
        int pos = 0;
        System.arraycopy(prefixBytes, 0, key, pos, prefixBytes.length);
        pos += prefixBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(tableBytes, 0, key, pos, tableBytes.length);
        pos += tableBytes.length;
        key[pos++] = SEPARATOR;
        return key;
    }
}
