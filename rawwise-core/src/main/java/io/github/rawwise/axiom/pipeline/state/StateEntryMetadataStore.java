package io.github.rawwise.axiom.pipeline.state;

import io.github.rawwise.governance.context.ContextJson;
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
public class StateEntryMetadataStore {

    private static final String PREFIX = "statelatest";
    private static final byte SEPARATOR = 0x00;

    private final Metastore metastore;

    public StateEntryMetadataStore(Metastore metastore) {
        this.metastore = metastore;
    }

    public record StateEntryMeta(
            String spaceId,
            String key,
            long latestVersion,
            String latestEntryKey,
            String stateName,
            long updatedAt) {}

    public Optional<StateEntryMeta> get(String spaceId, String key) throws IOException {
        byte[] metaKey = buildKey(spaceId, key);
        Optional<byte[]> value = metastore.get(metaKey);
        if (value.isEmpty()) return Optional.empty();
        return Optional.of(ContextJson.readValue(value.get(), StateEntryMeta.class));
    }

    public boolean compareAndSet(
            String spaceId, String key, StateEntryMeta expected, StateEntryMeta newValue)
            throws IOException {
        byte[] metaKey = buildKey(spaceId, key);
        byte[] newValueBytes = ContextJson.writeValueAsBytes(newValue);
        List<Precondition> preconditions = new ArrayList<>();
        if (expected == null) {
            preconditions.add(new Precondition.NotExists(metaKey));
        } else {
            byte[] expectedBytes = ContextJson.writeValueAsBytes(expected);
            preconditions.add(new Precondition.ValueEquals(metaKey, expectedBytes));
        }
        boolean success =
                metastore.compareAndSwap(
                        preconditions, List.of(new Mutation.Put(metaKey, newValueBytes)));
        if (success) {
            log.debug(
                    "StateEntry CAS succeeded spaceId={} key={} version={}",
                    spaceId,
                    key,
                    newValue.latestVersion());
        }
        return success;
    }

    public List<StateEntryMeta> scan(String spaceId, String prefix, int limit) throws IOException {
        byte[] scanPrefix = buildScanPrefix(spaceId, prefix);
        List<KeyValue> kvs = metastore.scanAll(scanPrefix);
        List<StateEntryMeta> metas = new ArrayList<>(Math.min(kvs.size(), limit));
        for (KeyValue kv : kvs) {
            if (metas.size() >= limit) break;
            try {
                metas.add(ContextJson.readValue(kv.value(), StateEntryMeta.class));
            } catch (Exception e) {
                log.warn(
                        "Failed to deserialize StateEntryMeta for key={}: {}",
                        new String(kv.key(), StandardCharsets.UTF_8),
                        e.getMessage());
            }
        }
        return metas;
    }

    public void delete(String spaceId, String key) throws IOException {
        byte[] metaKey = buildKey(spaceId, key);
        metastore.delete(metaKey);
    }

    private static byte[] buildKey(String spaceId, String key) {
        byte[] prefixBytes = PREFIX.getBytes(StandardCharsets.UTF_8);
        byte[] spaceBytes = spaceId.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[prefixBytes.length + 1 + spaceBytes.length + 1 + keyBytes.length];
        int pos = 0;
        System.arraycopy(prefixBytes, 0, result, pos, prefixBytes.length);
        pos += prefixBytes.length;
        result[pos++] = SEPARATOR;
        System.arraycopy(spaceBytes, 0, result, pos, spaceBytes.length);
        pos += spaceBytes.length;
        result[pos++] = SEPARATOR;
        System.arraycopy(keyBytes, 0, result, pos, keyBytes.length);
        return result;
    }

    private static byte[] buildScanPrefix(String spaceId, String prefix) {
        byte[] prefixBytes = PREFIX.getBytes(StandardCharsets.UTF_8);
        byte[] spaceBytes = spaceId.getBytes(StandardCharsets.UTF_8);
        byte[] prefixParamBytes = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] result =
                new byte[prefixBytes.length + 1 + spaceBytes.length + 1 + prefixParamBytes.length];
        int pos = 0;
        System.arraycopy(prefixBytes, 0, result, pos, prefixBytes.length);
        pos += prefixBytes.length;
        result[pos++] = SEPARATOR;
        System.arraycopy(spaceBytes, 0, result, pos, spaceBytes.length);
        pos += spaceBytes.length;
        result[pos++] = SEPARATOR;
        System.arraycopy(prefixParamBytes, 0, result, pos, prefixParamBytes.length);
        return result;
    }
}
