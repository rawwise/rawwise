package io.github.rawwise.axiom.memory;

import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentIndexStore {

    private static final String PREFIX = "agentidx";
    private static final byte SEPARATOR = 0x00;
    private final Metastore metastore;

    public AgentIndexStore(Metastore metastore) {
        this.metastore = metastore;
    }

    public void index(String agentId, String entryKey) throws IOException {
        byte[] key = buildKey(agentId, entryKey);
        byte[] value = entryKey.getBytes(StandardCharsets.UTF_8);
        metastore.put(key, value);
        log.debug("AgentIndex indexed agentId={} entryKey={}", agentId, entryKey);
    }

    public List<String> scanAgent(String agentId, int limit) throws IOException {
        byte[] prefix = buildAgentPrefix(agentId);
        List<KeyValue> kvs = metastore.scanAll(prefix);
        List<String> results = new ArrayList<>(Math.min(kvs.size(), Math.max(limit, 0)));
        for (KeyValue kv : kvs) {
            if (results.size() >= limit) break;
            results.add(new String(kv.value(), StandardCharsets.UTF_8));
        }
        return results;
    }

    public boolean exists(String agentId, String entryKey) throws IOException {
        byte[] key = buildKey(agentId, entryKey);
        return metastore.get(key).isPresent();
    }

    public void remove(String agentId, String entryKey) throws IOException {
        byte[] key = buildKey(agentId, entryKey);
        metastore.delete(key);
        log.debug("AgentIndex removed agentId={} entryKey={}", agentId, entryKey);
    }

    public Optional<String> get(String agentId, String entryKey) throws IOException {
        byte[] key = buildKey(agentId, entryKey);
        Optional<byte[]> value = metastore.get(key);
        if (value.isEmpty()) return Optional.empty();
        return Optional.of(new String(value.get(), StandardCharsets.UTF_8));
    }

    private byte[] buildKey(String agentId, String entryKey) {
        byte[] prefixBytes = PREFIX.getBytes(StandardCharsets.UTF_8);
        byte[] agentBytes = agentId.getBytes(StandardCharsets.UTF_8);
        byte[] entryBytes = entryKey.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefixBytes.length + 1 + agentBytes.length + 1 + entryBytes.length];
        int pos = 0;
        System.arraycopy(prefixBytes, 0, key, pos, prefixBytes.length);
        pos += prefixBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(agentBytes, 0, key, pos, agentBytes.length);
        pos += agentBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(entryBytes, 0, key, pos, entryBytes.length);
        return key;
    }

    private byte[] buildAgentPrefix(String agentId) {
        byte[] prefixBytes = PREFIX.getBytes(StandardCharsets.UTF_8);
        byte[] agentBytes = agentId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefixBytes.length + 1 + agentBytes.length + 1];
        int pos = 0;
        System.arraycopy(prefixBytes, 0, key, pos, prefixBytes.length);
        pos += prefixBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(agentBytes, 0, key, pos, agentBytes.length);
        pos += agentBytes.length;
        key[pos++] = SEPARATOR;
        return key;
    }
}
