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
public class StateSpaceDefinitionStore {

    private static final String PREFIX = "statespace";
    private static final String IDX_PREFIX = "statespaceidx";
    private static final byte SEPARATOR = 0x00;

    private final Metastore metastore;

    public StateSpaceDefinitionStore(Metastore metastore) {
        this.metastore = metastore;
    }

    public boolean create(StateSpace space) throws IOException {
        byte[] key = buildKey(PREFIX, space.spaceId());
        byte[] value = ContextJson.writeValueAsBytes(space);
        byte[] idxKey = buildIdxKey(space.agentId(), space.spaceId());
        boolean success =
                metastore.compareAndSwap(
                        List.of(new Precondition.NotExists(key)),
                        List.of(
                                new Mutation.Put(key, value),
                                new Mutation.Put(
                                        idxKey, space.spaceId().getBytes(StandardCharsets.UTF_8))));
        if (success) {
            log.debug("StateSpace created spaceId={} agentId={}", space.spaceId(), space.agentId());
        }
        return success;
    }

    public Optional<StateSpace> get(String spaceId) throws IOException {
        byte[] key = buildKey(PREFIX, spaceId);
        Optional<byte[]> value = metastore.get(key);
        if (value.isEmpty()) return Optional.empty();
        return Optional.of(ContextJson.readValue(value.get(), StateSpace.class));
    }

    public List<StateSpace> listByAgent(String agentId) throws IOException {
        byte[] prefix = buildIdxPrefix(agentId);
        List<KeyValue> kvs = metastore.scanAll(prefix);
        List<StateSpace> spaces = new ArrayList<>(kvs.size());
        for (KeyValue kv : kvs) {
            String spaceId = new String(kv.value(), StandardCharsets.UTF_8);
            Optional<byte[]> spaceBytes = metastore.get(buildKey(PREFIX, spaceId));
            if (spaceBytes.isPresent()) {
                spaces.add(ContextJson.readValue(spaceBytes.get(), StateSpace.class));
            }
        }
        return spaces;
    }

    public boolean delete(String spaceId) throws IOException {
        Optional<StateSpace> spaceOpt = get(spaceId);
        if (spaceOpt.isEmpty()) return false;
        StateSpace space = spaceOpt.get();
        byte[] key = buildKey(PREFIX, spaceId);
        byte[] idxKey = buildIdxKey(space.agentId(), spaceId);
        metastore.batchWrite(List.of(new Mutation.Delete(key), new Mutation.Delete(idxKey)));
        log.debug("StateSpace deleted spaceId={} agentId={}", spaceId, space.agentId());
        return true;
    }

    private static byte[] buildKey(String prefix, String id) {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefixBytes.length + 1 + idBytes.length];
        int pos = 0;
        System.arraycopy(prefixBytes, 0, key, pos, prefixBytes.length);
        pos += prefixBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(idBytes, 0, key, pos, idBytes.length);
        return key;
    }

    private static byte[] buildIdxKey(String agentId, String spaceId) {
        byte[] prefixBytes = IDX_PREFIX.getBytes(StandardCharsets.UTF_8);
        byte[] agentBytes = agentId.getBytes(StandardCharsets.UTF_8);
        byte[] spaceBytes = spaceId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefixBytes.length + 1 + agentBytes.length + 1 + spaceBytes.length];
        int pos = 0;
        System.arraycopy(prefixBytes, 0, key, pos, prefixBytes.length);
        pos += prefixBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(agentBytes, 0, key, pos, agentBytes.length);
        pos += agentBytes.length;
        key[pos++] = SEPARATOR;
        System.arraycopy(spaceBytes, 0, key, pos, spaceBytes.length);
        return key;
    }

    private static byte[] buildIdxPrefix(String agentId) {
        byte[] prefixBytes = IDX_PREFIX.getBytes(StandardCharsets.UTF_8);
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
