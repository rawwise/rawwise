package io.github.rawwise.axiom.pipeline.state;

import io.github.rawwise.governance.context.ContextJson;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultStateEngine implements StateEngine {

    private static final String PREFIX_SPACE = "state:space:";
    private static final String PREFIX_SPACE_IDX = "state:space:idx:agent:";
    private static final String PREFIX_ENTRY = "state:entry:";

    private final Metastore metastore;
    private final StateContract contract;

    public DefaultStateEngine(Metastore metastore) {
        this(metastore, new DefaultStateContract());
    }

    public DefaultStateEngine(Metastore metastore, StateContract contract) {
        this.metastore = metastore;
        this.contract = contract;
    }

    @Override
    public StateSpace createStateSpace(
            String agentId,
            String name,
            String initialState,
            List<String> states,
            List<StateTransition> transitions,
            Map<String, String> metadata)
            throws IOException {
        String spaceId = "ss-" + UUID.randomUUID();
        long now = System.currentTimeMillis();
        StateSpace space =
                new StateSpace(
                        spaceId,
                        agentId,
                        name,
                        initialState,
                        states,
                        transitions,
                        metadata,
                        now,
                        now);
        byte[] key = (PREFIX_SPACE + spaceId).getBytes(StandardCharsets.UTF_8);
        byte[] value = ContextJson.writeValueAsBytes(space);
        byte[] idxKey =
                (PREFIX_SPACE_IDX + agentId + ":" + spaceId).getBytes(StandardCharsets.UTF_8);
        boolean success =
                metastore.compareAndSwap(
                        List.of(new Precondition.NotExists(key)),
                        List.of(
                                new Mutation.Put(key, value),
                                new Mutation.Put(
                                        idxKey, spaceId.getBytes(StandardCharsets.UTF_8))));
        if (!success) throw new IOException("StateSpace already exists: " + spaceId);
        log.info("Created StateSpace spaceId={} agentId={} name={}", spaceId, agentId, name);
        return space;
    }

    @Override
    public Optional<StateSpace> getStateSpace(String spaceId) throws IOException {
        byte[] key = (PREFIX_SPACE + spaceId).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> value = metastore.get(key);
        if (value.isEmpty()) return Optional.empty();
        return Optional.of(ContextJson.readValue(value.get(), StateSpace.class));
    }

    @Override
    public List<StateSpace> listStateSpacesByAgent(String agentId) throws IOException {
        byte[] prefix = (PREFIX_SPACE_IDX + agentId + ":").getBytes(StandardCharsets.UTF_8);
        List<KeyValue> kvs = metastore.scanAll(prefix);
        List<StateSpace> spaces = new ArrayList<>(kvs.size());
        for (KeyValue kv : kvs) {
            String spaceId = new String(kv.value(), StandardCharsets.UTF_8);
            Optional<byte[]> spaceBytes =
                    metastore.get((PREFIX_SPACE + spaceId).getBytes(StandardCharsets.UTF_8));
            if (spaceBytes.isPresent()) {
                spaces.add(ContextJson.readValue(spaceBytes.get(), StateSpace.class));
            }
        }
        return spaces;
    }

    @Override
    public StateEntry read(String spaceId, String key) throws IOException {
        byte[] entryKey = (PREFIX_ENTRY + spaceId + ":" + key).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> value = metastore.get(entryKey);
        if (value.isEmpty()) return null;
        return ContextJson.readValue(value.get(), StateEntry.class);
    }

    @Override
    public StateEntry write(
            String spaceId,
            String key,
            String value,
            String stateName,
            Map<String, String> metadata)
            throws IOException {
        StateSpace space =
                getStateSpace(spaceId)
                        .orElseThrow(() -> new IOException("StateSpace not found: " + spaceId));
        if (!space.states().contains(stateName)) {
            throw new IOException("Invalid state: " + stateName + " for StateSpace: " + spaceId);
        }
        byte[] entryKey = (PREFIX_ENTRY + spaceId + ":" + key).getBytes(StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();
        StateEntry existing = read(spaceId, key);
        long version = existing != null ? existing.version() + 1 : 1;
        StateEntry entry = new StateEntry(key, value, stateName, version, metadata, now, now);
        boolean success;
        if (existing != null) {
            success =
                    metastore.compareAndSwap(
                            List.of(
                                    new Precondition.ValueEquals(
                                            entryKey, ContextJson.writeValueAsBytes(existing))),
                            List.of(
                                    new Mutation.Put(
                                            entryKey, ContextJson.writeValueAsBytes(entry))));
        } else {
            success =
                    metastore.compareAndSwap(
                            List.of(new Precondition.NotExists(entryKey)),
                            List.of(
                                    new Mutation.Put(
                                            entryKey, ContextJson.writeValueAsBytes(entry))));
        }
        if (!success) {
            throw new IOException(
                    "Failed to write state entry: " + spaceId + "/" + key + " (CAS conflict)");
        }
        log.debug(
                "Wrote state entry spaceId={} key={} state={} version={}",
                spaceId,
                key,
                stateName,
                version);
        return entry;
    }

    @Override
    public StateEntry transition(
            String spaceId, String key, String toState, Map<String, String> metadata)
            throws IOException {
        StateSpace space =
                getStateSpace(spaceId)
                        .orElseThrow(() -> new IOException("StateSpace not found: " + spaceId));
        StateEntry existing = read(spaceId, key);
        if (existing == null) throw new IOException("Entry not found: " + key);
        Optional<ContractViolation> violation = contract.validate(space, existing, toState);
        if (violation.isPresent()) {
            log.warn(
                    "Contract violation spaceId={} key={} from={} to={} reason={}",
                    spaceId,
                    key,
                    existing.stateName(),
                    toState,
                    violation.get().reason());
            throw new IOException("Contract violation: " + violation.get().reason());
        }
        StateEntry result = write(spaceId, key, existing.value(), toState, metadata);
        log.info(
                "State transition spaceId={} key={} {}→{} version={}",
                spaceId,
                key,
                existing.stateName(),
                toState,
                result.version());
        return result;
    }

    @Override
    public List<StateEntry> scan(String spaceId, String prefix, int limit) throws IOException {
        byte[] scanPrefix =
                (PREFIX_ENTRY + spaceId + ":" + prefix).getBytes(StandardCharsets.UTF_8);
        List<KeyValue> kvs = metastore.scanAll(scanPrefix);
        List<StateEntry> entries = new ArrayList<>();
        for (KeyValue kv : kvs) {
            entries.add(ContextJson.readValue(kv.value(), StateEntry.class));
            if (entries.size() >= limit) break;
        }
        return entries;
    }

    @Override
    public boolean deleteStateSpace(String spaceId) throws IOException {
        StateSpace space = getStateSpace(spaceId).orElse(null);
        if (space == null) return false;
        byte[] spaceKey = (PREFIX_SPACE + spaceId).getBytes(StandardCharsets.UTF_8);
        byte[] idxKey =
                (PREFIX_SPACE_IDX + space.agentId() + ":" + spaceId)
                        .getBytes(StandardCharsets.UTF_8);
        byte[] entryPrefix = (PREFIX_ENTRY + spaceId + ":").getBytes(StandardCharsets.UTF_8);
        List<KeyValue> entries = metastore.scanAll(entryPrefix);
        List<Mutation> deletes = new ArrayList<>();
        deletes.add(new Mutation.Delete(spaceKey));
        deletes.add(new Mutation.Delete(idxKey));
        for (KeyValue kv : entries) {
            deletes.add(new Mutation.Delete(kv.key()));
        }
        metastore.batchWrite(deletes);
        log.info(
                "Deleted StateSpace spaceId={} agentId={} entriesRemoved={}",
                spaceId,
                space.agentId(),
                entries.size());
        return true;
    }
}
