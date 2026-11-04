package io.github.rawwise.axiom.pipeline.state;

import io.github.rawwise.axiom.memory.AxiomClient;
import io.github.rawwise.governance.context.ContextJson;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AxiomStateEngine implements StateEngine {

    public static final String TRANSITION_LOG_CONFIG_KEY = "rawwise.state.transition-log.enabled";

    static final String TABLE_ID = "agent_state";
    static final String LOG_TABLE_ID = "agent_state_log";

    private final AxiomClient axiomClient;
    private final StateSpaceDefinitionStore defStore;
    private final StateEntryMetadataStore entryMetaStore;
    private final StateContract contract;

    public AxiomStateEngine(AxiomClient axiomClient, Metastore metastore) {
        this(axiomClient, metastore, new DefaultStateContract());
    }

    public AxiomStateEngine(AxiomClient axiomClient, Metastore metastore, StateContract contract) {
        this.axiomClient = axiomClient;
        this.defStore = new StateSpaceDefinitionStore(metastore);
        this.entryMetaStore = new StateEntryMetadataStore(metastore);
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
        boolean success = defStore.create(space);
        if (!success) throw new IOException("StateSpace already exists: " + spaceId);
        log.info("Created StateSpace spaceId={} agentId={} name={}", spaceId, agentId, name);
        return space;
    }

    @Override
    public Optional<StateSpace> getStateSpace(String spaceId) throws IOException {
        return defStore.get(spaceId);
    }

    @Override
    public List<StateSpace> listStateSpacesByAgent(String agentId) throws IOException {
        return defStore.listByAgent(agentId);
    }

    @Override
    public StateEntry read(String spaceId, String key) throws IOException {
        Optional<StateEntryMetadataStore.StateEntryMeta> meta = entryMetaStore.get(spaceId, key);
        if (meta.isEmpty()) return null;
        Optional<Map<String, Object>> entry =
                axiomClient.getEntry(TABLE_ID, meta.get().latestEntryKey());
        if (entry.isEmpty()) return null;
        return ContextJson.MAPPER.readValue(
                String.valueOf(entry.get().get("body")), StateEntry.class);
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
                defStore.get(spaceId)
                        .orElseThrow(() -> new IOException("StateSpace not found: " + spaceId));
        if (!space.states().contains(stateName)) {
            throw new IOException("Invalid state: " + stateName + " for StateSpace: " + spaceId);
        }

        Optional<StateEntryMetadataStore.StateEntryMeta> currentMeta =
                entryMetaStore.get(spaceId, key);
        long currentVersion =
                currentMeta.map(StateEntryMetadataStore.StateEntryMeta::latestVersion).orElse(0L);
        long newVersion = currentVersion + 1;

        long now = System.currentTimeMillis();
        StateEntry entry = new StateEntry(key, value, stateName, newVersion, metadata, now, now);

        String entryKey = "state:" + spaceId + ":" + key + ":v" + newVersion;
        Map<String, String> props = new LinkedHashMap<>();
        props.put("space_id", spaceId);
        props.put("agent_id", space.agentId());
        props.put("state_key", key);
        props.put("state_name", stateName);
        props.put("version", String.valueOf(newVersion));
        props.put("entry_type", "state_entry");
        String body = ContextJson.writeValueAsString(entry);
        axiomClient.ingest(TABLE_ID, entryKey, body, props);

        StateEntryMetadataStore.StateEntryMeta expectedMeta = currentMeta.orElse(null);
        StateEntryMetadataStore.StateEntryMeta newMeta =
                new StateEntryMetadataStore.StateEntryMeta(
                        spaceId, key, newVersion, entryKey, stateName, now);
        boolean casSuccess = entryMetaStore.compareAndSet(spaceId, key, expectedMeta, newMeta);

        if (!casSuccess) {
            throw new IOException(
                    "Failed to write state entry: " + spaceId + "/" + key + " (CAS conflict)");
        }
        log.debug(
                "Wrote state entry spaceId={} key={} state={} version={}",
                spaceId,
                key,
                stateName,
                newVersion);
        return entry;
    }

    @Override
    public StateEntry transition(
            String spaceId, String key, String toState, Map<String, String> metadata)
            throws IOException {
        StateSpace space =
                defStore.get(spaceId)
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
                "State transition spaceId={} key={} {}->{} version={}",
                spaceId,
                key,
                existing.stateName(),
                toState,
                result.version());

        if (Boolean.parseBoolean(System.getProperty(TRANSITION_LOG_CONFIG_KEY, "false"))) {
            logTransition(space, key, existing.stateName(), toState, result.version(), metadata);
        }

        return result;
    }

    private void logTransition(
            StateSpace space,
            String key,
            String fromState,
            String toState,
            long version,
            Map<String, String> metadata) {
        try {
            String logKey = "statelog:" + space.spaceId() + ":" + key + ":v" + version;
            String logBody =
                    ContextJson.writeValueAsString(
                            Map.of(
                                    "fromState", fromState,
                                    "toState", toState,
                                    "version", version,
                                    "timestamp", System.currentTimeMillis()));
            Map<String, String> logProps = new LinkedHashMap<>();
            logProps.put("space_id", space.spaceId());
            logProps.put("agent_id", space.agentId());
            logProps.put("state_key", key);
            logProps.put("action_type", "NOTIFY");
            logProps.put("from_state", fromState);
            logProps.put("to_state", toState);
            logProps.put("entry_type", "state_transition_log");
            if (metadata != null) {
                logProps.putAll(metadata);
            }
            axiomClient.ingest(LOG_TABLE_ID, logKey, logBody, logProps);
            log.debug(
                    "Transition log ingested spaceId={} key={} {}->{}",
                    space.spaceId(),
                    key,
                    fromState,
                    toState);
        } catch (Exception e) {
            log.warn(
                    "Failed to log transition spaceId={} key={} {}->{}: {}",
                    space.spaceId(),
                    key,
                    fromState,
                    toState,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public List<StateEntry> scan(String spaceId, String prefix, int limit) throws IOException {
        List<StateEntryMetadataStore.StateEntryMeta> metas =
                entryMetaStore.scan(spaceId, prefix, limit);
        List<StateEntry> entries = new ArrayList<>();
        for (StateEntryMetadataStore.StateEntryMeta meta : metas) {
            Optional<Map<String, Object>> entry =
                    axiomClient.getEntry(TABLE_ID, meta.latestEntryKey());
            if (entry.isPresent()) {
                entries.add(
                        ContextJson.MAPPER.readValue(
                                String.valueOf(entry.get().get("body")), StateEntry.class));
            }
        }
        return entries;
    }

    @Override
    public boolean deleteStateSpace(String spaceId) throws IOException {
        StateSpace space = defStore.get(spaceId).orElse(null);
        if (space == null) return false;
        List<StateEntryMetadataStore.StateEntryMeta> metas =
                entryMetaStore.scan(spaceId, "", Integer.MAX_VALUE);
        for (StateEntryMetadataStore.StateEntryMeta meta : metas) {
            entryMetaStore.delete(spaceId, meta.key());
        }
        defStore.delete(spaceId);
        log.info(
                "Deleted StateSpace spaceId={} agentId={} entriesRemoved={}",
                spaceId,
                space.agentId(),
                metas.size());
        return true;
    }
}
