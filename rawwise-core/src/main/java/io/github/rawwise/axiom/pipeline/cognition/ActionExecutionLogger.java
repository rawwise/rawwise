package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.Action;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ActionExecutionLogger {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new Jdk8Module())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static final String PREFIX_OPEVT = "action:opevt:";
    private static final String PREFIX_EXECLOG = "action:execlog:";

    private final KnowledgeStore knowledgeStore;
    private final String defaultAgentId;
    private final String executorId;

    private final ThreadLocal<List<OperationEvent>> currentEvents =
            ThreadLocal.withInitial(ArrayList::new);
    private final ThreadLocal<String> currentActionId = new ThreadLocal<>();
    private final ThreadLocal<String> currentPlanId = new ThreadLocal<>();
    private final ThreadLocal<String> currentAgentId = new ThreadLocal<>();

    public ActionExecutionLogger(
            KnowledgeStore knowledgeStore, String defaultAgentId, String executorId) {
        this.knowledgeStore = knowledgeStore;
        this.defaultAgentId = defaultAgentId != null ? defaultAgentId : "unknown";
        this.executorId = executorId != null ? executorId : "unknown";
    }

    public void logStart(
            String actionId, String planId, String agentId, Action.ActionType actionType) {
        currentEvents.set(new ArrayList<>());
        currentActionId.set(actionId);
        currentPlanId.set(planId);
        currentAgentId.set(agentId != null ? agentId : defaultAgentId);
    }

    public void record(OperationEvent.OperationType operation, String targetKey, String detail) {
        List<OperationEvent> events = currentEvents.get();
        String actionId = currentActionId.get();
        if (actionId == null) return;
        events.add(
                new OperationEvent(
                        "opevt:" + UUID.randomUUID(),
                        actionId,
                        currentPlanId.get(),
                        currentAgentId.get(),
                        operation,
                        targetKey,
                        null,
                        detail,
                        System.currentTimeMillis()));
    }

    public void logComplete(
            String actionId,
            String planId,
            String agentId,
            Action.ActionType actionType,
            long startedAt,
            String output,
            boolean success,
            String errorMessage) {
        try {
            List<OperationEvent> events = currentEvents.get();
            String effectiveAgentId = agentId != null ? agentId : defaultAgentId;

            persistOperationEvents(events);

            List<String> readData =
                    events.stream()
                            .filter(e -> e.operation() == OperationEvent.OperationType.READ)
                            .map(OperationEvent::targetKey)
                            .toList();
            List<String> modifiedData =
                    events.stream()
                            .filter(e -> e.operation() == OperationEvent.OperationType.WRITE)
                            .map(OperationEvent::targetKey)
                            .toList();
            List<String> lockResources =
                    events.stream()
                            .filter(
                                    e ->
                                            e.operation() == OperationEvent.OperationType.LOCK
                                                    || e.operation()
                                                            == OperationEvent.OperationType.UNLOCK)
                            .map(OperationEvent::targetKey)
                            .toList();

            ActionExecutionLog execLog =
                    new ActionExecutionLog(
                            "execlog:" + UUID.randomUUID(),
                            effectiveAgentId,
                            executorId,
                            actionId,
                            planId,
                            actionType,
                            startedAt,
                            System.currentTimeMillis(),
                            readData,
                            modifiedData,
                            lockResources,
                            output,
                            success,
                            errorMessage);
            persistExecutionLog(execLog);
        } finally {
            currentEvents.remove();
            currentActionId.remove();
            currentPlanId.remove();
            currentAgentId.remove();
        }
    }

    private void persistOperationEvents(List<OperationEvent> events) {
        for (OperationEvent evt : events) {
            try {
                String key =
                        PREFIX_OPEVT + evt.actionId() + ":" + evt.timestamp() + ":" + evt.eventId();
                byte[] value = MAPPER.writeValueAsBytes(evt);
                knowledgeStore.put(key.getBytes(StandardCharsets.UTF_8), value);
            } catch (IOException e) {
                log.warn("Failed to persist OperationEvent eventId={}", evt.eventId(), e);
            }
        }
    }

    private void persistExecutionLog(ActionExecutionLog execLog) {
        try {
            String key = PREFIX_EXECLOG + execLog.actionId() + ":" + execLog.startedAt();
            byte[] value = MAPPER.writeValueAsBytes(execLog);
            knowledgeStore.put(key.getBytes(StandardCharsets.UTF_8), value);
        } catch (IOException e) {
            log.warn("Failed to persist ActionExecutionLog logId={}", execLog.logId(), e);
        }
    }

    public List<OperationEvent> queryOperationEvents(
            String agentId, long fromTs, long toTs, int limit) {
        if (limit <= 0) limit = 200;
        List<OperationEvent> results = new ArrayList<>();
        try {
            byte[] prefix = PREFIX_OPEVT.getBytes(StandardCharsets.UTF_8);
            try (var it = knowledgeStore.scan(prefix, limit * 4)) {
                while (it.hasNext() && results.size() < limit) {
                    var kv = it.next();
                    OperationEvent evt = MAPPER.readValue(kv.value(), OperationEvent.class);
                    if (evt.timestamp() < fromTs || evt.timestamp() > toTs) continue;
                    if (agentId != null && !agentId.isBlank() && !agentId.equals(evt.agentId()))
                        continue;
                    results.add(evt);
                }
            }
        } catch (IOException e) {
            log.warn("queryOperationEvents failed for agentId={}", agentId, e);
        }
        results.sort((a, b) -> Long.compare(a.timestamp(), b.timestamp()));
        return results;
    }

    public List<ActionExecutionLog> queryExecutionLogs(
            String agentId, long fromTs, long toTs, int limit) {
        if (limit <= 0) limit = 100;
        List<ActionExecutionLog> results = new ArrayList<>();
        try {
            byte[] prefix = PREFIX_EXECLOG.getBytes(StandardCharsets.UTF_8);
            try (var it = knowledgeStore.scan(prefix, limit * 4)) {
                while (it.hasNext() && results.size() < limit) {
                    var kv = it.next();
                    ActionExecutionLog logEntry =
                            MAPPER.readValue(kv.value(), ActionExecutionLog.class);
                    if (logEntry.startedAt() < fromTs || logEntry.startedAt() > toTs) continue;
                    if (agentId != null
                            && !agentId.isBlank()
                            && !agentId.equals(logEntry.agentId())) continue;
                    results.add(logEntry);
                }
            }
        } catch (IOException e) {
            log.warn("queryExecutionLogs failed for agentId={}", agentId, e);
        }
        results.sort((a, b) -> Long.compare(a.startedAt(), b.startedAt()));
        return results;
    }

    public List<ActionExecutionLog> queryExecutionLogsByAction(String actionId) {
        List<ActionExecutionLog> results = new ArrayList<>();
        if (actionId == null || actionId.isBlank()) return results;
        try {
            byte[] prefix = (PREFIX_EXECLOG + actionId + ":").getBytes(StandardCharsets.UTF_8);
            try (var it = knowledgeStore.scan(prefix, 100)) {
                while (it.hasNext()) {
                    var kv = it.next();
                    results.add(MAPPER.readValue(kv.value(), ActionExecutionLog.class));
                }
            }
        } catch (IOException e) {
            log.warn("queryExecutionLogsByAction failed for actionId={}", actionId, e);
        }
        return results;
    }
}
