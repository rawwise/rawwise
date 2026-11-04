package io.github.rawwise.governance.auth;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultProvenanceStore implements ProvenanceStore {

    private static final String PREFIX_EVENT = "provenance:event:";
    private static final String PREFIX_BY_PRINCIPAL = "provenance:principal:";
    private static final String PREFIX_BY_TARGET = "provenance:target:";
    private static final String PREFIX_BY_ACTION = "provenance:action:";
    private static final int DEFAULT_QUERY_LIMIT = 1000;

    private final Metastore metastore;
    private final int queryLimit;

    public DefaultProvenanceStore(Metastore metastore) {
        this(metastore, DEFAULT_QUERY_LIMIT);
    }

    public DefaultProvenanceStore(Metastore metastore, int queryLimit) {
        this.metastore = metastore;
        this.queryLimit = queryLimit;
    }

    @Override
    public void record(ProvenanceEvent event) throws IOException {
        byte[] value = AuthJson.MAPPER.writeValueAsBytes(event);
        String tsKey = padTimestamp(event.timestamp());
        byte[] eventKey =
                (PREFIX_EVENT + tsKey + ":" + encode(event.eventId()))
                        .getBytes(StandardCharsets.UTF_8);

        List<Precondition> preconditions = List.of(new Precondition.NotExists(eventKey));
        List<Mutation> mutations =
                List.of(
                        new Mutation.Put(eventKey, value),
                        new Mutation.Put(
                                (PREFIX_BY_PRINCIPAL
                                                + encode(event.principalId())
                                                + ":"
                                                + tsKey
                                                + ":"
                                                + encode(event.eventId()))
                                        .getBytes(StandardCharsets.UTF_8),
                                value),
                        new Mutation.Put(
                                (PREFIX_BY_TARGET
                                                + encode(event.targetType())
                                                + ":"
                                                + encode(event.targetId())
                                                + ":"
                                                + tsKey
                                                + ":"
                                                + encode(event.eventId()))
                                        .getBytes(StandardCharsets.UTF_8),
                                value),
                        new Mutation.Put(
                                (PREFIX_BY_ACTION
                                                + encode(event.action())
                                                + ":"
                                                + tsKey
                                                + ":"
                                                + encode(event.eventId()))
                                        .getBytes(StandardCharsets.UTF_8),
                                value));

        boolean success = metastore.compareAndSwap(preconditions, mutations);
        if (!success) {
            log.warn("Duplicate provenance event ignored: eventId={}", event.eventId());
        }
    }

    @Override
    public List<ProvenanceEvent> queryByPrincipal(String principalId, long fromTs, long toTs)
            throws IOException {
        String prefix = PREFIX_BY_PRINCIPAL + encode(principalId) + ":";
        return scanRange(prefix, fromTs, toTs, queryLimit);
    }

    @Override
    public List<ProvenanceEvent> queryByTarget(
            String targetType, String targetId, long fromTs, long toTs) throws IOException {
        String prefix = PREFIX_BY_TARGET + encode(targetType) + ":" + encode(targetId) + ":";
        return scanRange(prefix, fromTs, toTs, queryLimit);
    }

    @Override
    public List<ProvenanceEvent> queryByAction(String action, long fromTs, long toTs)
            throws IOException {
        String prefix = PREFIX_BY_ACTION + encode(action) + ":";
        return scanRange(prefix, fromTs, toTs, queryLimit);
    }

    @Override
    public List<ProvenanceEvent> queryByTimeRange(long fromTs, long toTs) throws IOException {
        String prefix = PREFIX_EVENT;
        return scanRange(prefix, fromTs, toTs, queryLimit);
    }

    @Override
    public List<ProvenanceEvent> queryByPrincipal(
            String principalId, long fromTs, long toTs, int limit) throws IOException {
        String prefix = PREFIX_BY_PRINCIPAL + encode(principalId) + ":";
        return scanRange(prefix, fromTs, toTs, limit);
    }

    @Override
    public List<ProvenanceEvent> queryByTarget(
            String targetType, String targetId, long fromTs, long toTs, int limit)
            throws IOException {
        String prefix = PREFIX_BY_TARGET + encode(targetType) + ":" + encode(targetId) + ":";
        return scanRange(prefix, fromTs, toTs, limit);
    }

    @Override
    public List<ProvenanceEvent> queryByAction(String action, long fromTs, long toTs, int limit)
            throws IOException {
        String prefix = PREFIX_BY_ACTION + encode(action) + ":";
        return scanRange(prefix, fromTs, toTs, limit);
    }

    @Override
    public List<ProvenanceEvent> queryByTimeRange(long fromTs, long toTs, int limit)
            throws IOException {
        String prefix = PREFIX_EVENT;
        return scanRange(prefix, fromTs, toTs, limit);
    }

    private List<ProvenanceEvent> scanRange(String prefix, long fromTs, long toTs, int limit)
            throws IOException {
        byte[] startKey = (prefix + padTimestamp(fromTs)).getBytes(StandardCharsets.UTF_8);
        byte[] endKey = (prefix + padTimestamp(toTs + 1)).getBytes(StandardCharsets.UTF_8);

        List<ProvenanceEvent> results = new ArrayList<>();
        try (var iter = metastore.scanRange(startKey, endKey, limit)) {
            while (iter.hasNext()) {
                var kv = iter.next();
                results.add(AuthJson.MAPPER.readValue(kv.value(), ProvenanceEvent.class));
            }
        }
        return results;
    }

    private static String padTimestamp(long ts) {
        return String.format("%019d", ts);
    }

    private static String encode(String raw) {
        return URLEncoder.encode(raw, StandardCharsets.UTF_8);
    }
}
