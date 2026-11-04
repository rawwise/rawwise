package io.github.rawwise.governance.context;

import io.github.rawwise.axiom.memory.Depth;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.Closeable;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultContextManager implements ContextManager, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultContextManager.class);

    private static final String PREFIX_SESSION = "ctx:session:";
    private static final String PREFIX_SESSION_ENTRY = ":entry:";
    private static final String PREFIX_SESSION_HIST = ":hist:";
    private static final String PREFIX_KNOWLEDGE = "ctx:knowledge:";
    private static final String PREFIX_KNOWLEDGE_ENTRY = ":entry:";
    private static final String PREFIX_KNOWLEDGE_HIST = ":hist:";
    private static final String PREFIX_KNOWLEDGE_IDX_AGENT = "ctx:knowledge:idx:agent:";
    private static final String PREFIX_KNOWLEDGE_IDX_DOMAIN = "ctx:knowledge:idx:domain:";
    private static final String PREFIX_L0_IDX = "ctx:idx:agent:";

    private static final long DEFAULT_CLEANUP_INTERVAL_MS = 600_000L;

    private final Metastore metastore;
    private final int sessionHistoryVersions;
    private final int knowledgeHistoryVersions;
    private final ScheduledExecutorService cleanupExecutor;

    public DefaultContextManager(Metastore metastore, int sessionHistoryVersions) {
        this(metastore, sessionHistoryVersions, 50);
    }

    public DefaultContextManager(
            Metastore metastore, int sessionHistoryVersions, int knowledgeHistoryVersions) {
        this.metastore = metastore;
        this.sessionHistoryVersions = sessionHistoryVersions;
        this.knowledgeHistoryVersions = knowledgeHistoryVersions;
        this.cleanupExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "context-cleanup");
                            t.setDaemon(true);
                            return t;
                        });
        this.cleanupExecutor.scheduleWithFixedDelay(
                () -> {
                    try {
                        cleanupExpiredSessions();
                    } catch (Exception e) {
                        LOG.warn("Failed to cleanup expired sessions", e);
                    }
                },
                DEFAULT_CLEANUP_INTERVAL_MS,
                DEFAULT_CLEANUP_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static String encode(String raw) {
        return URLEncoder.encode(raw, StandardCharsets.UTF_8);
    }

    private static String decode(String encoded) {
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static String padVersion(int version) {
        return String.format("%010d", version);
    }

    @Override
    public SessionContext createSession(String agentId, String userId, long ttlMs)
            throws IOException {
        String sessionId = "sess-" + UUID.randomUUID();
        long now = System.currentTimeMillis();
        long expiresAt = ttlMs > 0 ? now + ttlMs : 0;
        SessionContext session =
                new SessionContext(
                        sessionId,
                        agentId,
                        userId,
                        now,
                        now,
                        expiresAt,
                        0,
                        SessionContext.SessionStatus.ACTIVE);
        byte[] key = bytes(PREFIX_SESSION + sessionId);
        byte[] value = ContextJson.writeValueAsBytes(session);
        List<Precondition> preconditions = List.of(new Precondition.NotExists(key));
        List<Mutation> mutations = List.of(new Mutation.Put(key, value));
        if (!metastore.compareAndSwap(preconditions, mutations)) {
            throw new IOException("Failed to create session: " + sessionId + " (CAS conflict)");
        }
        return session;
    }

    @Override
    public Optional<SessionContext> getSession(String sessionId) throws IOException {
        byte[] key = bytes(PREFIX_SESSION + sessionId);
        Optional<byte[]> raw = metastore.get(key);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        SessionContext session = ContextJson.readValue(raw.get(), SessionContext.class);
        if (session.status() == SessionContext.SessionStatus.TERMINATED) {
            return Optional.empty();
        }
        if (session.status() == SessionContext.SessionStatus.ACTIVE
                && session.expiresAt() > 0
                && System.currentTimeMillis() > session.expiresAt()) {
            SessionContext expired =
                    new SessionContext(
                            sessionId,
                            session.agentId(),
                            session.userId(),
                            session.createdAt(),
                            session.updatedAt(),
                            session.expiresAt(),
                            session.entryCount(),
                            SessionContext.SessionStatus.EXPIRED);
            byte[] newValue = ContextJson.writeValueAsBytes(expired);
            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(key, raw.get()));
            List<Mutation> mutations = List.of(new Mutation.Put(key, newValue));
            metastore.compareAndSwap(preconditions, mutations);
            return Optional.empty();
        }
        return Optional.of(session);
    }

    @Override
    public SessionContext touchSession(String sessionId) throws IOException {
        byte[] key = bytes(PREFIX_SESSION + sessionId);
        Optional<byte[]> raw = metastore.get(key);
        if (raw.isEmpty()) {
            throw new IOException("Session not found: " + sessionId);
        }
        SessionContext session = ContextJson.readValue(raw.get(), SessionContext.class);
        if (session.status() != SessionContext.SessionStatus.ACTIVE) {
            throw new IOException(
                    "Session not active: " + sessionId + " status=" + session.status());
        }
        long now = System.currentTimeMillis();
        SessionContext updated =
                new SessionContext(
                        sessionId,
                        session.agentId(),
                        session.userId(),
                        session.createdAt(),
                        now,
                        session.expiresAt(),
                        session.entryCount(),
                        SessionContext.SessionStatus.ACTIVE);
        byte[] newValue = ContextJson.writeValueAsBytes(updated);
        List<Precondition> preconditions = List.of(new Precondition.ValueEquals(key, raw.get()));
        List<Mutation> mutations = List.of(new Mutation.Put(key, newValue));
        if (!metastore.compareAndSwap(preconditions, mutations)) {
            throw new IOException("Failed to touch session: " + sessionId + " (CAS conflict)");
        }
        return updated;
    }

    @Override
    public void terminateSession(String sessionId) throws IOException {
        byte[] key = bytes(PREFIX_SESSION + sessionId);
        Optional<byte[]> raw = metastore.get(key);
        if (raw.isEmpty()) {
            throw new IOException("Session not found: " + sessionId);
        }
        SessionContext session = ContextJson.readValue(raw.get(), SessionContext.class);
        if (session.status() == SessionContext.SessionStatus.TERMINATED) {
            return;
        }
        long now = System.currentTimeMillis();
        SessionContext terminated =
                new SessionContext(
                        sessionId,
                        session.agentId(),
                        session.userId(),
                        session.createdAt(),
                        now,
                        session.expiresAt(),
                        session.entryCount(),
                        SessionContext.SessionStatus.TERMINATED);
        byte[] newValue = ContextJson.writeValueAsBytes(terminated);
        List<Precondition> preconditions = List.of(new Precondition.ValueEquals(key, raw.get()));
        List<Mutation> mutations = List.of(new Mutation.Put(key, newValue));
        if (!metastore.compareAndSwap(preconditions, mutations)) {
            throw new IOException("Failed to terminate session: " + sessionId + " (CAS conflict)");
        }
    }

    @Override
    public ContextEntry putSessionEntry(
            String sessionId,
            String key,
            String value,
            String contentType,
            String encoding,
            Map<String, String> metadata)
            throws IOException {
        byte[] sessionKey = bytes(PREFIX_SESSION + sessionId);
        Optional<byte[]> rawSession = metastore.get(sessionKey);
        if (rawSession.isEmpty()) {
            throw new IOException("Session not found: " + sessionId);
        }
        SessionContext session = ContextJson.readValue(rawSession.get(), SessionContext.class);
        if (session.status() != SessionContext.SessionStatus.ACTIVE) {
            throw new IOException("Session not active: " + sessionId);
        }

        long now = System.currentTimeMillis();
        byte[] entryKeyBytes =
                bytes(PREFIX_SESSION + sessionId + PREFIX_SESSION_ENTRY + encode(key));
        Optional<byte[]> rawExisting = metastore.get(entryKeyBytes);

        ContextEntry existingEntry = null;
        int currentVersion = 0;
        int newVersion = 1;
        long createdAt = now;

        if (rawExisting.isPresent()) {
            existingEntry = ContextJson.readValue(rawExisting.get(), ContextEntry.class);
            currentVersion = existingEntry.version();
            newVersion = currentVersion + 1;
            createdAt = existingEntry.createdAt();
        }

        ContextEntry newEntry =
                ContextEntry.builder(key, value)
                        .contentType(contentType != null ? contentType : "text/plain")
                        .encoding(encoding != null ? encoding : "utf-8")
                        .version(newVersion)
                        .metadata(metadata != null ? metadata : Map.of())
                        .createdAt(createdAt)
                        .updatedAt(now)
                        .build();

        byte[] newEntryBytes = ContextJson.writeValueAsBytes(newEntry);

        List<Precondition> preconditions = new ArrayList<>();
        preconditions.add(new Precondition.ValueEquals(sessionKey, rawSession.get()));

        List<Mutation> mutations = new ArrayList<>();

        if (existingEntry != null) {
            preconditions.add(new Precondition.ValueEquals(entryKeyBytes, rawExisting.get()));
            if (newVersion <= sessionHistoryVersions) {
                byte[] histKey =
                        bytes(
                                PREFIX_SESSION
                                        + sessionId
                                        + PREFIX_SESSION_HIST
                                        + encode(key)
                                        + ":"
                                        + padVersion(existingEntry.version()));
                mutations.add(new Mutation.Put(histKey, rawExisting.get()));
            }
        } else {
            preconditions.add(new Precondition.NotExists(entryKeyBytes));
        }

        mutations.add(new Mutation.Put(entryKeyBytes, newEntryBytes));

        int newEntryCount = session.entryCount() + (existingEntry == null ? 1 : 0);
        SessionContext updatedSession =
                new SessionContext(
                        sessionId,
                        session.agentId(),
                        session.userId(),
                        session.createdAt(),
                        now,
                        session.expiresAt(),
                        newEntryCount,
                        SessionContext.SessionStatus.ACTIVE);
        byte[] updatedSessionBytes = ContextJson.writeValueAsBytes(updatedSession);
        mutations.add(new Mutation.Put(sessionKey, updatedSessionBytes));

        byte[] l0IdxKey =
                bytes(
                        PREFIX_L0_IDX
                                + encode(session.agentId())
                                + ":l0:session:"
                                + sessionId
                                + ":"
                                + encode(key));
        mutations.add(new Mutation.Put(l0IdxKey, new byte[0]));

        if (!metastore.compareAndSwap(preconditions, mutations)) {
            throw new IOException(
                    "Failed to put session entry: " + sessionId + "/" + key + " (CAS conflict)");
        }

        return newEntry;
    }

    @Override
    public Optional<ContextEntry> getSessionEntry(String sessionId, String key) throws IOException {
        byte[] sessionKey = bytes(PREFIX_SESSION + sessionId);
        Optional<byte[]> rawSession = metastore.get(sessionKey);
        if (rawSession.isEmpty()) {
            return Optional.empty();
        }
        SessionContext session = ContextJson.readValue(rawSession.get(), SessionContext.class);
        if (session.status() != SessionContext.SessionStatus.ACTIVE) {
            return Optional.empty();
        }
        byte[] entryKey = bytes(PREFIX_SESSION + sessionId + PREFIX_SESSION_ENTRY + encode(key));
        Optional<byte[]> rawEntry = metastore.get(entryKey);
        return rawEntry.map(
                r -> {
                    try {
                        return ContextJson.readValue(r, ContextEntry.class);
                    } catch (IOException e) {
                        LOG.warn("Failed to deserialize session entry: {}/{}", sessionId, key, e);
                        return null;
                    }
                });
    }

    @Override
    public List<ContextEntry> listSessionEntries(String sessionId) throws IOException {
        byte[] sessionKey = bytes(PREFIX_SESSION + sessionId);
        Optional<byte[]> rawSession = metastore.get(sessionKey);
        if (rawSession.isEmpty()) {
            return List.of();
        }
        SessionContext session = ContextJson.readValue(rawSession.get(), SessionContext.class);
        if (session.status() != SessionContext.SessionStatus.ACTIVE) {
            return List.of();
        }
        byte[] prefix = bytes(PREFIX_SESSION + sessionId + PREFIX_SESSION_ENTRY);
        List<ContextEntry> entries = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                try {
                    entries.add(ContextJson.readValue(it.next().value(), ContextEntry.class));
                } catch (IOException e) {
                    LOG.warn("Failed to deserialize session entry during list: {}", sessionId, e);
                }
            }
        }
        return entries;
    }

    @Override
    public boolean removeSessionEntry(String sessionId, String key) throws IOException {
        byte[] sessionKey = bytes(PREFIX_SESSION + sessionId);
        Optional<byte[]> rawSession = metastore.get(sessionKey);
        if (rawSession.isEmpty()) {
            return false;
        }
        SessionContext session = ContextJson.readValue(rawSession.get(), SessionContext.class);
        if (session.status() != SessionContext.SessionStatus.ACTIVE) {
            return false;
        }

        byte[] entryKeyBytes =
                bytes(PREFIX_SESSION + sessionId + PREFIX_SESSION_ENTRY + encode(key));
        Optional<byte[]> rawEntry = metastore.get(entryKeyBytes);
        if (rawEntry.isEmpty()) {
            return false;
        }

        long now = System.currentTimeMillis();
        SessionContext updatedSession =
                new SessionContext(
                        sessionId,
                        session.agentId(),
                        session.userId(),
                        session.createdAt(),
                        now,
                        session.expiresAt(),
                        Math.max(0, session.entryCount() - 1),
                        SessionContext.SessionStatus.ACTIVE);
        byte[] updatedSessionBytes = ContextJson.writeValueAsBytes(updatedSession);

        List<Precondition> preconditions =
                List.of(
                        new Precondition.ValueEquals(sessionKey, rawSession.get()),
                        new Precondition.ValueEquals(entryKeyBytes, rawEntry.get()));
        List<Mutation> mutations =
                List.of(
                        new Mutation.Delete(entryKeyBytes),
                        new Mutation.Put(sessionKey, updatedSessionBytes),
                        new Mutation.Delete(
                                bytes(
                                        PREFIX_L0_IDX
                                                + encode(session.agentId())
                                                + ":l0:session:"
                                                + sessionId
                                                + ":"
                                                + encode(key))));

        if (!metastore.compareAndSwap(preconditions, mutations)) {
            throw new IOException(
                    "Failed to remove session entry: " + sessionId + "/" + key + " (CAS conflict)");
        }

        return true;
    }

    @Override
    public KnowledgeContext createKnowledge(
            String agentId,
            String userId,
            String domain,
            String description,
            KnowledgeContext.KnowledgeScope scope,
            List<String> sharedAgents)
            throws IOException {
        String knowledgeId = "know-" + UUID.randomUUID();
        long now = System.currentTimeMillis();
        Set<String> sharedSet = sharedAgents != null ? Set.copyOf(sharedAgents) : Set.of();
        KnowledgeContext knowledge =
                new KnowledgeContext(
                        knowledgeId,
                        agentId,
                        userId,
                        domain != null ? domain : "",
                        description != null ? description : "",
                        scope != null ? scope : KnowledgeContext.KnowledgeScope.PRIVATE,
                        sharedSet,
                        0,
                        now,
                        now);

        byte[] key = bytes(PREFIX_KNOWLEDGE + knowledgeId);
        byte[] value = ContextJson.writeValueAsBytes(knowledge);
        byte[] domainIdxKey =
                bytes(PREFIX_KNOWLEDGE_IDX_DOMAIN + encode(knowledge.domain()) + ":" + knowledgeId);

        List<Precondition> preconditions = new ArrayList<>();
        preconditions.add(new Precondition.NotExists(key));

        List<Mutation> mutations = new ArrayList<>();
        mutations.add(new Mutation.Put(key, value));
        mutations.add(new Mutation.Put(domainIdxKey, new byte[0]));

        mutations.add(
                new Mutation.Put(
                        bytes(PREFIX_KNOWLEDGE_IDX_AGENT + encode(agentId) + ":" + knowledgeId),
                        new byte[0]));
        for (String sharedAgent : sharedSet) {
            if (!sharedAgent.equals(agentId)) {
                mutations.add(
                        new Mutation.Put(
                                bytes(
                                        PREFIX_KNOWLEDGE_IDX_AGENT
                                                + encode(sharedAgent)
                                                + ":"
                                                + knowledgeId),
                                new byte[0]));
            }
        }

        if (!metastore.compareAndSwap(preconditions, mutations)) {
            throw new IOException("Failed to create knowledge: " + knowledgeId + " (CAS conflict)");
        }

        return knowledge;
    }

    @Override
    public Optional<KnowledgeContext> getKnowledge(String knowledgeId) throws IOException {
        byte[] key = bytes(PREFIX_KNOWLEDGE + knowledgeId);
        Optional<byte[]> raw = metastore.get(key);
        return raw.map(
                r -> {
                    try {
                        return ContextJson.readValue(r, KnowledgeContext.class);
                    } catch (IOException e) {
                        LOG.warn("Failed to deserialize knowledge: {}", knowledgeId, e);
                        return null;
                    }
                });
    }

    @Override
    public List<KnowledgeContext> listKnowledgeByAgent(String agentId) throws IOException {
        byte[] prefix = bytes(PREFIX_KNOWLEDGE_IDX_AGENT + encode(agentId) + ":");
        List<KnowledgeContext> result = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                String fullKey = str(kv.key());
                String knowledgeId = fullKey.substring(fullKey.lastIndexOf(':') + 1);
                getKnowledge(knowledgeId).ifPresent(result::add);
            }
        }
        return result;
    }

    @Override
    public List<KnowledgeContext> listKnowledgeByDomain(String domain) throws IOException {
        byte[] prefix = bytes(PREFIX_KNOWLEDGE_IDX_DOMAIN + encode(domain) + ":");
        List<KnowledgeContext> result = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                String fullKey = str(kv.key());
                String knowledgeId = fullKey.substring(fullKey.lastIndexOf(':') + 1);
                getKnowledge(knowledgeId).ifPresent(result::add);
            }
        }
        return result;
    }

    @Override
    public List<KnowledgeContext> listPublicKnowledge() throws IOException {
        byte[] prefix = bytes(PREFIX_KNOWLEDGE);
        List<KnowledgeContext> result = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                String fullKey = str(kv.key());
                if (fullKey.contains(PREFIX_KNOWLEDGE_IDX_AGENT)
                        || fullKey.contains(PREFIX_KNOWLEDGE_IDX_DOMAIN)) {
                    continue;
                }
                if (fullKey.contains(PREFIX_KNOWLEDGE_ENTRY)
                        || fullKey.contains(PREFIX_KNOWLEDGE_HIST)) {
                    continue;
                }
                try {
                    KnowledgeContext kc = ContextJson.readValue(kv.value(), KnowledgeContext.class);
                    if (kc.scope() == KnowledgeContext.KnowledgeScope.PUBLIC) {
                        result.add(kc);
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to deserialize knowledge during public scan", e);
                }
            }
        }
        return result;
    }

    @Override
    public ContextEntry putKnowledgeEntry(
            String knowledgeId,
            String key,
            String value,
            String contentType,
            String encoding,
            Map<String, String> metadata)
            throws IOException {
        byte[] knowledgeKey = bytes(PREFIX_KNOWLEDGE + knowledgeId);
        Optional<byte[]> rawKnowledge = metastore.get(knowledgeKey);
        if (rawKnowledge.isEmpty()) {
            throw new IOException("Knowledge not found: " + knowledgeId);
        }
        KnowledgeContext knowledge =
                ContextJson.readValue(rawKnowledge.get(), KnowledgeContext.class);

        long now = System.currentTimeMillis();
        byte[] entryKeyBytes =
                bytes(PREFIX_KNOWLEDGE + knowledgeId + PREFIX_KNOWLEDGE_ENTRY + encode(key));
        Optional<byte[]> rawExisting = metastore.get(entryKeyBytes);

        ContextEntry existingEntry = null;
        int currentVersion = 0;
        int newVersion = 1;
        long createdAt = now;

        if (rawExisting.isPresent()) {
            existingEntry = ContextJson.readValue(rawExisting.get(), ContextEntry.class);
            currentVersion = existingEntry.version();
            newVersion = currentVersion + 1;
            createdAt = existingEntry.createdAt();
        }

        ContextEntry newEntry =
                ContextEntry.builder(key, value)
                        .contentType(contentType != null ? contentType : "text/plain")
                        .encoding(encoding != null ? encoding : "utf-8")
                        .version(newVersion)
                        .metadata(metadata != null ? metadata : Map.of())
                        .createdAt(createdAt)
                        .updatedAt(now)
                        .build();

        byte[] newEntryBytes = ContextJson.writeValueAsBytes(newEntry);

        List<Precondition> preconditions = new ArrayList<>();
        preconditions.add(new Precondition.ValueEquals(knowledgeKey, rawKnowledge.get()));

        List<Mutation> mutations = new ArrayList<>();

        if (existingEntry != null) {
            preconditions.add(new Precondition.ValueEquals(entryKeyBytes, rawExisting.get()));
            byte[] histKey =
                    bytes(
                            PREFIX_KNOWLEDGE
                                    + knowledgeId
                                    + PREFIX_KNOWLEDGE_HIST
                                    + encode(key)
                                    + ":"
                                    + padVersion(existingEntry.version()));
            mutations.add(new Mutation.Put(histKey, rawExisting.get()));
        } else {
            preconditions.add(new Precondition.NotExists(entryKeyBytes));
        }

        mutations.add(new Mutation.Put(entryKeyBytes, newEntryBytes));

        int newEntryCount = knowledge.entryCount() + (existingEntry == null ? 1 : 0);
        KnowledgeContext updatedKnowledge =
                new KnowledgeContext(
                        knowledgeId,
                        knowledge.agentId(),
                        knowledge.userId(),
                        knowledge.domain(),
                        knowledge.description(),
                        knowledge.scope(),
                        knowledge.sharedAgents(),
                        newEntryCount,
                        knowledge.createdAt(),
                        now);
        byte[] updatedKnowledgeBytes = ContextJson.writeValueAsBytes(updatedKnowledge);
        mutations.add(new Mutation.Put(knowledgeKey, updatedKnowledgeBytes));

        byte[] l0IdxKey =
                bytes(
                        PREFIX_L0_IDX
                                + encode(knowledge.agentId())
                                + ":l0:knowledge:"
                                + knowledgeId
                                + ":"
                                + encode(key));
        mutations.add(new Mutation.Put(l0IdxKey, new byte[0]));

        if (!metastore.compareAndSwap(preconditions, mutations)) {
            throw new IOException(
                    "Failed to put knowledge entry: "
                            + knowledgeId
                            + "/"
                            + key
                            + " (CAS conflict)");
        }

        trimKnowledgeHistory(knowledgeId, key);

        return newEntry;
    }

    @Override
    public Optional<ContextEntry> getKnowledgeEntry(String knowledgeId, String key)
            throws IOException {
        byte[] knowledgeKey = bytes(PREFIX_KNOWLEDGE + knowledgeId);
        if (metastore.get(knowledgeKey).isEmpty()) {
            return Optional.empty();
        }
        byte[] entryKey =
                bytes(PREFIX_KNOWLEDGE + knowledgeId + PREFIX_KNOWLEDGE_ENTRY + encode(key));
        Optional<byte[]> rawEntry = metastore.get(entryKey);
        return rawEntry.map(
                r -> {
                    try {
                        return ContextJson.readValue(r, ContextEntry.class);
                    } catch (IOException e) {
                        LOG.warn(
                                "Failed to deserialize knowledge entry: {}/{}",
                                knowledgeId,
                                key,
                                e);
                        return null;
                    }
                });
    }

    @Override
    public List<ContextEntry> listKnowledgeEntries(String knowledgeId) throws IOException {
        byte[] prefix = bytes(PREFIX_KNOWLEDGE + knowledgeId + PREFIX_KNOWLEDGE_ENTRY);
        List<ContextEntry> entries = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                try {
                    entries.add(ContextJson.readValue(it.next().value(), ContextEntry.class));
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to deserialize knowledge entry during list: {}",
                            knowledgeId,
                            e);
                }
            }
        }
        return entries;
    }

    @Override
    public boolean removeKnowledgeEntry(String knowledgeId, String key) throws IOException {
        byte[] knowledgeKey = bytes(PREFIX_KNOWLEDGE + knowledgeId);
        Optional<byte[]> rawKnowledge = metastore.get(knowledgeKey);
        if (rawKnowledge.isEmpty()) {
            return false;
        }
        KnowledgeContext knowledge =
                ContextJson.readValue(rawKnowledge.get(), KnowledgeContext.class);

        byte[] entryKeyBytes =
                bytes(PREFIX_KNOWLEDGE + knowledgeId + PREFIX_KNOWLEDGE_ENTRY + encode(key));
        Optional<byte[]> rawEntry = metastore.get(entryKeyBytes);
        if (rawEntry.isEmpty()) {
            return false;
        }

        long now = System.currentTimeMillis();
        KnowledgeContext updatedKnowledge =
                new KnowledgeContext(
                        knowledgeId,
                        knowledge.agentId(),
                        knowledge.userId(),
                        knowledge.domain(),
                        knowledge.description(),
                        knowledge.scope(),
                        knowledge.sharedAgents(),
                        Math.max(0, knowledge.entryCount() - 1),
                        knowledge.createdAt(),
                        now);
        byte[] updatedKnowledgeBytes = ContextJson.writeValueAsBytes(updatedKnowledge);

        List<Precondition> preconditions =
                List.of(
                        new Precondition.ValueEquals(knowledgeKey, rawKnowledge.get()),
                        new Precondition.ValueEquals(entryKeyBytes, rawEntry.get()));
        List<Mutation> mutations =
                List.of(
                        new Mutation.Delete(entryKeyBytes),
                        new Mutation.Put(knowledgeKey, updatedKnowledgeBytes),
                        new Mutation.Delete(
                                bytes(
                                        PREFIX_L0_IDX
                                                + encode(knowledge.agentId())
                                                + ":l0:knowledge:"
                                                + knowledgeId
                                                + ":"
                                                + encode(key))));

        if (!metastore.compareAndSwap(preconditions, mutations)) {
            throw new IOException(
                    "Failed to remove knowledge entry: "
                            + knowledgeId
                            + "/"
                            + key
                            + " (CAS conflict)");
        }

        return true;
    }

    @Override
    public boolean deleteKnowledge(String knowledgeId) throws IOException {
        byte[] knowledgeKey = bytes(PREFIX_KNOWLEDGE + knowledgeId);
        Optional<byte[]> rawKnowledge = metastore.get(knowledgeKey);
        if (rawKnowledge.isEmpty()) {
            return false;
        }
        KnowledgeContext knowledge =
                ContextJson.readValue(rawKnowledge.get(), KnowledgeContext.class);

        List<Mutation> mutations = new ArrayList<>();
        mutations.add(new Mutation.Delete(knowledgeKey));
        mutations.add(
                new Mutation.Delete(
                        bytes(
                                PREFIX_KNOWLEDGE_IDX_AGENT
                                        + encode(knowledge.agentId())
                                        + ":"
                                        + knowledgeId)));
        mutations.add(
                new Mutation.Delete(
                        bytes(
                                PREFIX_KNOWLEDGE_IDX_DOMAIN
                                        + encode(knowledge.domain())
                                        + ":"
                                        + knowledgeId)));

        byte[] entryPrefix = bytes(PREFIX_KNOWLEDGE + knowledgeId + PREFIX_KNOWLEDGE_ENTRY);
        try (CloseableIterator<KeyValue> it = metastore.scan(entryPrefix)) {
            while (it.hasNext()) {
                mutations.add(new Mutation.Delete(it.next().key()));
            }
        }

        byte[] histPrefix = bytes(PREFIX_KNOWLEDGE + knowledgeId + PREFIX_KNOWLEDGE_HIST);
        try (CloseableIterator<KeyValue> it = metastore.scan(histPrefix)) {
            while (it.hasNext()) {
                mutations.add(new Mutation.Delete(it.next().key()));
            }
        }

        byte[] l0Prefix =
                bytes(
                        PREFIX_L0_IDX
                                + encode(knowledge.agentId())
                                + ":l0:knowledge:"
                                + knowledgeId
                                + ":");
        try (CloseableIterator<KeyValue> it = metastore.scan(l0Prefix)) {
            while (it.hasNext()) {
                mutations.add(new Mutation.Delete(it.next().key()));
            }
        }

        metastore.batchWrite(mutations);
        return true;
    }

    @Override
    public List<ContextEntry> getEntryHistory(
            String contextType, String contextId, String key, int limit) throws IOException {
        String prefix;
        if ("session".equals(contextType)) {
            prefix = PREFIX_SESSION + contextId + PREFIX_SESSION_HIST + encode(key) + ":";
        } else {
            prefix = PREFIX_KNOWLEDGE + contextId + PREFIX_KNOWLEDGE_HIST + encode(key) + ":";
        }
        byte[] prefixBytes = bytes(prefix);
        List<ContextEntry> results = new ArrayList<>();

        if ("session".equals(contextType)) {
            getSessionEntry(contextId, key).ifPresent(results::add);
        } else {
            getKnowledgeEntry(contextId, key).ifPresent(results::add);
        }

        try (CloseableIterator<KeyValue> it = metastore.scan(prefixBytes)) {
            while (it.hasNext()) {
                try {
                    results.add(ContextJson.readValue(it.next().value(), ContextEntry.class));
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to deserialize history entry: {} {}",
                            contextType,
                            contextId,
                            e);
                }
            }
        }
        results.sort(Comparator.comparingInt(ContextEntry::version).reversed());
        if (results.size() > limit) {
            return results.subList(0, limit);
        }
        return results;
    }

    private void trimKnowledgeHistory(String knowledgeId, String key) throws IOException {
        byte[] histPrefix =
                bytes(PREFIX_KNOWLEDGE + knowledgeId + PREFIX_KNOWLEDGE_HIST + encode(key) + ":");
        List<KeyValue> versions = metastore.scanAll(histPrefix);
        if (versions.size() <= knowledgeHistoryVersions) {
            return;
        }
        versions.sort(
                Comparator.comparingInt(
                        kv -> {
                            String suffix = str(kv.key()).substring(str(histPrefix).length());
                            try {
                                return Integer.parseInt(suffix);
                            } catch (NumberFormatException e) {
                                return 0;
                            }
                        }));
        List<Mutation> deletes = new ArrayList<>();
        for (int i = 0; i < versions.size() - knowledgeHistoryVersions; i++) {
            deletes.add(new Mutation.Delete(versions.get(i).key()));
        }
        if (!deletes.isEmpty()) {
            metastore.batchWrite(deletes);
        }
    }

    @Override
    public void cleanupExpiredSessions() throws IOException {
        long now = System.currentTimeMillis();
        byte[] prefix = bytes(PREFIX_SESSION);
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                String fullKey = str(kv.key());
                if (fullKey.contains(PREFIX_SESSION_ENTRY)
                        || fullKey.contains(PREFIX_SESSION_HIST)) {
                    continue;
                }
                try {
                    SessionContext session =
                            ContextJson.readValue(kv.value(), SessionContext.class);
                    if (session.status() == SessionContext.SessionStatus.ACTIVE
                            && session.expiresAt() > 0
                            && session.expiresAt() < now) {
                        SessionContext expired =
                                new SessionContext(
                                        session.sessionId(),
                                        session.agentId(),
                                        session.userId(),
                                        session.createdAt(),
                                        session.updatedAt(),
                                        session.expiresAt(),
                                        session.entryCount(),
                                        SessionContext.SessionStatus.EXPIRED);
                        byte[] sessionKey = bytes(PREFIX_SESSION + session.sessionId());
                        metastore.compareAndSwap(
                                List.of(new Precondition.ValueEquals(sessionKey, kv.value())),
                                List.of(
                                        new Mutation.Put(
                                                sessionKey,
                                                ContextJson.writeValueAsBytes(expired))));
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to process session during cleanup", e);
                }
            }
        }
    }

    @Override
    public Optional<ContextEntry> getEntryAtTier(
            String contextType, String contextId, String key, Depth depth) throws IOException {
        Optional<ContextEntry> entry;
        if ("session".equals(contextType)) {
            entry = getSessionEntry(contextId, key);
        } else {
            entry = getKnowledgeEntry(contextId, key);
        }
        if (entry.isEmpty()) {
            return Optional.empty();
        }
        return entry.get().getContentAtTier(depth).isPresent() ? entry : Optional.empty();
    }

    @Override
    public List<L0Summary> scanL0(String agentId, String contextType, int limit)
            throws IOException {
        byte[] prefix = bytes(PREFIX_L0_IDX + encode(agentId) + ":l0:" + contextType + ":");
        String prefixStr = str(prefix);
        List<L0Summary> results = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext() && results.size() < limit) {
                KeyValue kv = it.next();
                String fullKey = str(kv.key());
                String suffix = fullKey.substring(prefixStr.length());
                int colonIdx = suffix.indexOf(':');
                if (colonIdx < 0) {
                    continue;
                }
                String contextId = suffix.substring(0, colonIdx);
                String encodedEntryKey = suffix.substring(colonIdx + 1);
                String entryKey = decode(encodedEntryKey);

                Optional<ContextEntry> entryOpt;
                if ("session".equals(contextType)) {
                    entryOpt = getSessionEntry(contextId, entryKey);
                } else {
                    entryOpt = getKnowledgeEntry(contextId, entryKey);
                }

                entryOpt.ifPresent(
                        entry ->
                                results.add(
                                        new L0Summary(
                                                contextType,
                                                contextId,
                                                entryKey,
                                                entry.l0Abstract(),
                                                entry.importance(),
                                                entry.decayFactor(),
                                                entry.createdAt(),
                                                entry.lastAccessedAt(),
                                                entry.accessCount())));
            }
        }
        return results;
    }

    @Override
    public List<L0Summary> rerankL1(
            String agentId, String contextType, List<L0Summary> l0Entries, String query)
            throws IOException {
        if (query == null || query.isEmpty()) {
            return l0Entries;
        }

        String lowerQuery = query.toLowerCase();
        Map<L0Summary, Integer> scores = new LinkedHashMap<>();

        for (L0Summary l0 : l0Entries) {
            Optional<ContextEntry> entryOpt;
            if ("session".equals(l0.contextType())) {
                entryOpt = getSessionEntry(l0.contextId(), l0.entryKey());
            } else {
                entryOpt = getKnowledgeEntry(l0.contextId(), l0.entryKey());
            }

            int score = 0;
            if (entryOpt.isPresent()) {
                String l1 = entryOpt.get().l1Overview().toLowerCase();
                for (String token : lowerQuery.split("\\s+")) {
                    if (!token.isEmpty() && l1.contains(token)) {
                        score++;
                    }
                }
            }
            scores.put(l0, score);
        }

        List<L0Summary> result = new ArrayList<>(l0Entries);
        result.sort((a, b) -> Integer.compare(scores.get(b), scores.get(a)));
        return result;
    }

    @Override
    public ContextEntry updateEntry(String contextType, String contextId, ContextEntry entry)
            throws IOException {
        String prefix;
        String histPrefix;
        int historyVersions;
        if ("session".equals(contextType)) {
            prefix = PREFIX_SESSION + contextId + PREFIX_SESSION_ENTRY;
            histPrefix = PREFIX_SESSION + contextId + PREFIX_SESSION_HIST;
            historyVersions = sessionHistoryVersions;
        } else {
            prefix = PREFIX_KNOWLEDGE + contextId + PREFIX_KNOWLEDGE_ENTRY;
            histPrefix = PREFIX_KNOWLEDGE + contextId + PREFIX_KNOWLEDGE_HIST;
            historyVersions = knowledgeHistoryVersions;
        }
        byte[] entryKeyBytes = bytes(prefix + encode(entry.key()));
        Optional<byte[]> rawExisting = metastore.get(entryKeyBytes);
        if (rawExisting.isEmpty()) {
            throw new IOException("Entry not found: " + contextId + "/" + entry.key());
        }
        ContextEntry existingEntry = ContextJson.readValue(rawExisting.get(), ContextEntry.class);
        if (historyVersions > 0 && existingEntry.version() > 0) {
            byte[] histKey =
                    bytes(
                            histPrefix
                                    + encode(entry.key())
                                    + ":"
                                    + padVersion(existingEntry.version()));
            metastore.compareAndSwap(
                    List.of(), List.of(new Mutation.Put(histKey, rawExisting.get())));
        }
        byte[] newEntryBytes = ContextJson.writeValueAsBytes(entry);
        boolean success =
                metastore.compareAndSwap(
                        List.of(new Precondition.ValueEquals(entryKeyBytes, rawExisting.get())),
                        List.of(new Mutation.Put(entryKeyBytes, newEntryBytes)));
        if (!success) {
            throw new IOException(
                    "Failed to update entry: " + contextId + "/" + entry.key() + " (CAS conflict)");
        }
        return entry;
    }
}
