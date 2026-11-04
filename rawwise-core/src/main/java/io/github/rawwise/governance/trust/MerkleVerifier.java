package io.github.rawwise.governance.trust;

import io.github.rawwise.governance.context.ContextEntry;
import io.github.rawwise.governance.context.ContextJson;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MerkleVerifier implements TrustEngine {

    private static final String PREFIX_TRANSITION = "trust:transition:";
    private static final String PREFIX_TRANSITION_IDX = "trust:transition:idx:";

    private final ContextManager contextManager;
    private final Metastore metastore;

    public MerkleVerifier(ContextManager contextManager, Metastore metastore) {
        this.contextManager = contextManager;
        this.metastore = metastore;
    }

    public VerifiableTransition recordTransition(
            String contextType,
            String contextId,
            String entryKey,
            int fromVersion,
            int toVersion,
            String operation,
            Map<String, String> metadata)
            throws IOException {
        ContextEntry entry = loadEntry(contextType, contextId, entryKey);
        if (entry == null) throw new IOException("Entry not found: " + entryKey);
        String toHash = computeHash(entry);
        String fromHash =
                fromVersion > 0
                        ? computeHashForVersion(contextType, contextId, entryKey, fromVersion)
                        : "0".repeat(64);
        VerifiableTransition transition =
                new VerifiableTransition(
                        "vt-" + UUID.randomUUID(),
                        contextType,
                        contextId,
                        entryKey,
                        fromVersion,
                        toVersion,
                        fromHash,
                        toHash,
                        operation,
                        metadata,
                        System.currentTimeMillis());
        byte[] key =
                (PREFIX_TRANSITION + transition.transitionId()).getBytes(StandardCharsets.UTF_8);
        byte[] value = ContextJson.writeValueAsBytes(transition);
        byte[] idxKey =
                (PREFIX_TRANSITION_IDX
                                + contextType
                                + ":"
                                + contextId
                                + ":"
                                + entryKey
                                + ":"
                                + toVersion)
                        .getBytes(StandardCharsets.UTF_8);
        boolean success =
                metastore.compareAndSwap(
                        List.of(new Precondition.NotExists(key)),
                        List.of(
                                new Mutation.Put(key, value),
                                new Mutation.Put(
                                        idxKey,
                                        transition
                                                .transitionId()
                                                .getBytes(StandardCharsets.UTF_8))));
        if (!success) {
            log.warn("Transition already exists vtId={}", transition.transitionId());
        }
        log.debug(
                "Recorded transition vtId={} contextType={} entryKey={} {}→{} op={}",
                transition.transitionId(),
                contextType,
                entryKey,
                fromVersion,
                toVersion,
                operation);
        return transition;
    }

    public VerificationResult verify(
            String contextType, String contextId, String entryKey, int version) throws IOException {
        ContextEntry entry = loadEntry(contextType, contextId, entryKey);
        if (entry == null) {
            log.warn(
                    "Verify failed: entry not found contextType={} contextId={} entryKey={}",
                    contextType,
                    contextId,
                    entryKey);
            return VerificationResult.fail(
                    contextId, entryKey, version, "", "", List.of("Entry not found"));
        }
        String computedHash = computeHash(entry);
        VerifiableTransition lastTransition =
                findTransition(contextType, contextId, entryKey, version);
        if (lastTransition == null) {
            log.warn(
                    "Verify failed: no transition record contextType={} contextId={} entryKey={} version={}",
                    contextType,
                    contextId,
                    entryKey,
                    version);
            return VerificationResult.fail(
                    contextId,
                    entryKey,
                    version,
                    computedHash,
                    "",
                    List.of("No transition record found for version " + version));
        }
        if (!computedHash.equals(lastTransition.toHash())) {
            log.error(
                    "Hash mismatch contextType={} contextId={} entryKey={} version={} computed={} stored={}",
                    contextType,
                    contextId,
                    entryKey,
                    version,
                    computedHash,
                    lastTransition.toHash());
            return VerificationResult.fail(
                    contextId,
                    entryKey,
                    version,
                    computedHash,
                    lastTransition.toHash(),
                    List.of(
                            "Hash mismatch: content has been tampered or modified outside the system"));
        }
        log.debug(
                "Verify passed contextType={} contextId={} entryKey={} version={}",
                contextType,
                contextId,
                entryKey,
                version);
        return VerificationResult.ok(
                contextId, entryKey, version, computedHash, lastTransition.toHash());
    }

    private ContextEntry loadEntry(String contextType, String contextId, String entryKey)
            throws IOException {
        if ("session".equals(contextType)) {
            return contextManager.getSessionEntry(contextId, entryKey).orElse(null);
        } else {
            return contextManager.getKnowledgeEntry(contextId, entryKey).orElse(null);
        }
    }

    private String computeHash(ContextEntry entry) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String content =
                    entry.key()
                            + ":"
                            + entry.value()
                            + ":"
                            + entry.version()
                            + ":"
                            + entry.updatedAt();
            byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private String computeHashForVersion(
            String contextType, String contextId, String entryKey, int version) throws IOException {
        List<ContextEntry> history =
                contextManager.getEntryHistory(contextType, contextId, entryKey, 100);
        for (ContextEntry e : history) {
            if (e.version() == version) return computeHash(e);
        }
        return "0".repeat(64);
    }

    private VerifiableTransition findTransition(
            String contextType, String contextId, String entryKey, int version) throws IOException {
        byte[] idxKey =
                (PREFIX_TRANSITION_IDX
                                + contextType
                                + ":"
                                + contextId
                                + ":"
                                + entryKey
                                + ":"
                                + version)
                        .getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> idxValue = metastore.get(idxKey);
        if (idxValue.isPresent()) {
            String vtId = new String(idxValue.get(), StandardCharsets.UTF_8);
            byte[] vtKey = (PREFIX_TRANSITION + vtId).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> vtValue = metastore.get(vtKey);
            if (vtValue.isPresent()) {
                return ContextJson.readValue(vtValue.get(), VerifiableTransition.class);
            }
        }
        return null;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    @Override
    public ProvenanceChain getProvenance(String id) {
        try {
            List<VerifiableTransition> matched = new ArrayList<>();
            byte[] prefix = PREFIX_TRANSITION_IDX.getBytes(StandardCharsets.UTF_8);
            metastore
                    .scan(prefix)
                    .forEachRemaining(
                            kv -> {
                                String idxKey = new String(kv.key(), StandardCharsets.UTF_8);
                                if (idxKey.contains(":" + id + ":")) {
                                    String vtId = new String(kv.value(), StandardCharsets.UTF_8);
                                    try {
                                        byte[] vtKey =
                                                (PREFIX_TRANSITION + vtId)
                                                        .getBytes(StandardCharsets.UTF_8);
                                        byte[] vtValue = metastore.get(vtKey).orElse(null);
                                        if (vtValue != null) {
                                            matched.add(
                                                    ContextJson.readValue(
                                                            vtValue, VerifiableTransition.class));
                                        }
                                    } catch (IOException ex) {
                                        log.warn(
                                                "provenance transition read failed for vtId={}",
                                                vtId,
                                                ex);
                                    }
                                }
                            });
            if (matched.isEmpty()) return null;
            matched.sort(Comparator.comparingLong(VerifiableTransition::timestamp));
            boolean allValid =
                    matched.stream()
                            .allMatch(
                                    vt -> {
                                        try {
                                            VerificationResult vr =
                                                    verify(
                                                            vt.contextType(),
                                                            vt.contextId(),
                                                            vt.entryKey(),
                                                            vt.toVersion());
                                            return vr.valid();
                                        } catch (IOException e) {
                                            log.debug(
                                                    "Provenance verify failed for vtId={}: {}",
                                                    vt.transitionId(),
                                                    e.getMessage());
                                            return false;
                                        }
                                    });
            return new ProvenanceChain(id, matched, allValid, System.currentTimeMillis());
        } catch (IOException e) {
            log.warn("getProvenance failed for id={}", id, e);
            return null;
        }
    }

    @Override
    public List<AuditEntry> queryAudit(String agentId, long fromTs, long toTs, int limit) {
        int effLimit = (limit <= 0) ? 100 : limit;
        List<AuditEntry> results = new ArrayList<>();
        try {
            byte[] prefix = PREFIX_TRANSITION.getBytes(StandardCharsets.UTF_8);
            metastore
                    .scan(prefix)
                    .forEachRemaining(
                            kv -> {
                                if (results.size() >= effLimit) return;
                                try {
                                    VerifiableTransition vt =
                                            ContextJson.readValue(
                                                    kv.value(), VerifiableTransition.class);
                                    if (vt.timestamp() < fromTs || vt.timestamp() > toTs) return;
                                    if (agentId != null && !agentId.isBlank()) {
                                        String vtAgentId =
                                                vt.metadata() != null
                                                        ? vt.metadata().get("agentId")
                                                        : null;
                                        if (!agentId.equals(vtAgentId)) return;
                                    }
                                    results.add(
                                            new AuditEntry(
                                                    agentId != null ? agentId : "unknown",
                                                    vt.operation(),
                                                    vt.contextType(),
                                                    vt.contextId(),
                                                    vt.entryKey(),
                                                    vt.metadata(),
                                                    vt.timestamp()));
                                } catch (IOException ex) {
                                    log.warn(
                                            "queryAudit read failed for key={}",
                                            new String(kv.key(), StandardCharsets.UTF_8),
                                            ex);
                                }
                            });
        } catch (IOException e) {
            log.warn("queryAudit failed for agentId={}", agentId, e);
        }
        results.sort(Comparator.comparingLong(AuditEntry::timestamp));
        return results;
    }

    @Override
    public TrustStatus getStatus() {
        int totalTransitions = 0;
        int failed = 0;
        try {
            byte[] prefix = PREFIX_TRANSITION.getBytes(StandardCharsets.UTF_8);
            List<VerifiableTransition> all = new ArrayList<>();
            metastore
                    .scan(prefix)
                    .forEachRemaining(
                            kv -> {
                                try {
                                    all.add(
                                            ContextJson.readValue(
                                                    kv.value(), VerifiableTransition.class));
                                } catch (IOException ex) {
                                    log.warn("getStatus read failed", ex);
                                }
                            });
            totalTransitions = all.size();
            for (VerifiableTransition vt : all) {
                try {
                    VerificationResult vr =
                            verify(vt.contextType(), vt.contextId(), vt.entryKey(), vt.toVersion());
                    if (!vr.valid()) failed++;
                } catch (IOException e) {
                    log.debug(
                            "Status verify failed for vtId={}: {}",
                            vt.transitionId(),
                            e.getMessage());
                    failed++;
                }
            }
        } catch (IOException e) {
            log.warn("getStatus failed", e);
        }
        return new TrustStatus(
                totalTransitions, totalTransitions - failed, failed, System.currentTimeMillis());
    }
}
