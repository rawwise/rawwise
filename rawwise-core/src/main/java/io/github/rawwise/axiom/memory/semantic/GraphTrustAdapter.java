package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.governance.context.ContextJson;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphTrustAdapter {

    private static final String PREFIX_TRUST_HASH = "trust:graph:hash:";

    private final Metastore metastore;

    public GraphTrustAdapter(Metastore metastore) {
        this.metastore = metastore;
    }

    public void recordNodeChange(String graphId, String nodeId, long version, Object node) {
        try {
            byte[] nodeBytes = ContextJson.writeValueAsBytes(node);
            String hash = computeHash(nodeBytes);
            byte[] hashKey =
                    (PREFIX_TRUST_HASH + graphId + ":" + nodeId + ":" + version)
                            .getBytes(StandardCharsets.UTF_8);
            metastore.batchWrite(
                    List.of(new Mutation.Put(hashKey, hash.getBytes(StandardCharsets.UTF_8))));
        } catch (IOException e) {
            log.warn(
                    "Trust hash recording failed for node graphId={} nodeId={} version={}",
                    graphId,
                    nodeId,
                    version,
                    e);
        }
    }

    public void recordEdgeChange(String graphId, String edgeId, long version, Object edge) {
        try {
            byte[] edgeBytes = ContextJson.writeValueAsBytes(edge);
            String hash = computeHash(edgeBytes);
            byte[] hashKey =
                    (PREFIX_TRUST_HASH + graphId + ":edge:" + edgeId + ":" + version)
                            .getBytes(StandardCharsets.UTF_8);
            metastore.batchWrite(
                    List.of(new Mutation.Put(hashKey, hash.getBytes(StandardCharsets.UTF_8))));
        } catch (IOException e) {
            log.warn(
                    "Trust hash recording failed for edge graphId={} edgeId={} version={}",
                    graphId,
                    edgeId,
                    version,
                    e);
        }
    }

    public boolean verifyNodeIntegrity(String graphId, String nodeId, long version, Object node) {
        try {
            byte[] nodeBytes = ContextJson.writeValueAsBytes(node);
            String computedHash = computeHash(nodeBytes);
            byte[] hashKey =
                    (PREFIX_TRUST_HASH + graphId + ":" + nodeId + ":" + version)
                            .getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> stored = metastore.get(hashKey);
            if (stored.isEmpty()) return false;
            String storedHash = new String(stored.get(), StandardCharsets.UTF_8);
            return computedHash.equals(storedHash);
        } catch (IOException e) {
            log.warn(
                    "Trust verification failed for node graphId={} nodeId={} version={}",
                    graphId,
                    nodeId,
                    version,
                    e);
            return false;
        }
    }

    public boolean verifyEdgeIntegrity(String graphId, String edgeId, long version, Object edge) {
        try {
            byte[] edgeBytes = ContextJson.writeValueAsBytes(edge);
            String computedHash = computeHash(edgeBytes);
            byte[] hashKey =
                    (PREFIX_TRUST_HASH + graphId + ":edge:" + edgeId + ":" + version)
                            .getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> stored = metastore.get(hashKey);
            if (stored.isEmpty()) return false;
            String storedHash = new String(stored.get(), StandardCharsets.UTF_8);
            return computedHash.equals(storedHash);
        } catch (IOException e) {
            log.warn(
                    "Trust verification failed for edge graphId={} edgeId={} version={}",
                    graphId,
                    edgeId,
                    version,
                    e);
            return false;
        }
    }

    public double computeTrustScore(String graphId, GraphNode node, List<GraphEdge> edges) {
        if (node == null) return 0.0;

        boolean integrityVerified =
                verifyNodeIntegrity(graphId, node.nodeId(), node.version(), node);
        double integrityScore = integrityVerified ? 1.0 : 0.0;

        double importanceScore = Math.max(0, Math.min(node.importance(), 10)) / 10.0;

        int edgeCount = edges == null ? 0 : edges.size();
        double connectivityScore = Math.min(edgeCount / 10.0, 1.0);

        double versionScore = Math.min(node.version() / 5.0, 1.0);

        float decay = node.decayFactor();
        double decayScore = Math.max(0, Math.min(decay, 1.0));

        return 0.30 * integrityScore
                + 0.25 * importanceScore
                + 0.20 * connectivityScore
                + 0.15 * versionScore
                + 0.10 * decayScore;
    }

    private String computeHash(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
