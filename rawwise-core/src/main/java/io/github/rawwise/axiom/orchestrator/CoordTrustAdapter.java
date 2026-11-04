package io.github.rawwise.axiom.orchestrator;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

public class CoordTrustAdapter {
    private static final String TRUST_PREFIX = "coord::trust::";
    private final Metastore metastore;

    public CoordTrustAdapter(Metastore metastore) {
        this.metastore = metastore;
    }

    public void recordLockChange(
            String resource, String agentId, String operation, long timestamp) {
        recordChange("lock", resource, agentId, operation, timestamp);
    }

    public void recordElectionChange(
            String electionId, String agentId, String operation, long timestamp) {
        recordChange("election", electionId, agentId, operation, timestamp);
    }

    public void recordLeaseChange(
            String leaseId, String agentId, String operation, long timestamp) {
        recordChange("lease", leaseId, agentId, operation, timestamp);
    }

    private void recordChange(
            String type, String resourceId, String agentId, String operation, long timestamp) {
        try {
            String data =
                    type + ":" + resourceId + ":" + agentId + ":" + operation + ":" + timestamp;
            String hash = computeHash(data.getBytes(StandardCharsets.UTF_8));
            byte[] key =
                    (TRUST_PREFIX
                                    + type
                                    + "::"
                                    + resourceId
                                    + "::"
                                    + timestamp
                                    + "::"
                                    + UUID.randomUUID())
                            .getBytes(StandardCharsets.UTF_8);
            metastore.batchWrite(
                    List.of(new Mutation.Put(key, hash.getBytes(StandardCharsets.UTF_8))));
        } catch (Exception e) {
            // Trust recording is best-effort, never block coordination operations
        }
    }

    private String computeHash(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(data);
        return HexFormat.of().formatHex(hash);
    }
}
