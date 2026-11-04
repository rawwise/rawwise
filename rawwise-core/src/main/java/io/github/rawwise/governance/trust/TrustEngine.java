package io.github.rawwise.governance.trust;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface TrustEngine {
    VerifiableTransition recordTransition(
            String contextType,
            String contextId,
            String entryKey,
            int fromVersion,
            int toVersion,
            String operation,
            Map<String, String> metadata)
            throws IOException;

    VerificationResult verify(String contextType, String contextId, String entryKey, int version)
            throws IOException;

    ProvenanceChain getProvenance(String id);

    List<AuditEntry> queryAudit(String agentId, long fromTs, long toTs, int limit);

    TrustStatus getStatus();
}
