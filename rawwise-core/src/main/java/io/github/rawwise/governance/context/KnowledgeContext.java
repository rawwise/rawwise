package io.github.rawwise.governance.context;

import java.util.Set;

public record KnowledgeContext(
        String knowledgeId,
        String agentId,
        String userId,
        String domain,
        String description,
        KnowledgeScope scope,
        Set<String> sharedAgents,
        int entryCount,
        long createdAt,
        long updatedAt) {

    public KnowledgeContext {
        if (knowledgeId == null || knowledgeId.isEmpty()) {
            throw new IllegalArgumentException("knowledgeId must not be null or empty");
        }
        if (agentId == null || agentId.isEmpty()) {
            throw new IllegalArgumentException("agentId must not be null or empty");
        }
        if (userId == null || userId.isEmpty()) {
            throw new IllegalArgumentException("userId must not be null or empty");
        }
        if (domain == null) {
            domain = "";
        }
        if (scope == null) {
            scope = KnowledgeScope.PRIVATE;
        }
        if (sharedAgents == null) {
            sharedAgents = Set.of();
        } else {
            sharedAgents = Set.copyOf(sharedAgents);
        }
    }

    @Override
    public Set<String> sharedAgents() {
        return sharedAgents;
    }

    public enum KnowledgeScope {
        PRIVATE,
        SHARED,
        PUBLIC
    }
}
