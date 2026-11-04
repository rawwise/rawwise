package io.github.rawwise.governance.context;

import io.github.rawwise.axiom.memory.Depth;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ContextManager {

    SessionContext createSession(String agentId, String userId, long ttlMs) throws IOException;

    Optional<SessionContext> getSession(String sessionId) throws IOException;

    SessionContext touchSession(String sessionId) throws IOException;

    void terminateSession(String sessionId) throws IOException;

    ContextEntry putSessionEntry(
            String sessionId,
            String key,
            String value,
            String contentType,
            String encoding,
            Map<String, String> metadata)
            throws IOException;

    Optional<ContextEntry> getSessionEntry(String sessionId, String key) throws IOException;

    List<ContextEntry> listSessionEntries(String sessionId) throws IOException;

    boolean removeSessionEntry(String sessionId, String key) throws IOException;

    KnowledgeContext createKnowledge(
            String agentId,
            String userId,
            String domain,
            String description,
            KnowledgeContext.KnowledgeScope scope,
            List<String> sharedAgents)
            throws IOException;

    Optional<KnowledgeContext> getKnowledge(String knowledgeId) throws IOException;

    List<KnowledgeContext> listKnowledgeByAgent(String agentId) throws IOException;

    List<KnowledgeContext> listKnowledgeByDomain(String domain) throws IOException;

    List<KnowledgeContext> listPublicKnowledge() throws IOException;

    ContextEntry putKnowledgeEntry(
            String knowledgeId,
            String key,
            String value,
            String contentType,
            String encoding,
            Map<String, String> metadata)
            throws IOException;

    Optional<ContextEntry> getKnowledgeEntry(String knowledgeId, String key) throws IOException;

    List<ContextEntry> listKnowledgeEntries(String knowledgeId) throws IOException;

    boolean removeKnowledgeEntry(String knowledgeId, String key) throws IOException;

    boolean deleteKnowledge(String knowledgeId) throws IOException;

    List<ContextEntry> getEntryHistory(String contextType, String contextId, String key, int limit)
            throws IOException;

    void cleanupExpiredSessions() throws IOException;

    Optional<ContextEntry> getEntryAtTier(
            String contextType, String contextId, String key, Depth depth) throws IOException;

    List<L0Summary> scanL0(String agentId, String contextType, int limit) throws IOException;

    List<L0Summary> rerankL1(
            String agentId, String contextType, List<L0Summary> l0Entries, String query)
            throws IOException;

    ContextEntry updateEntry(String contextType, String contextId, ContextEntry entry)
            throws IOException;
}
