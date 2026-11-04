package io.github.rawwise.governance.context;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.memory.Depth;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Context E2E - ContextManager full-chain business scenario test")
class ContextE2ETest {

    private InMemoryMetastore metastore;
    private ContextManager contextManager;

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        contextManager = new DefaultContextManager(metastore, 10);
    }

    @Nested
    @DisplayName("HappyPath - will and CRUD")
    class HappyPath {

        @Test
        @DisplayName("createwill verifyfield")
        void createSessionAndVerifyFields() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);

            assertThat(session.sessionId()).startsWith("sess-");
            assertThat(session.agentId()).isEqualTo("agent1");
            assertThat(session.userId()).isEqualTo("user1");
            assertThat(session.status()).isEqualTo(SessionContext.SessionStatus.ACTIVE);
            assertThat(session.entryCount()).isEqualTo(0);
            assertThat(session.createdAt()).isGreaterThan(0);
            assertThat(session.updatedAt()).isGreaterThan(0);
        }

        @Test
        @DisplayName("acquirewill")
        void getSessionReturnsPresent() throws IOException {
            SessionContext created = contextManager.createSession("agent1", "user1", 60000);
            Optional<SessionContext> got = contextManager.getSession(created.sessionId());

            assertThat(got).isPresent();
            assertThat(got.get().sessionId()).isEqualTo(created.sessionId());
            assertThat(got.get().agentId()).isEqualTo(created.agentId());
            assertThat(got.get().userId()).isEqualTo(created.userId());
            assertThat(got.get().status()).isEqualTo(SessionContext.SessionStatus.ACTIVE);
        }

        @Test
        @DisplayName("will after getSession returns empty")
        void expiredSessionReturnsEmpty() throws IOException, InterruptedException {
            SessionContext session = contextManager.createSession("agent1", "user1", 1);
            Thread.sleep(10);
            Optional<SessionContext> result = contextManager.getSession(session.sessionId());

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("will after getSession returns empty")
        void terminateSessionReturnsEmpty() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.terminateSession(session.sessionId());

            Optional<SessionContext> result = contextManager.getSession(session.sessionId());
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("writewill entry verify value")
        void putSessionEntryReturnsCorrectEntry() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);

            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(),
                            "key1",
                            "value1",
                            "text/plain",
                            "utf-8",
                            Map.of("tag", "test"));

            assertThat(entry.key()).isEqualTo("key1");
            assertThat(entry.value()).isEqualTo("value1");
            assertThat(entry.version()).isEqualTo(1);
            assertThat(entry.contentType()).isEqualTo("text/plain");
            assertThat(entry.encoding()).isEqualTo("utf-8");
            assertThat(entry.metadata()).containsEntry("tag", "test");
        }

        @Test
        @DisplayName("readwill entry")
        void getSessionEntryReturnsPresent() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.putSessionEntry(session.sessionId(), "key1", "value1", null, null, null);

            Optional<ContextEntry> entry =
                    contextManager.getSessionEntry(session.sessionId(), "key1");
            assertThat(entry).isPresent();
            assertThat(entry.get().key()).isEqualTo("key1");
            assertThat(entry.get().value()).isEqualTo("value1");
        }

        @Test
        @DisplayName("column will entry")
        void listSessionEntriesReturnsAll() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.putSessionEntry(session.sessionId(), "key1", "v1", null, null, null);
            contextManager.putSessionEntry(session.sessionId(), "key2", "v2", null, null, null);
            contextManager.putSessionEntry(session.sessionId(), "key3", "v3", null, null, null);

            List<ContextEntry> entries = contextManager.listSessionEntries(session.sessionId());
            assertThat(entries).hasSize(3);
            assertThat(entries)
                    .extracting(ContextEntry::key)
                    .containsExactlyInAnyOrder("key1", "key2", "key3");
        }

        @Test
        @DisplayName("deletewill entry")
        void removeSessionEntryReturnsTrue() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.putSessionEntry(session.sessionId(), "key1", "v1", null, null, null);

            boolean removed = contextManager.removeSessionEntry(session.sessionId(), "key1");
            assertThat(removed).isTrue();

            Optional<ContextEntry> entry =
                    contextManager.getSessionEntry(session.sessionId(), "key1");
            assertThat(entry).isEmpty();
        }

        @Test
        @DisplayName("entryversionhistory")
        void entryVersionHistory() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.putSessionEntry(
                    session.sessionId(), "histkey", "value_v1", null, null, null);
            contextManager.putSessionEntry(
                    session.sessionId(), "histkey", "value_v2", null, null, null);

            List<ContextEntry> history =
                    contextManager.getEntryHistory("session", session.sessionId(), "histkey", 10);

            assertThat(history).hasSize(2);
            assertThat(history.get(0).version()).isEqualTo(2);
            assertThat(history.get(1).version()).isEqualTo(1);
        }

        @Test
        @DisplayName("concurrent writesame key, version >= 2")
        void concurrentPutSessionEntryCasConflict() throws Exception {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            String sessionId = session.sessionId();

            contextManager.putSessionEntry(sessionId, "concurrentKey", "v1", null, null, null);

            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);

            Runnable task =
                    () -> {
                        try {
                            start.await();
                            contextManager.putSessionEntry(
                                    sessionId,
                                    "concurrentKey",
                                    "value",
                                    "text/plain",
                                    "utf-8",
                                    Map.of());
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            failCount.incrementAndGet();
                        }
                    };

            var executor = Executors.newFixedThreadPool(2);
            executor.submit(task);
            executor.submit(task);
            start.countDown();
            executor.shutdown();
            executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);

            assertThat(successCount.get() + failCount.get()).isEqualTo(2);

            Optional<ContextEntry> entry =
                    contextManager.getSessionEntry(sessionId, "concurrentKey");
            assertThat(entry).isPresent();
            assertThat(entry.get().version()).isGreaterThanOrEqualTo(2);
        }

        @Test
        @DisplayName("createknowledge context verifyfield")
        void createKnowledgeAndVerifyFields() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "finance",
                            "Financial knowledge base",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());

            assertThat(knowledge.knowledgeId()).startsWith("know-");
            assertThat(knowledge.agentId()).isEqualTo("agent1");
            assertThat(knowledge.userId()).isEqualTo("user1");
            assertThat(knowledge.domain()).isEqualTo("finance");
            assertThat(knowledge.description()).isEqualTo("Financial knowledge base");
            assertThat(knowledge.scope()).isEqualTo(KnowledgeContext.KnowledgeScope.PRIVATE);
            assertThat(knowledge.entryCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("acquireknowledge context")
        void getKnowledgeReturnsPresent() throws IOException {
            KnowledgeContext created =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "medical",
                            "desc",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());

            Optional<KnowledgeContext> got = contextManager.getKnowledge(created.knowledgeId());
            assertThat(got).isPresent();
            assertThat(got.get().knowledgeId()).isEqualTo(created.knowledgeId());
        }

        @Test
        @DisplayName("write entry verify")
        void putKnowledgeEntryReturnsCorrectEntry() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "legal",
                            "Legal docs",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());

            ContextEntry entry =
                    contextManager.putKnowledgeEntry(
                            knowledge.knowledgeId(),
                            "law1",
                            "Article 1",
                            "text/markdown",
                            "utf-8",
                            Map.of());

            assertThat(entry.key()).isEqualTo("law1");
            assertThat(entry.value()).isEqualTo("Article 1");
            assertThat(entry.version()).isEqualTo(1);
            assertThat(entry.contentType()).isEqualTo("text/markdown");
        }

        @Test
        @DisplayName("read entry")
        void getKnowledgeEntryReturnsPresent() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "tech",
                            "Tech docs",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "doc1", "content", null, null, null);

            Optional<ContextEntry> entry =
                    contextManager.getKnowledgeEntry(knowledge.knowledgeId(), "doc1");
            assertThat(entry).isPresent();
            assertThat(entry.get().value()).isEqualTo("content");
        }

        @Test
        @DisplayName("column entry")
        void listKnowledgeEntriesReturnsAll() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "science",
                            "Science facts",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            contextManager.putKnowledgeEntry(knowledge.knowledgeId(), "e1", "v1", null, null, null);
            contextManager.putKnowledgeEntry(knowledge.knowledgeId(), "e2", "v2", null, null, null);

            List<ContextEntry> entries =
                    contextManager.listKnowledgeEntries(knowledge.knowledgeId());
            assertThat(entries).hasSize(2);
            assertThat(entries).extracting(ContextEntry::key).containsExactlyInAnyOrder("e1", "e2");
        }

        @Test
        @DisplayName("delete entry")
        void removeKnowledgeEntryReturnsTrue() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "test",
                            "desc",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "rm1", "val", null, null, null);

            boolean removed = contextManager.removeKnowledgeEntry(knowledge.knowledgeId(), "rm1");
            assertThat(removed).isTrue();

            Optional<ContextEntry> entry =
                    contextManager.getKnowledgeEntry(knowledge.knowledgeId(), "rm1");
            assertThat(entry).isEmpty();
        }

        @Test
        @DisplayName("will can")
        void activeSessionPersists() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 0);

            Optional<SessionContext> got = contextManager.getSession(session.sessionId());
            assertThat(got).isPresent();
            assertThat(got.get().status()).isEqualTo(SessionContext.SessionStatus.ACTIVE);
        }

        @Test
        @DisplayName("will cleanupafterwillstatus is EXPIRED")
        void cleanupExpiredSessionsMarksAsExpired() throws IOException, InterruptedException {
            SessionContext active = contextManager.createSession("agent1", "user1", 0);
            SessionContext expiring = contextManager.createSession("agent2", "user1", 1);

            Thread.sleep(10);
            contextManager.cleanupExpiredSessions();

            Optional<SessionContext> activeGot = contextManager.getSession(active.sessionId());
            assertThat(activeGot).isPresent();
            assertThat(activeGot.get().status()).isEqualTo(SessionContext.SessionStatus.ACTIVE);

            Optional<SessionContext> expiredGot = contextManager.getSession(expiring.sessionId());
            if (expiredGot.isPresent()) {
                assertThat(expiredGot.get().status())
                        .isEqualTo(SessionContext.SessionStatus.EXPIRED);
            }
        }

        @Test
        @DisplayName("entryencodeand typestorageverify")
        void entryContentTypeAndEncodingStoredCorrectly() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);

            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(),
                            "jsonKey",
                            "{\"data\":1}",
                            "application/json",
                            "utf-8",
                            Map.of());

            assertThat(entry.contentType()).isEqualTo("application/json");
            assertThat(entry.encoding()).isEqualTo("utf-8");

            Optional<ContextEntry> got =
                    contextManager.getSessionEntry(session.sessionId(), "jsonKey");
            assertThat(got).isPresent();
            assertThat(got.get().contentType()).isEqualTo("application/json");
            assertThat(got.get().encoding()).isEqualTo("utf-8");
            assertThat(got.get().value()).isEqualTo("{\"data\":1}");
        }

        @Test
        @DisplayName("touchSession update")
        void touchSessionUpdatesTime() throws IOException, InterruptedException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            long initialUpdatedAt = session.updatedAt();

            Thread.sleep(5);

            SessionContext touched = contextManager.touchSession(session.sessionId());
            assertThat(touched.updatedAt()).isGreaterThan(initialUpdatedAt);
        }
    }

    @Nested
    @DisplayName("TieredLoading - ABSTRACT/DETAIL depth")
    class TieredLoading {

        @Test
        @DisplayName("ABSTRACT getEntryAtTier Optional.empty")
        void sessionL0ReturnsEmpty() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.putSessionEntry(
                    session.sessionId(), "tierKey", "value", null, null, null);

            Optional<ContextEntry> entry =
                    contextManager.getEntryAtTier(
                            "session", session.sessionId(), "tierKey", Depth.ABSTRACT);

            assertThat(entry).isEmpty();
        }

        @Test
        @DisplayName("ABSTRACT getEntryAtTier Optional.empty")
        void sessionL1ReturnsEmpty() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.putSessionEntry(
                    session.sessionId(), "tierKey", "value", null, null, null);

            Optional<ContextEntry> entry =
                    contextManager.getEntryAtTier(
                            "session", session.sessionId(), "tierKey", Depth.ABSTRACT);

            assertThat(entry).isEmpty();
        }

        @Test
        @DisplayName("DETAIL load")
        void sessionL2ReturnsFullEntry() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.putSessionEntry(
                    session.sessionId(),
                    "tierKey",
                    "full content",
                    "text/plain",
                    "utf-8",
                    Map.of());

            Optional<ContextEntry> entry =
                    contextManager.getEntryAtTier(
                            "session", session.sessionId(), "tierKey", Depth.DETAIL);

            assertThat(entry).isPresent();
            assertThat(entry.get().value()).isEqualTo("full content");
        }

        @Test
        @DisplayName("scanL0 package writeentryofcolumntable")
        void scanL0ReturnsList() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            contextManager.putSessionEntry(
                    session.sessionId(), "scanKey", "scanValue", null, null, null);

            List<L0Summary> summaries = contextManager.scanL0("agent1", "session", 50);

            assertThat(summaries).isNotNull();
            assertThat(summaries).isNotEmpty();
            assertThat(summaries.get(0).contextType()).isEqualTo("session");
            assertThat(summaries.get(0).entryKey()).isEqualTo("scanKey");
        }

        @Test
        @DisplayName("rerankL1 columntable returns empty list")
        void rerankL1EmptyInputReturnsEmpty() throws IOException {
            List<L0Summary> result =
                    contextManager.rerankL1("agent1", "session", List.of(), "keyword");

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("knowledge context getEntryAtTier(ABSTRACT) returns empty")
        void knowledgeL0ReturnsEmpty() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "domain",
                            "desc",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "ktKey", "ktValue", null, null, null);

            Optional<ContextEntry> entry =
                    contextManager.getEntryAtTier(
                            "knowledge", knowledge.knowledgeId(), "ktKey", Depth.ABSTRACT);

            assertThat(entry).isEmpty();
        }

        @Test
        @DisplayName("knowledge context getEntryAtTier(DETAIL) entry")
        void knowledgeL2ReturnsFullEntry() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "domain",
                            "desc",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "ktKey", "ktFullContent", null, null, null);

            Optional<ContextEntry> entry =
                    contextManager.getEntryAtTier(
                            "knowledge", knowledge.knowledgeId(), "ktKey", Depth.DETAIL);

            assertThat(entry).isPresent();
            assertThat(entry.get().value()).isEqualTo("ktFullContent");
        }

        @Test
        @DisplayName("knowledge context scanL0 columntable")
        void knowledgeScanL0ReturnsList() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "domain",
                            "desc",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "klKey", "klValue", null, null, null);

            List<L0Summary> summaries = contextManager.scanL0("agent1", "knowledge", 50);

            assertThat(summaries).isNotNull();
            assertThat(summaries).isNotEmpty();
            assertThat(summaries.get(0).contextType()).isEqualTo("knowledge");
            assertThat(summaries.get(0).entryKey()).isEqualTo("klKey");
        }

        @Test
        @DisplayName("knowledge context rerankL1 key")
        void knowledgeRerankL1Works() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "domain",
                            "desc",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "kk1", "v1", null, null, null);
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "kk2", "v2", null, null, null);

            List<L0Summary> l0Entries = contextManager.scanL0("agent1", "knowledge", 50);
            assertThat(l0Entries).isNotEmpty();

            List<L0Summary> reranked =
                    contextManager.rerankL1("agent1", "knowledge", l0Entries, "keyword");

            assertThat(reranked).hasSize(l0Entries.size());
        }
    }

    @Nested
    @DisplayName("KnowledgeVisibility - can")
    class KnowledgeVisibility {

        @Test
        @DisplayName("PRIVATE forits agent can")
        void privateKnowledgeNotVisibleToOtherAgent() throws IOException {
            contextManager.createKnowledge(
                    "agent1",
                    "user1",
                    "secret",
                    "Private knowledge",
                    KnowledgeContext.KnowledgeScope.PRIVATE,
                    List.of());

            List<KnowledgeContext> agent2Knowledge = contextManager.listKnowledgeByAgent("agent2");
            assertThat(agent2Knowledge).isEmpty();
        }

        @Test
        @DisplayName("SHARED for agent can")
        void sharedKnowledgeVisibleToSharedAgent() throws IOException {
            KnowledgeContext shared =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "shared",
                            "Shared knowledge",
                            KnowledgeContext.KnowledgeScope.SHARED,
                            List.of("agent1", "agent2"));

            List<KnowledgeContext> agent2Knowledge = contextManager.listKnowledgeByAgent("agent2");
            assertThat(agent2Knowledge).isNotEmpty();
            assertThat(agent2Knowledge)
                    .extracting(KnowledgeContext::knowledgeId)
                    .contains(shared.knowledgeId());
        }

        @Test
        @DisplayName("PUBLIC in listPublicKnowledge incan")
        void publicKnowledgeVisibleInListPublic() throws IOException {
            KnowledgeContext pub =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "open",
                            "Public knowledge",
                            KnowledgeContext.KnowledgeScope.PUBLIC,
                            List.of());

            List<KnowledgeContext> publicKnowledge = contextManager.listPublicKnowledge();
            assertThat(publicKnowledge).isNotEmpty();
            assertThat(publicKnowledge)
                    .extracting(KnowledgeContext::knowledgeId)
                    .contains(pub.knowledgeId());
        }
    }

    @Nested
    @DisplayName("BoundaryConditions - boundary condition test")
    class BoundaryConditions {

        @Test
        @DisplayName("duplicatecreatewill generate ID")
        void duplicateCreateSessionGeneratesUniqueId() throws IOException {
            SessionContext session1 = contextManager.createSession("agent1", "user1", 60000);
            SessionContext session2 = contextManager.createSession("agent1", "user1", 60000);

            assertThat(session1.sessionId()).isNotEqualTo(session2.sessionId());
            assertThat(session1.sessionId()).startsWith("sess-");
            assertThat(session2.sessionId()).startsWith("sess-");
        }

        @Test
        @DisplayName("delete cleanupall data")
        void deleteKnowledgeCleansUpEverything() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent1",
                            "user1",
                            "clean",
                            "To be deleted",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "ek1", "v1", null, null, null);
            contextManager.putKnowledgeEntry(
                    knowledge.knowledgeId(), "ek2", "v2", null, null, null);

            boolean deleted = contextManager.deleteKnowledge(knowledge.knowledgeId());
            assertThat(deleted).isTrue();

            Optional<KnowledgeContext> got = contextManager.getKnowledge(knowledge.knowledgeId());
            assertThat(got).isEmpty();

            List<KnowledgeContext> byAgent = contextManager.listKnowledgeByAgent("agent1");
            assertThat(byAgent)
                    .extracting(KnowledgeContext::knowledgeId)
                    .doesNotContain(knowledge.knowledgeId());
        }

        @Test
        @DisplayName("putSessionEntry correctly updates entryCount")
        void putSessionEntryUpdatesEntryCount() throws IOException {
            SessionContext session = contextManager.createSession("agent1", "user1", 60000);
            assertThat(session.entryCount()).isEqualTo(0);

            contextManager.putSessionEntry(
                    session.sessionId(), "countKey1", "val1", null, null, null);
            SessionContext afterNew = contextManager.getSession(session.sessionId()).orElseThrow();
            assertThat(afterNew.entryCount()).isEqualTo(1);

            contextManager.putSessionEntry(
                    session.sessionId(), "countKey1", "val2", null, null, null);
            SessionContext afterSameKey =
                    contextManager.getSession(session.sessionId()).orElseThrow();
            assertThat(afterSameKey.entryCount()).isEqualTo(1);

            contextManager.putSessionEntry(
                    session.sessionId(), "countKey2", "val3", null, null, null);
            SessionContext afterSecondKey =
                    contextManager.getSession(session.sessionId()).orElseThrow();
            assertThat(afterSecondKey.entryCount()).isEqualTo(2);

            assertThat(afterSecondKey.status()).isEqualTo(SessionContext.SessionStatus.ACTIVE);
        }
    }
}
