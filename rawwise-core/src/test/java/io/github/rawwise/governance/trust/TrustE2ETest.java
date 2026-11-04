package io.github.rawwise.governance.trust;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.governance.context.*;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Trust E2E - Trust Engine full-chain business scenario test")
class TrustE2ETest {

    private Metastore metastore;
    private ContextManager contextManager;
    private MerkleVerifier verifier;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        contextManager = new DefaultContextManager(metastore, 10);
        verifier = new MerkleVerifier(contextManager, metastore);
    }

    @Nested
    @DisplayName("S1-HappyPath - canverifystatus lifecycle")
    class HappyPath {

        @Test
        @DisplayName("record verifywill entry")
        void recordAndVerifySessionTransition() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            VerifiableTransition vt =
                    verifier.recordTransition(
                            "session",
                            session.sessionId(),
                            "key1",
                            0,
                            entry.version(),
                            "CREATE",
                            Map.of());

            assertThat(vt.transitionId()).startsWith("vt-");
            assertThat(vt.contextType()).isEqualTo("session");
            assertThat(vt.contextId()).isEqualTo(session.sessionId());
            assertThat(vt.entryKey()).isEqualTo("key1");
            assertThat(vt.fromVersion()).isEqualTo(0);
            assertThat(vt.toVersion()).isEqualTo(entry.version());
            assertThat(vt.operation()).isEqualTo("CREATE");
            assertThat(vt.fromHash()).isNotEmpty();
            assertThat(vt.toHash()).isNotEmpty();
            assertThat(vt.timestamp()).isGreaterThan(0);

            VerificationResult result =
                    verifier.verify("session", session.sessionId(), "key1", entry.version());

            assertThat(result.valid()).isTrue();
            assertThat(result.computedHash()).isEqualTo(result.storedHash());
        }

        @Test
        @DisplayName("record verify entry")
        void recordAndVerifyKnowledgeTransition() throws IOException {
            KnowledgeContext knowledge =
                    contextManager.createKnowledge(
                            "agent-1",
                            "user-1",
                            "finance",
                            "Financial data",
                            KnowledgeContext.KnowledgeScope.PRIVATE,
                            List.of());
            ContextEntry entry =
                    contextManager.putKnowledgeEntry(
                            knowledge.knowledgeId(),
                            "stock",
                            "AAPL:150",
                            "text/plain",
                            "utf-8",
                            Map.of());

            VerifiableTransition vt =
                    verifier.recordTransition(
                            "knowledge",
                            knowledge.knowledgeId(),
                            "stock",
                            0,
                            entry.version(),
                            "CREATE",
                            Map.of());

            assertThat(vt.contextType()).isEqualTo("knowledge");

            VerificationResult result =
                    verifier.verify("knowledge", knowledge.knowledgeId(), "stock", entry.version());

            assertThat(result.valid()).isTrue();
        }

        @Test
        @DisplayName("multiple timesupdateafterverify version")
        void verifyAfterMultipleUpdates() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry v1 =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            verifier.recordTransition(
                    "session", session.sessionId(), "key1", 0, v1.version(), "CREATE", Map.of());

            ContextEntry v2 =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value2", "text/plain", "utf-8", Map.of());

            verifier.recordTransition(
                    "session",
                    session.sessionId(),
                    "key1",
                    v1.version(),
                    v2.version(),
                    "UPDATE",
                    Map.of());

            VerificationResult result =
                    verifier.verify("session", session.sessionId(), "key1", v2.version());

            assertThat(result.valid()).isTrue();
        }

        @Test
        @DisplayName("lifecycle: create->verify->update->verify")
        void fullTrustLifecycle() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);

            ContextEntry v1 =
                    contextManager.putSessionEntry(
                            session.sessionId(),
                            "doc1",
                            "original content",
                            "text/plain",
                            "utf-8",
                            Map.of());

            VerifiableTransition createVt =
                    verifier.recordTransition(
                            "session",
                            session.sessionId(),
                            "doc1",
                            0,
                            v1.version(),
                            "CREATE",
                            Map.of());
            assertThat(createVt.operation()).isEqualTo("CREATE");

            VerificationResult createResult =
                    verifier.verify("session", session.sessionId(), "doc1", v1.version());
            assertThat(createResult.valid()).isTrue();

            ContextEntry v2 =
                    contextManager.putSessionEntry(
                            session.sessionId(),
                            "doc1",
                            "updated content",
                            "text/plain",
                            "utf-8",
                            Map.of());

            VerifiableTransition updateVt =
                    verifier.recordTransition(
                            "session",
                            session.sessionId(),
                            "doc1",
                            v1.version(),
                            v2.version(),
                            "UPDATE",
                            Map.of());
            assertThat(updateVt.operation()).isEqualTo("UPDATE");

            VerificationResult updateResult =
                    verifier.verify("session", session.sessionId(), "doc1", v2.version());
            assertThat(updateResult.valid()).isTrue();
        }

        @Test
        @DisplayName("VerifiableTransition datastorage")
        void transitionMetadataStored() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            VerifiableTransition vt =
                    verifier.recordTransition(
                            "session",
                            session.sessionId(),
                            "key1",
                            0,
                            entry.version(),
                            "CREATE",
                            Map.of("source", "api", "userId", "user-1"));

            assertThat(vt.metadata()).containsEntry("source", "api");
            assertThat(vt.metadata()).containsEntry("userId", "user-1");
        }

        @Test
        @DisplayName("VerificationResult.ok")
        void verificationResultOkFactory() {
            VerificationResult result = VerificationResult.ok("ctx-1", "key1", 1, "hash1", "hash1");

            assertThat(result.valid()).isTrue();
            assertThat(result.errors()).isEmpty();
        }

        @Test
        @DisplayName("VerificationResult.fail")
        void verificationResultFailFactory() {
            VerificationResult result =
                    VerificationResult.fail(
                            "ctx-1", "key1", 1, "hash1", "hash2", List.of("Hash mismatch"));

            assertThat(result.valid()).isFalse();
            assertThat(result.errors()).containsExactly("Hash mismatch");
        }
    }

    @Nested
    @DisplayName("S2-Concurrency - concurrentverifyoperations")
    class Concurrency {

        @Test
        @DisplayName("concurrentrecorddifferententryof")
        void concurrentRecordDifferentEntries() throws Exception {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry1 =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());
            ContextEntry entry2 =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key2", "value2", "text/plain", "utf-8", Map.of());

            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);

            Runnable task1 =
                    () -> {
                        try {
                            start.await();
                            verifier.recordTransition(
                                    "session",
                                    session.sessionId(),
                                    "key1",
                                    0,
                                    entry1.version(),
                                    "CREATE",
                                    Map.of());
                            successCount.incrementAndGet();
                        } catch (Exception ignored) {
                        }
                    };

            Runnable task2 =
                    () -> {
                        try {
                            start.await();
                            verifier.recordTransition(
                                    "session",
                                    session.sessionId(),
                                    "key2",
                                    0,
                                    entry2.version(),
                                    "CREATE",
                                    Map.of());
                            successCount.incrementAndGet();
                        } catch (Exception ignored) {
                        }
                    };

            var executor = Executors.newFixedThreadPool(2);
            executor.submit(task1);
            executor.submit(task2);
            start.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            assertThat(successCount.get()).isGreaterThanOrEqualTo(1);

            VerificationResult result1 =
                    verifier.verify("session", session.sessionId(), "key1", entry1.version());
            if (successCount.get() == 2) {
                assertThat(result1.valid()).isTrue();
            }
        }

        @Test
        @DisplayName("concurrentverifysameentry")
        void concurrentVerifySameEntry() throws Exception {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());
            verifier.recordTransition(
                    "session", session.sessionId(), "key1", 0, entry.version(), "CREATE", Map.of());

            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger validCount = new AtomicInteger(0);

            Runnable task =
                    () -> {
                        try {
                            start.await();
                            VerificationResult result =
                                    verifier.verify(
                                            "session",
                                            session.sessionId(),
                                            "key1",
                                            entry.version());
                            if (result.valid()) validCount.incrementAndGet();
                        } catch (Exception ignored) {
                        }
                    };

            var executor = Executors.newFixedThreadPool(5);
            for (int i = 0; i < 5; i++) {
                executor.submit(task);
            }
            start.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            assertThat(validCount.get()).isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("S3-FaultRecovery - failure recovery")
    class FaultRecovery {

        @Test
        @DisplayName("verifynon-existententryreturns failure")
        void verifyNonExistentEntryFails() throws IOException {
            VerificationResult result = verifier.verify("session", "nonexistent", "key1", 1);

            assertThat(result.valid()).isFalse();
            assertThat(result.errors()).anyMatch(e -> e.contains("not found"));
        }

        @Test
        @DisplayName("verifyno recordofversionreturns failure")
        void verifyNoTransitionRecordFails() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            contextManager.putSessionEntry(
                    session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            VerificationResult result =
                    verifier.verify("session", session.sessionId(), "key1", 999);

            assertThat(result.valid()).isFalse();
            assertThat(result.errors()).anyMatch(e -> e.contains("No transition record"));
        }

        @Test
        @DisplayName("record entrydoes not exist exception")
        void recordTransitionNonExistentEntryThrows() throws IOException {
            assertThatThrownBy(
                            () ->
                                    verifier.recordTransition(
                                            "session",
                                            "nonexistent",
                                            "key1",
                                            0,
                                            1,
                                            "CREATE",
                                            Map.of()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("not found");
        }

        @Test
        @DisplayName("verifyfail alreadyhas record")
        void failedVerifyDoesNotCorruptTransitions() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            verifier.recordTransition(
                    "session", session.sessionId(), "key1", 0, entry.version(), "CREATE", Map.of());

            verifier.verify("session", session.sessionId(), "key1", 999);

            VerificationResult validResult =
                    verifier.verify("session", session.sessionId(), "key1", entry.version());
            assertThat(validResult.valid()).isTrue();
        }

        @Test
        @DisplayName("be afterverifyfail(hashdo not match)")
        void tamperedContentVerificationFails() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(),
                            "key1",
                            "original value",
                            "text/plain",
                            "utf-8",
                            Map.of());

            verifier.recordTransition(
                    "session", session.sessionId(), "key1", 0, entry.version(), "CREATE", Map.of());

            contextManager.putSessionEntry(
                    session.sessionId(), "key1", "tampered value", "text/plain", "utf-8", Map.of());

            VerificationResult result =
                    verifier.verify("session", session.sessionId(), "key1", entry.version());

            assertThat(result.valid()).isFalse();
            assertThat(result.errors())
                    .anyMatch(e -> e.contains("mismatch") || e.contains("tampered"));
        }
    }

    @Nested
    @DisplayName("S4-Boundary - boundary conditions")
    class Boundary {

        @Test
        @DisplayName("fromVersion=0 fromHashis")
        void fromVersionZeroHashIsAllZeros() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            VerifiableTransition vt =
                    verifier.recordTransition(
                            "session",
                            session.sessionId(),
                            "key1",
                            0,
                            entry.version(),
                            "CREATE",
                            Map.of());

            assertThat(vt.fromVersion()).isEqualTo(0);
            assertThat(vt.fromHash()).isEqualTo("0".repeat(64));
        }

        @Test
        @DisplayName("metadataof record")
        void transitionWithNullMetadata() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            VerifiableTransition vt =
                    verifier.recordTransition(
                            "session",
                            session.sessionId(),
                            "key1",
                            0,
                            entry.version(),
                            "CREATE",
                            null);

            assertThat(vt.metadata()).isNotNull();
            assertThat(vt.metadata()).isEmpty();
        }

        @Test
        @DisplayName("verifyversion0(does not exist)returns failure")
        void verifyVersionZeroFails() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            contextManager.putSessionEntry(
                    session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            VerificationResult result = verifier.verify("session", session.sessionId(), "key1", 0);

            assertThat(result.valid()).isFalse();
        }

        @Test
        @DisplayName("different agents context entries verify isolation")
        void verificationIsolationBetweenAgents() throws IOException {
            SessionContext session1 = contextManager.createSession("agent-1", "user-1", 0);
            SessionContext session2 = contextManager.createSession("agent-2", "user-2", 0);

            ContextEntry entry1 =
                    contextManager.putSessionEntry(
                            session1.sessionId(),
                            "key1",
                            "agent1 data",
                            "text/plain",
                            "utf-8",
                            Map.of());
            ContextEntry entry2 =
                    contextManager.putSessionEntry(
                            session2.sessionId(),
                            "key1",
                            "agent2 data",
                            "text/plain",
                            "utf-8",
                            Map.of());

            verifier.recordTransition(
                    "session",
                    session1.sessionId(),
                    "key1",
                    0,
                    entry1.version(),
                    "CREATE",
                    Map.of());
            verifier.recordTransition(
                    "session",
                    session2.sessionId(),
                    "key1",
                    0,
                    entry2.version(),
                    "CREATE",
                    Map.of());

            VerificationResult result1 =
                    verifier.verify("session", session1.sessionId(), "key1", entry1.version());
            VerificationResult result2 =
                    verifier.verify("session", session2.sessionId(), "key1", entry2.version());

            assertThat(result1.valid()).isTrue();
            assertThat(result2.valid()).isTrue();
            assertThat(result1.computedHash()).isNotEqualTo(result2.computedHash());
        }

        @Test
        @DisplayName("SHA-256 is64")
        void sha256HashLength() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);
            ContextEntry entry =
                    contextManager.putSessionEntry(
                            session.sessionId(), "key1", "value1", "text/plain", "utf-8", Map.of());

            VerifiableTransition vt =
                    verifier.recordTransition(
                            "session",
                            session.sessionId(),
                            "key1",
                            0,
                            entry.version(),
                            "CREATE",
                            Map.of());

            assertThat(vt.toHash()).hasSize(64);
            assertThat(vt.toHash()).matches("[0-9a-f]+");
        }

        @Test
        @DisplayName("same same")
        void sameContentSameHash() throws IOException {
            SessionContext session1 = contextManager.createSession("agent-1", "user-1", 0);
            SessionContext session2 = contextManager.createSession("agent-2", "user-2", 0);

            ContextEntry entry1 =
                    contextManager.putSessionEntry(
                            session1.sessionId(),
                            "key1",
                            "identical content",
                            "text/plain",
                            "utf-8",
                            Map.of());
            ContextEntry entry2 =
                    contextManager.putSessionEntry(
                            session2.sessionId(),
                            "key1",
                            "identical content",
                            "text/plain",
                            "utf-8",
                            Map.of());

            VerifiableTransition vt1 =
                    verifier.recordTransition(
                            "session",
                            session1.sessionId(),
                            "key1",
                            0,
                            entry1.version(),
                            "CREATE",
                            Map.of());
            VerifiableTransition vt2 =
                    verifier.recordTransition(
                            "session",
                            session2.sessionId(),
                            "key1",
                            0,
                            entry2.version(),
                            "CREATE",
                            Map.of());

            assertThat(vt1.toHash()).isEqualTo(vt2.toHash());
        }

        @Test
        @DisplayName("recordofverify can")
        void manyTransitionsVerificationPerformance() throws IOException {
            SessionContext session = contextManager.createSession("agent-1", "user-1", 0);

            for (int i = 0; i < 20; i++) {
                ContextEntry entry =
                        contextManager.putSessionEntry(
                                session.sessionId(),
                                "key-" + i,
                                "value-" + i,
                                "text/plain",
                                "utf-8",
                                Map.of());
                verifier.recordTransition(
                        "session",
                        session.sessionId(),
                        "key-" + i,
                        0,
                        entry.version(),
                        "CREATE",
                        Map.of());
            }

            ContextEntry lastEntry =
                    contextManager.putSessionEntry(
                            session.sessionId(),
                            "key-19",
                            "value-19",
                            "text/plain",
                            "utf-8",
                            Map.of());
            verifier.recordTransition(
                    "session",
                    session.sessionId(),
                    "key-19",
                    1,
                    lastEntry.version(),
                    "UPDATE",
                    Map.of());

            VerificationResult result =
                    verifier.verify("session", session.sessionId(), "key-19", lastEntry.version());

            assertThat(result.valid()).isTrue();
        }
    }
}
