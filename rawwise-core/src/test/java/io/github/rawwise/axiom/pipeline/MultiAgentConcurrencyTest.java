package io.github.rawwise.axiom.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.axiom.pipeline.cognition.*;
import io.github.rawwise.axiom.pipeline.evaluation.DefaultEvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.record.Semantic;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MultiAgentConcurrencyTest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private DefaultCognitionEngine cognitionEngine;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        EvaluationEngine evalEngine = new DefaultEvaluationEngine(metastore, knowledgeStore);
        ActionExecutor actionExecutor = new DefaultActionExecutor(metastore, knowledgeStore, null);
        cognitionEngine =
                new DefaultCognitionEngine(metastore, knowledgeStore, evalEngine, actionExecutor);
    }

    @Test
    void c1_deriveIntentCas_onlyOneAgentClaims() throws Exception {
        String rawId = "raw:test-001";
        byte[] intentKey = ("derive_intent:CHUNK:" + rawId).getBytes(StandardCharsets.UTF_8);

        int agentCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(agentCount);
        AtomicInteger claimCount = new AtomicInteger(0);

        for (int i = 0; i < agentCount; i++) {
            final int agentIdx = i;
            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    boolean claimed =
                                            metastore.compareAndSwap(
                                                    List.of(new Precondition.NotExists(intentKey)),
                                                    List.of(
                                                            new Mutation.Put(
                                                                    intentKey,
                                                                    ("agent:" + agentIdx)
                                                                            .getBytes(
                                                                                    StandardCharsets
                                                                                            .UTF_8))));
                                    if (claimed) claimCount.incrementAndGet();
                                } catch (Exception e) {
                                    // ignore
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);

        assertEquals(1, claimCount.get(), "Only one agent should claim the derive intent");
        assertTrue(metastore.get(intentKey).isPresent(), "Intent key should exist after claim");
    }

    @Test
    void c2_viewLatestCas_versionMonotonicallyIncreasing() throws Exception {
        String viewId = "view:test-001";
        byte[] latestKey = ("view:latest:" + viewId).getBytes(StandardCharsets.UTF_8);

        int agentCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(agentCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < agentCount; i++) {
            final int version = i + 1;
            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (int attempt = 0; attempt < 8; attempt++) {
                                        Optional<byte[]> current = metastore.get(latestKey);
                                        int currentVersion =
                                                current.isEmpty()
                                                        ? 0
                                                        : Integer.parseInt(
                                                                new String(
                                                                                current.get(),
                                                                                StandardCharsets
                                                                                        .UTF_8)
                                                                        .trim());
                                        if (version <= currentVersion) break;

                                        List<Precondition> preconditions =
                                                current.isPresent()
                                                        ? List.of(
                                                                new Precondition.ValueEquals(
                                                                        latestKey, current.get()))
                                                        : List.of(
                                                                new Precondition.NotExists(
                                                                        latestKey));
                                        boolean success =
                                                metastore.compareAndSwap(
                                                        preconditions,
                                                        List.of(
                                                                new Mutation.Put(
                                                                        latestKey,
                                                                        String.valueOf(version)
                                                                                .getBytes(
                                                                                        StandardCharsets
                                                                                                .UTF_8))));
                                        if (success) {
                                            successCount.incrementAndGet();
                                            break;
                                        }
                                        Thread.sleep(10);
                                    }
                                } catch (Exception e) {
                                    // ignore
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);

        Optional<byte[]> latestVal = metastore.get(latestKey);
        assertTrue(latestVal.isPresent(), "view:latest should exist");
        int finalVersion =
                Integer.parseInt(new String(latestVal.get(), StandardCharsets.UTF_8).trim());
        assertEquals(agentCount, finalVersion, "Final version should equal agent count");
        assertTrue(successCount.get() > 0, "At least one agent should succeed");
    }

    @Test
    void c3_concurrentSemanticDerive_allSucceedWithUniqueIds() throws Exception {
        String viewId = "view:test-001";
        metastore.put(
                ("view:" + viewId).getBytes(StandardCharsets.UTF_8),
                "{\"content\":\"test\"}".getBytes(StandardCharsets.UTF_8));

        int agentCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(agentCount);
        Set<String> semanticIds = Collections.synchronizedSet(new HashSet<>());

        for (int i = 0; i < agentCount; i++) {
            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    Semantic sem =
                                            cognitionEngine.deriveSemantic(
                                                    List.of(viewId),
                                                    Semantic.SemanticType.CONCEPT,
                                                    Map.of());
                                    semanticIds.add(sem.semanticId());
                                } catch (Exception e) {
                                    // ignore
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);

        assertEquals(
                agentCount,
                semanticIds.size(),
                "All agents should produce unique semantic IDs (UUID-based, no conflict)");
    }

    @Test
    void c5_chainAppendCas_noLostEntries() throws Exception {
        byte[] chainKey = "deriv_chain:test-entity".getBytes(StandardCharsets.UTF_8);

        int appendCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(appendCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < appendCount; i++) {
            final int idx = i;
            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (int attempt = 0; attempt < 8; attempt++) {
                                        Optional<byte[]> current = metastore.get(chainKey);
                                        List<String> list =
                                                current.isEmpty()
                                                        ? new ArrayList<>()
                                                        : new ArrayList<>(
                                                                Arrays.asList(
                                                                        new String(
                                                                                        current
                                                                                                .get(),
                                                                                        StandardCharsets
                                                                                                .UTF_8)
                                                                                .split(",")));
                                        list.add("chain-" + idx);
                                        byte[] newValue =
                                                String.join(",", list)
                                                        .getBytes(StandardCharsets.UTF_8);

                                        List<Precondition> preconditions =
                                                current.isPresent()
                                                        ? List.of(
                                                                new Precondition.ValueEquals(
                                                                        chainKey, current.get()))
                                                        : List.of(
                                                                new Precondition.NotExists(
                                                                        chainKey));
                                        boolean success =
                                                metastore.compareAndSwap(
                                                        preconditions,
                                                        List.of(
                                                                new Mutation.Put(
                                                                        chainKey, newValue)));
                                        if (success) {
                                            successCount.incrementAndGet();
                                            break;
                                        }
                                        Thread.sleep(10);
                                    }
                                } catch (Exception e) {
                                    // ignore
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);

        Optional<byte[]> result = metastore.get(chainKey);
        assertTrue(result.isPresent(), "Chain should exist after appends");
        String[] entries = new String(result.get(), StandardCharsets.UTF_8).split(",");
        assertEquals(
                appendCount,
                entries.length,
                "All chain entries should be present (no lost appends)");
    }

    @Test
    void casRetryHelper_succeedsOnNoContention() throws Exception {
        byte[] key = "test:cas:basic".getBytes(StandardCharsets.UTF_8);
        metastore.put(key, "v1".getBytes(StandardCharsets.UTF_8));

        boolean result =
                CasRetryHelper.casWithRetry(
                        metastore,
                        List.of(
                                new Precondition.ValueEquals(
                                        key, "v1".getBytes(StandardCharsets.UTF_8))),
                        List.of(new Mutation.Put(key, "v2".getBytes(StandardCharsets.UTF_8))),
                        "test:cas:basic",
                        3,
                        1,
                        10);

        assertTrue(result, "CAS should succeed on first attempt when no contention");
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), metastore.get(key).orElse(null));
    }

    @Test
    void casRetryHelper_failsAfterRetries() throws Exception {
        byte[] key = "test:cas:fail".getBytes(StandardCharsets.UTF_8);
        metastore.put(key, "v1".getBytes(StandardCharsets.UTF_8));

        boolean result =
                CasRetryHelper.casWithRetry(
                        metastore,
                        List.of(
                                new Precondition.ValueEquals(
                                        key, "wrong-expected".getBytes(StandardCharsets.UTF_8))),
                        List.of(new Mutation.Put(key, "v2".getBytes(StandardCharsets.UTF_8))),
                        "test:cas:fail",
                        3,
                        1,
                        10);

        assertFalse(result, "CAS should fail when precondition never matches");
    }

    @Test
    void c1_intentKeyCleanedOnFailure() throws Exception {
        String rawId = "raw:cleanup-test-001";
        byte[] intentKey = ("derive_intent:CHUNK:" + rawId).getBytes(StandardCharsets.UTF_8);

        boolean claimed =
                metastore.compareAndSwap(
                        List.of(new Precondition.NotExists(intentKey)),
                        List.of(
                                new Mutation.Put(
                                        intentKey, "agent:test".getBytes(StandardCharsets.UTF_8))));
        assertTrue(claimed, "First claim should succeed");

        metastore.delete(intentKey);

        boolean reclaimed =
                metastore.compareAndSwap(
                        List.of(new Precondition.NotExists(intentKey)),
                        List.of(
                                new Mutation.Put(
                                        intentKey,
                                        "agent:test2".getBytes(StandardCharsets.UTF_8))));
        assertTrue(reclaimed, "After cleanup, intent key should be reclaimable");
    }
}
