package io.github.rawwise.engine.storage.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.block.read.LmdbDataBlockReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("RangeIndex E2E\u6D4B\u8BD5")
class RangeIndexE2ETest {

    private RangeIndex rangeIndex;

    private LmdbInstance mockInstance(String id, byte[] minKey, byte[] maxKey) {
        LmdbInstance inst = mock(LmdbInstance.class);
        when(inst.instanceId()).thenReturn(id);
        when(inst.minKey()).thenReturn(minKey);
        when(inst.maxKey()).thenReturn(maxKey);
        when(inst.readOnly()).thenReturn(true);
        return inst;
    }

    private LmdbInstance mockWritableInstance(String id, byte[] minKey, byte[] maxKey) {
        LmdbInstance inst = mock(LmdbInstance.class);
        when(inst.instanceId()).thenReturn(id);
        when(inst.minKey()).thenReturn(minKey);
        when(inst.maxKey()).thenReturn(maxKey);
        when(inst.readOnly()).thenReturn(false);
        return inst;
    }

    private byte[] k(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private List<LmdbInstance> invokeResolveInstancesWithRangeIndex(
            LmdbDataBlockReader reader, byte[] key) {
        try {
            Method m =
                    LmdbDataBlockReader.class.getDeclaredMethod(
                            "resolveInstancesWithRangeIndex", byte[].class);
            m.setAccessible(true);
            return (List<LmdbInstance>) m.invoke(reader, new Object[] {key});
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke resolveInstancesWithRangeIndex", e);
        }
    }

    @BeforeEach
    void setUp() {
        rangeIndex = new RangeIndex();
    }

    @Nested
    @DisplayName("RangeIndex\u6CE8\u518C\u4E0E\u70B9\u67E5\u67E5\u627E")
    class RegisterAndLookupTest {

        @Test
        @DisplayName(
                "\u6CE8\u518C\u591A\u4E2A\u5B9E\u4F8B\u540E\u70B9\u67E5\u8FD4\u56DE\u6B63\u786E\u5019\u9009\u96C6")
        void testRegisterMultipleInstancesAndLookup() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));
            LmdbInstance instC = mockInstance("inst-c", k("ggg"), k("iii"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            rangeIndex.register(instC);

            assertThat(rangeIndex.size()).isEqualTo(3);
            assertThat(rangeIndex.isEmpty()).isFalse();

            List<String> candidates = rangeIndex.lookup(k("eee"));
            assertThat(candidates).contains("inst-b");
        }

        @Test
        @DisplayName(
                "\u70B9\u67E5\u7CBE\u786E\u5339\u914DminKey\u8FD4\u56DE\u5BF9\u5E94\u5B9E\u4F8B")
        void testLookupExactMinKey() {
            LmdbInstance instA = mockInstance("inst-a", k("100"), k("200"));
            rangeIndex.register(instA);

            List<String> candidates = rangeIndex.lookup(k("100"));
            assertThat(candidates).contains("inst-a");
        }

        @Test
        @DisplayName(
                "\u70B9\u67E5\u5728\u4E24\u4E2A\u5B9E\u4F8B\u8FB9\u754C\u4E0A\u8FD4\u56DE\u6B63\u786E\u5019\u9009")
        void testLookupAtBoundaryBetweenInstances() {
            LmdbInstance instA = mockInstance("inst-a", k("a"), k("c"));
            LmdbInstance instB = mockInstance("inst-b", k("d"), k("f"));
            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookup(k("d"));
            assertThat(candidates).contains("inst-b");
        }

        @Test
        @DisplayName("point query returnspackage keyofinstance(maxKey)")
        void testLookupReturnsOnlyInstancesContainingTargetKey() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));
            LmdbInstance instC = mockInstance("inst-c", k("ggg"), k("iii"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            rangeIndex.register(instC);

            List<String> candidates = rangeIndex.lookup(k("eee"));
            assertThat(candidates).containsExactly("inst-b");
        }
    }

    @Nested
    @DisplayName("RangeIndex\u6CE8\u9500\u5B9E\u4F8B")
    class UnregisterTest {

        @Test
        @DisplayName(
                "\u6CE8\u9500\u5B9E\u4F8B\u540E\u67E5\u627E\u4E0D\u518D\u8FD4\u56DE\u8BE5\u5B9E\u4F8B")
        void testUnregisterRemovesInstanceFromLookup() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            assertThat(rangeIndex.size()).isEqualTo(2);

            rangeIndex.unregister("inst-a");
            assertThat(rangeIndex.size()).isEqualTo(1);

            List<String> candidates = rangeIndex.lookup(k("bbb"));
            assertThat(candidates).doesNotContain("inst-a");
        }

        @Test
        @DisplayName(
                "\u6CE8\u9500\u4E0D\u5B58\u5728\u7684\u5B9E\u4F8B\u4E0D\u4F1A\u5F71\u54CD\u5DF2\u6709\u7D22\u5F15")
        void testUnregisterNonExistentInstanceIsNoOp() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            rangeIndex.register(instA);

            rangeIndex.unregister("inst-nonexistent");
            assertThat(rangeIndex.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("\u6CE8\u9500\u6240\u6709\u5B9E\u4F8B\u540E\u7D22\u5F15\u4E3A\u7A7A")
        void testUnregisterAllLeavesEmptyIndex() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            rangeIndex.unregister("inst-a");
            rangeIndex.unregister("inst-b");

            assertThat(rangeIndex.isEmpty()).isTrue();
            assertThat(rangeIndex.size()).isZero();
        }
    }

    @Nested
    @DisplayName("RangeIndex\u91CD\u53E0\u533A\u95F4\u70B9\u67E5")
    class OverlappingRangePointLookupTest {

        @Test
        @DisplayName(
                "\u76EE\u6807key\u5728\u591A\u4E2A\u5B9E\u4F8B\u8303\u56F4\u5185\u65F6lookup\u8FD4\u56DEfloor\u53CA\u540E\u7EED\u5B9E\u4F8B")
        void testOverlappingRangesLookupReturnsFloorAndTail() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("eee"));
            LmdbInstance instB = mockInstance("inst-b", k("ccc"), k("ggg"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookup(k("ddd"));
            assertThat(candidates).contains("inst-a", "inst-b");
        }

        @Test
        @DisplayName(
                "\u76EE\u6807key\u5C0F\u4E8E\u6240\u6709minKey\u65F6lookup\u8FD4\u56DE\u7A7A\u5217\u8868")
        void testTargetKeyBeforeAllMinKeysReturnsEmpty() {
            LmdbInstance instA = mockInstance("inst-a", k("mmm"), k("ppp"));
            LmdbInstance instB = mockInstance("inst-b", k("qqq"), k("zzz"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookup(k("aaa"));
            assertThat(candidates).isEmpty();
        }

        @Test
        @DisplayName("key allminKey lookup package thekeyofinstance")
        void testTargetKeyAfterAllMinKeysReturnsMatchingInstance() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookup(k("zzz"));
            assertThat(candidates).isEmpty();
        }
    }

    @Nested
    @DisplayName("RangeIndex\u4E0D\u91CD\u53E0\u533A\u95F4\u70B9\u67E5")
    class NonOverlappingRangePointLookupTest {

        @Test
        @DisplayName("non-overlapping interval point query returns instance containing target key")
        void testNonOverlappingRangesLookupReturnsMatchingInstances() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));
            LmdbInstance instC = mockInstance("inst-c", k("ggg"), k("iii"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            rangeIndex.register(instC);

            List<String> candidates = rangeIndex.lookup(k("eee"));
            assertThat(candidates).containsExactly("inst-b");
            assertThat(candidates).doesNotContain("inst-a", "inst-c");
        }

        @Test
        @DisplayName(
                "\u70B9\u67E5\u7CBE\u786E\u5339\u914D\u67D0\u4E2A\u5B9E\u4F8B\u7684minKey\u65F6\u8FD4\u56DE\u8BE5\u5B9E\u4F8B\u53CA\u540E\u7EED")
        void testPointQueryExactMinKeyMatch() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookup(k("ddd"));
            assertThat(candidates).contains("inst-b");
        }
    }

    @Nested
    @DisplayName("RangeIndex\u7A7A\u7D22\u5F15")
    class EmptyIndexTest {

        @Test
        @DisplayName("\u7A7ARangeIndex\u70B9\u67E5\u8FD4\u56DE\u7A7A\u5217\u8868")
        void testEmptyIndexLookupReturnsEmptyList() {
            assertThat(rangeIndex.isEmpty()).isTrue();
            assertThat(rangeIndex.size()).isZero();

            List<String> candidates = rangeIndex.lookup(k("any"));
            assertThat(candidates).isEmpty();
        }

        @Test
        @DisplayName("\u7A7ARangeIndex\u8303\u56F4\u67E5\u627E\u8FD4\u56DE\u7A7A\u5217\u8868")
        void testEmptyIndexLookupRangeReturnsEmptyList() {
            List<String> candidates = rangeIndex.lookupRange(k("aaa"), k("zzz"));
            assertThat(candidates).isEmpty();
        }

        @Test
        @DisplayName("clear\u540E\u7D22\u5F15\u4E3A\u7A7A")
        void testClearMakesIndexEmpty() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            rangeIndex.register(instA);
            assertThat(rangeIndex.isEmpty()).isFalse();

            rangeIndex.clear();
            assertThat(rangeIndex.isEmpty()).isTrue();
            assertThat(rangeIndex.size()).isZero();
            assertThat(rangeIndex.lookup(k("bbb"))).isEmpty();
        }
    }

    @Nested
    @DisplayName("RangeIndex\u7A7A\u503CminKey/maxKey\u5904\u7406")
    class NullKeyRangeTest {

        @Test
        @DisplayName("minKey\u4E3Anull\u65F6\u6CE8\u518C\u4E3A\u7A7A\u64CD\u4F5C")
        void testRegisterWithNullMinKeyIsNoOp() {
            LmdbInstance inst = mockInstance("inst-null-min", null, k("zzz"));
            rangeIndex.register(inst);

            assertThat(rangeIndex.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("maxKey\u4E3Anull\u65F6\u6CE8\u518C\u4E3A\u7A7A\u64CD\u4F5C")
        void testRegisterWithNullMaxKeyIsNoOp() {
            LmdbInstance inst = mockInstance("inst-null-max", k("aaa"), null);
            rangeIndex.register(inst);

            assertThat(rangeIndex.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("minKey\u548CmaxKey\u5747\u4E3Anull\u65F6\u6CE8\u518C\u4E3A\u7A7A\u64CD\u4F5C")
        void testRegisterWithBothNullKeysIsNoOp() {
            LmdbInstance inst = mockInstance("inst-null-both", null, null);
            rangeIndex.register(inst);

            assertThat(rangeIndex.isEmpty()).isTrue();
        }
    }

    @Nested
    @DisplayName("RangeIndex\u5E76\u53D1\u6CE8\u518C\u4E0E\u6CE8\u9500")
    class ConcurrentRegisterUnregisterTest {

        @Test
        @DisplayName("\u591A\u7EBF\u7A0B\u5E76\u53D1\u6CE8\u518C\u65E0\u6570\u636E\u4E22\u5931")
        void testConcurrentRegisterNoDataLoss() throws Exception {
            int instanceCount = 100;
            int threadCount = 10;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicBoolean hasError = new AtomicBoolean(false);

            List<LmdbInstance> instances = new ArrayList<>();
            for (int i = 0; i < instanceCount; i++) {
                String key = String.format("%05d", i);
                instances.add(mockInstance("inst-" + i, k(key), k(key + "z")));
            }

            int perThread = instanceCount / threadCount;
            for (int t = 0; t < threadCount; t++) {
                final int start = t * perThread;
                final int end = (t == threadCount - 1) ? instanceCount : start + perThread;
                new Thread(
                                () -> {
                                    try {
                                        startLatch.await();
                                        for (int i = start; i < end; i++) {
                                            rangeIndex.register(instances.get(i));
                                        }
                                    } catch (Exception e) {
                                        hasError.set(true);
                                    } finally {
                                        doneLatch.countDown();
                                    }
                                })
                        .start();
            }

            startLatch.countDown();
            boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).isTrue();
            assertThat(hasError.get()).isFalse();
            assertThat(rangeIndex.size()).isEqualTo(instanceCount);
        }

        @Test
        @DisplayName(
                "\u591A\u7EBF\u7A0B\u5E76\u53D1\u6CE8\u518C\u548C\u6CE8\u9500\u65E0\u5F02\u5E38")
        void testConcurrentRegisterAndUnregisterNoException() throws Exception {
            int instanceCount = 50;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(2);
            AtomicBoolean hasError = new AtomicBoolean(false);

            List<LmdbInstance> instances = new ArrayList<>();
            for (int i = 0; i < instanceCount; i++) {
                String key = String.format("%05d", i);
                instances.add(mockInstance("inst-" + i, k(key), k(key + "z")));
            }

            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (LmdbInstance inst : instances) {
                                        rangeIndex.register(inst);
                                    }
                                } catch (Exception e) {
                                    hasError.set(true);
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();

            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (int i = 0; i < instanceCount; i++) {
                                        rangeIndex.unregister("inst-" + i);
                                    }
                                } catch (Exception e) {
                                    hasError.set(true);
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();

            startLatch.countDown();
            boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).isTrue();
            assertThat(hasError.get()).isFalse();
        }

        @Test
        @DisplayName(
                "\u591A\u7EBF\u7A0B\u5E76\u53D1\u67E5\u627E\u4E0E\u6CE8\u518C\u65E0\u5F02\u5E38")
        void testConcurrentLookupAndRegister() throws Exception {
            int registerCount = 50;
            int lookupCount = 200;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(2);
            AtomicBoolean hasError = new AtomicBoolean(false);
            AtomicInteger successfulLookups = new AtomicInteger(0);

            List<LmdbInstance> instances = new ArrayList<>();
            for (int i = 0; i < registerCount; i++) {
                String key = String.format("%05d", i);
                instances.add(mockInstance("inst-" + i, k(key), k(key + "z")));
            }

            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (LmdbInstance inst : instances) {
                                        rangeIndex.register(inst);
                                    }
                                } catch (Exception e) {
                                    hasError.set(true);
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();

            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (int i = 0; i < lookupCount; i++) {
                                        String key = String.format("%05d", i % registerCount);
                                        List<String> result = rangeIndex.lookup(k(key));
                                        if (!result.isEmpty()) {
                                            successfulLookups.incrementAndGet();
                                        }
                                    }
                                } catch (Exception e) {
                                    hasError.set(true);
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();

            startLatch.countDown();
            boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).isTrue();
            assertThat(hasError.get()).isFalse();
        }
    }

    @Nested
    @DisplayName("RangeIndex\u8303\u56F4\u67E5\u627E")
    class LookupRangeTest {

        @Test
        @DisplayName("find minKeyin[startKey,endKey]within rangeofinstance")
        void testLookupRangeReturnsInstancesWithMinKeyInRange() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));
            LmdbInstance instC = mockInstance("inst-c", k("ggg"), k("iii"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            rangeIndex.register(instC);

            List<String> candidates = rangeIndex.lookupRange(k("bbb"), k("eee"));
            assertThat(candidates).contains("inst-a", "inst-b");
        }

        @Test
        @DisplayName(
                "\u8303\u56F4\u67E5\u627E\u8986\u76D6\u6240\u6709\u5B9E\u4F8B\u65F6\u8FD4\u56DE\u5168\u90E8")
        void testLookupRangeCoversAllInstances() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));
            LmdbInstance instC = mockInstance("inst-c", k("ggg"), k("iii"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            rangeIndex.register(instC);

            List<String> candidates = rangeIndex.lookupRange(k("aaa"), k("zzz"));
            assertThat(candidates).containsExactlyInAnyOrder("inst-a", "inst-b", "inst-c");
        }

        @Test
        @DisplayName(
                "\u8303\u56F4\u67E5\u627E\u4EC5\u5305\u542B\u4E00\u4E2A\u5B9E\u4F8B\u65F6\u8FD4\u56DE\u8BE5\u5B9E\u4F8B")
        void testLookupRangeSingleInstanceOnly() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));

            rangeIndex.register(instA);

            List<String> candidates = rangeIndex.lookupRange(k("aaa"), k("bbb"));
            assertThat(candidates).containsExactly("inst-a");
        }

        @Test
        @DisplayName(
                "\u8303\u56F4\u67E5\u627E\u4E0E\u5B9E\u4F8B\u8FB9\u754C\u7CBE\u786E\u5339\u914D")
        void testLookupRangeExactBoundary() {
            LmdbInstance instA = mockInstance("inst-a", k("100"), k("200"));
            rangeIndex.register(instA);

            List<String> candidates = rangeIndex.lookupRange(k("100"), k("200"));
            assertThat(candidates).contains("inst-a");
        }

        @Test
        @DisplayName("\u8303\u56F4\u67E5\u627EstartKey\u4E3Anull\u65F6\u4ECE\u5934\u5F00\u59CB")
        void testLookupRangeNullStartKey() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookupRange(null, k("eee"));
            assertThat(candidates).contains("inst-a", "inst-b");
        }

        @Test
        @DisplayName("findendKeyisnull to")
        void testLookupRangeNullEndKey() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookupRange(k("bbb"), null);
            assertThat(candidates).contains("inst-a", "inst-b");
        }

        @Test
        @DisplayName(
                "\u8303\u56F4\u67E5\u627E\u4E24\u4E2Akey\u5747\u4E3Anull\u65F6\u8FD4\u56DE\u5168\u90E8\u5B9E\u4F8B")
        void testLookupRangeBothNullKeys() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookupRange(null, null);
            assertThat(candidates).containsExactlyInAnyOrder("inst-a", "inst-b");
        }
    }

    @Nested
    @DisplayName("LmdbDataBlockReader resolveInstancesWithRangeIndex")
    class ResolveInstancesWithRangeIndexTest {

        @Test
        @DisplayName("RangeIndex\u4E3A\u7A7A\u65F6\u8FD4\u56DE\u6240\u6709\u5B9E\u4F8B\u964D\u5E8F")
        void testResolveWithEmptyRangeIndexReturnsAllInstances() {
            TableStore tableStore = mock(TableStore.class);
            RangeIndex emptyIndex = new RangeIndex();
            when(tableStore.getRangeIndex()).thenReturn(emptyIndex);

            LmdbInstance writableInst = mockWritableInstance("w-1", k("aaa"), k("zzz"));
            when(tableStore.getWritableInstances()).thenReturn(List.of(writableInst));
            when(tableStore.getAllInstancesDescending()).thenReturn(List.of(writableInst));

            LmdbDataBlockReader reader = new LmdbDataBlockReader(tableStore);
            List<LmdbInstance> resolved = invokeResolveInstancesWithRangeIndex(reader, k("bbb"));

            assertThat(resolved).contains(writableInst);
        }

        @Test
        @DisplayName(
                "\u70B9\u67E5\u8FD4\u56DE\u5339\u914D\u7684readOnly\u5B9E\u4F8B\u548C\u6240\u6709writable\u5B9E\u4F8B")
        void testResolveReturnsMatchingReadOnlyAndAllWritable() {
            TableStore tableStore = mock(TableStore.class);
            RangeIndex ri = new RangeIndex();

            LmdbInstance readOnlyInst = mockInstance("ro-1", k("aaa"), k("ccc"));
            ri.register(readOnlyInst);

            LmdbInstance writableInst = mockWritableInstance("w-1", k("mmm"), k("zzz"));

            when(tableStore.getRangeIndex()).thenReturn(ri);
            when(tableStore.getWritableInstances()).thenReturn(List.of(writableInst));
            when(tableStore.getInstance("ro-1")).thenReturn(readOnlyInst);

            LmdbDataBlockReader reader = new LmdbDataBlockReader(tableStore);
            List<LmdbInstance> resolved = invokeResolveInstancesWithRangeIndex(reader, k("bbb"));

            assertThat(resolved).contains(writableInst, readOnlyInst);
        }

        @Test
        @DisplayName(
                "\u70B9\u67E5key\u4E0D\u5728readOnly\u5B9E\u4F8B\u8303\u56F4\u5185\u65F6\u4EC5\u8FD4\u56DEwritable\u5B9E\u4F8B")
        void testResolveKeyNotInReadOnlyRangeReturnsOnlyWritable() {
            TableStore tableStore = mock(TableStore.class);
            RangeIndex ri = new RangeIndex();

            LmdbInstance readOnlyInst = mockInstance("ro-1", k("aaa"), k("ccc"));
            ri.register(readOnlyInst);

            LmdbInstance writableInst = mockWritableInstance("w-1", k("mmm"), k("zzz"));

            when(tableStore.getRangeIndex()).thenReturn(ri);
            when(tableStore.getWritableInstances()).thenReturn(List.of(writableInst));
            when(tableStore.getInstance("ro-1")).thenReturn(readOnlyInst);

            LmdbDataBlockReader reader = new LmdbDataBlockReader(tableStore);
            List<LmdbInstance> resolved = invokeResolveInstancesWithRangeIndex(reader, k("zzz"));

            assertThat(resolved).contains(writableInst);
            assertThat(resolved).doesNotContain(readOnlyInst);
        }

        @Test
        @DisplayName(
                "\u591A\u4E2AreadOnly\u5B9E\u4F8B\u91CD\u53E0\u65F6resolveInstancesWithRangeIndex\u8FD4\u56DEKeyRange\u8FC7\u6EE4\u540E\u7684\u5B9E\u4F8B")
        void testResolveMultipleOverlappingReadOnlyInstances() {
            TableStore tableStore = mock(TableStore.class);
            RangeIndex ri = new RangeIndex();

            LmdbInstance ro1 = mockInstance("ro-1", k("aaa"), k("eee"));
            LmdbInstance ro2 = mockInstance("ro-2", k("ddd"), k("hhh"));
            ri.register(ro1);
            ri.register(ro2);

            LmdbInstance writableInst = mockWritableInstance("w-1", k("iii"), k("zzz"));

            when(tableStore.getRangeIndex()).thenReturn(ri);
            when(tableStore.getWritableInstances()).thenReturn(List.of(writableInst));
            when(tableStore.getInstance("ro-1")).thenReturn(ro1);
            when(tableStore.getInstance("ro-2")).thenReturn(ro2);

            LmdbDataBlockReader reader = new LmdbDataBlockReader(tableStore);
            List<LmdbInstance> resolved = invokeResolveInstancesWithRangeIndex(reader, k("eee"));

            assertThat(resolved).contains(writableInst, ro2);
        }

        @Test
        @DisplayName("\u5B9E\u4F8B\u6309instanceId\u964D\u5E8F\u6392\u5217")
        void testResolvedInstancesSortedByInstanceIdDescending() {
            TableStore tableStore = mock(TableStore.class);
            RangeIndex ri = new RangeIndex();

            LmdbInstance ro1 = mockInstance("ro-alpha", k("aaa"), k("zzz"));
            LmdbInstance ro2 = mockInstance("ro-beta", k("bbb"), k("zzz"));
            ri.register(ro1);
            ri.register(ro2);

            when(tableStore.getRangeIndex()).thenReturn(ri);
            when(tableStore.getWritableInstances()).thenReturn(Collections.emptyList());
            when(tableStore.getInstance("ro-alpha")).thenReturn(ro1);
            when(tableStore.getInstance("ro-beta")).thenReturn(ro2);

            LmdbDataBlockReader reader = new LmdbDataBlockReader(tableStore);
            List<LmdbInstance> resolved = invokeResolveInstancesWithRangeIndex(reader, k("mmm"));

            assertThat(resolved).isNotEmpty();
            List<String> ids = resolved.stream().map(LmdbInstance::instanceId).toList();
            for (int i = 1; i < ids.size(); i++) {
                assertThat(ids.get(i - 1).compareTo(ids.get(i))).isGreaterThan(0);
            }
        }

        @Test
        @DisplayName(
                "RangeIndex\u5019\u9009\u4E2D\u7684\u5B9E\u4F8B\u5728TableStore\u4E2D\u4E0D\u5B58\u5728\u65F6\u88AB\u8FC7\u6EE4")
        void testResolveFiltersOutMissingInstances() {
            TableStore tableStore = mock(TableStore.class);
            RangeIndex ri = new RangeIndex();

            LmdbInstance ro1 = mockInstance("ro-1", k("aaa"), k("zzz"));
            ri.register(ro1);

            when(tableStore.getRangeIndex()).thenReturn(ri);
            when(tableStore.getWritableInstances()).thenReturn(Collections.emptyList());
            when(tableStore.getInstance("ro-1")).thenReturn(null);

            LmdbDataBlockReader reader = new LmdbDataBlockReader(tableStore);
            List<LmdbInstance> resolved = invokeResolveInstancesWithRangeIndex(reader, k("mmm"));

            assertThat(resolved).isEmpty();
        }

        @Test
        @DisplayName(
                "writable\u5B9E\u4F8B\u4E0D\u88ABRangeIndex\u5019\u9009\u91CD\u590D\u6DFB\u52A0")
        void testWritableInstancesNotDuplicatedFromRangeIndexCandidates() {
            TableStore tableStore = mock(TableStore.class);
            RangeIndex ri = new RangeIndex();

            LmdbInstance roInst = mockInstance("ro-1", k("aaa"), k("zzz"));
            ri.register(roInst);

            LmdbInstance writableInst = mockWritableInstance("w-1", k("aaa"), k("zzz"));

            when(tableStore.getRangeIndex()).thenReturn(ri);
            when(tableStore.getWritableInstances()).thenReturn(List.of(writableInst));
            when(tableStore.getInstance("ro-1")).thenReturn(roInst);

            LmdbDataBlockReader reader = new LmdbDataBlockReader(tableStore);
            List<LmdbInstance> resolved = invokeResolveInstancesWithRangeIndex(reader, k("mmm"));

            long writableCount =
                    resolved.stream().filter(i -> i.instanceId().equals("w-1")).count();
            assertThat(writableCount).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("test: maxKey + rebuildconsistency + lookupRangecorrect")
    class ReviewSupplementTests {

        @Test
        @DisplayName("lookupfor package keyofinstance maxKey")
        void testLookupMaxKeyPruning() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("bbb"));
            LmdbInstance instB = mockInstance("inst-b", k("ccc"), k("ddd"));
            LmdbInstance instC = mockInstance("inst-c", k("eee"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            rangeIndex.register(instC);

            assertThat(rangeIndex.lookup(k("eee"))).containsExactly("inst-c");
            assertThat(rangeIndex.lookup(k("ddd"))).containsExactly("inst-b");
            assertThat(rangeIndex.lookup(k("bbb"))).containsExactly("inst-a");
            assertThat(rangeIndex.lookup(k("zzz"))).isEmpty();
        }

        @Test
        @DisplayName("lookup allpackage keyofinstance")
        void testLookupOverlappingRanges() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("eee"));
            LmdbInstance instB = mockInstance("inst-b", k("ccc"), k("ggg"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookup(k("ddd"));
            assertThat(candidates).containsExactlyInAnyOrder("inst-a", "inst-b");

            List<String> candidatesA = rangeIndex.lookup(k("bbb"));
            assertThat(candidatesA).containsExactly("inst-a");
            assertThat(candidatesA).doesNotContain("inst-b");
        }

        @Test
        @DisplayName("rebuildafterindexand itemregisterconsistent")
        void testRebuildConsistency() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("ccc"));
            LmdbInstance instB = mockInstance("inst-b", k("ddd"), k("fff"));
            LmdbInstance instC = mockInstance("inst-c", k("ggg"), k("iii"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            rangeIndex.register(instC);

            RangeIndex rebuilt =
                    RangeIndex.rebuild(List.of(instA, instB, instC), Collections.emptyList());

            assertThat(rebuilt.lookup(k("bbb"))).containsExactly("inst-a");
            assertThat(rebuilt.lookup(k("eee"))).containsExactly("inst-b");
            assertThat(rebuilt.lookup(k("hhh"))).containsExactly("inst-c");
            assertThat(rebuilt.lookup(k("zzz"))).isEmpty();
            assertThat(rebuilt.size()).isEqualTo(rangeIndex.size());
        }

        @Test
        @DisplayName("lookupRange maxKeyfilterstartKey beforeofinstance")
        void testLookupRangeStartKeyFiltering() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("eee"));
            LmdbInstance instB = mockInstance("inst-b", k("ccc"), k("ggg"));
            LmdbInstance instC = mockInstance("inst-c", k("fff"), k("iii"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);
            rangeIndex.register(instC);

            List<String> candidates = rangeIndex.lookupRange(k("ddd"), k("hhh"));
            assertThat(candidates).containsExactlyInAnyOrder("inst-a", "inst-b", "inst-c");
        }

        @Test
        @DisplayName("lookupRange startKeyfilter maxKey startKeyofinstance")
        void testLookupRangeStartKeyPrunesEarlyInstances() {
            LmdbInstance instA = mockInstance("inst-a", k("aaa"), k("bbb"));
            LmdbInstance instB = mockInstance("inst-b", k("ccc"), k("fff"));

            rangeIndex.register(instA);
            rangeIndex.register(instB);

            List<String> candidates = rangeIndex.lookupRange(k("ddd"), null);
            assertThat(candidates).containsExactly("inst-b");
            assertThat(candidates).doesNotContain("inst-a");
        }

        @Test
        @DisplayName("minKeyis empty bytes registeris operations")
        void testRegisterWithEmptyMinKeyIsNoOp() {
            LmdbInstance inst = mockInstance("inst-empty-min", new byte[0], k("zzz"));
            rangeIndex.register(inst);
            assertThat(rangeIndex.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("maxKeyis empty bytes registeris operations")
        void testRegisterWithEmptyMaxKeyIsNoOp() {
            LmdbInstance inst = mockInstance("inst-empty-max", k("aaa"), new byte[0]);
            rangeIndex.register(inst);
            assertThat(rangeIndex.isEmpty()).isTrue();
        }
    }
}
