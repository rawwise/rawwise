package io.github.rawwise.engine.storage.block.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RowGroupIndexAndPKIndexE2ETest {

    private RowGroupIndex rowGroupIndex;
    private PrimaryKeyIndex primaryKeyIndex;

    @BeforeEach
    void setUp() {
        rowGroupIndex =
                new RowGroupIndex(
                        List.of(
                                new RowGroupIndex.RowGroupEntry(k("aaa"), k("ccc"), 100, 0),
                                new RowGroupIndex.RowGroupEntry(k("ddd"), k("fff"), 200, 4096),
                                new RowGroupIndex.RowGroupEntry(k("ggg"), k("iii"), 150, 8192)));
        primaryKeyIndex = new PrimaryKeyIndex();
    }

    private static byte[] k(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Nested
    @DisplayName("RowGroupIndex E2Etest")
    class RowGroupIndexTests {

        @Test
        @DisplayName("point query returnspackage keyofRowGroup")
        void testCandidateRowGroupsReturnsMatchingRowGroups() {
            List<Integer> candidates = rowGroupIndex.candidateRowGroups(k("eee"));
            assertThat(candidates).containsExactly(1);
        }

        @Test
        @DisplayName("point querykeyinfirstRowGroupwithin range")
        void testCandidateRowGroupsFirstRowGroup() {
            List<Integer> candidates = rowGroupIndex.candidateRowGroups(k("bbb"));
            assertThat(candidates).containsExactly(0);
        }

        @Test
        @DisplayName("point querykey in RowGroupwithin range")
        void testCandidateRowGroupsNoMatch() {
            List<Integer> candidates = rowGroupIndex.candidateRowGroups(k("zzz"));
            assertThat(candidates).isEmpty();
        }

        @Test
        @DisplayName("anyRowGroupMightContainreturns truewhenhasmatchRowGroup")
        void testAnyRowGroupMightContainTrue() {
            assertThat(rowGroupIndex.anyRowGroupMightContain(k("eee"))).isTrue();
            assertThat(rowGroupIndex.anyRowGroupMightContain(k("bbb"))).isTrue();
            assertThat(rowGroupIndex.anyRowGroupMightContain(k("hhh"))).isTrue();
        }

        @Test
        @DisplayName("anyRowGroupMightContainreturns falsewhennomatchRowGroup")
        void testAnyRowGroupMightContainFalse() {
            assertThat(rowGroupIndex.anyRowGroupMightContain(k("zzz"))).isFalse();
        }

        @Test
        @DisplayName("RowGroupIndexofcandidateRowGroupsreturns empty list")
        void testEmptyIndexCandidateRowGroups() {
            RowGroupIndex empty = RowGroupIndex.empty();
            assertThat(empty.candidateRowGroups(k("aaa"))).isEmpty();
            assertThat(empty.anyRowGroupMightContain(k("aaa"))).isFalse();
        }

        @Test
        @DisplayName("RowGroupEntry mightContainboundarytest")
        void testRowGroupEntryMightContainBoundary() {
            RowGroupIndex.RowGroupEntry entry =
                    new RowGroupIndex.RowGroupEntry(k("aaa"), k("ccc"), 100, 0);
            assertThat(entry.mightContain(k("aaa"))).isTrue();
            assertThat(entry.mightContain(k("ccc"))).isTrue();
            assertThat(entry.mightContain(k("bbb"))).isTrue();
            assertThat(entry.mightContain(k("zzz"))).isFalse();
            assertThat(entry.mightContain(new byte[] {(byte) 0x00})).isFalse();
        }

        @Test
        @DisplayName("RowGroupEntry minKey/maxKey mightContainreturns true")
        void testRowGroupEntryEmptyKeyRangeReturnsTrue() {
            RowGroupIndex.RowGroupEntry entry =
                    new RowGroupIndex.RowGroupEntry(new byte[0], new byte[0], 100, 0);
            assertThat(entry.mightContain(k("anything"))).isTrue();
        }

        @Test
        @DisplayName("RowGroupEntrysingle nullconstruct test")
        void testRowGroupEntryNullConstructor() {
            RowGroupIndex.RowGroupEntry e1 = new RowGroupIndex.RowGroupEntry(null, null, 100, 0);
            assertThat(e1.mightContain(k("x"))).isTrue();

            RowGroupIndex.RowGroupEntry e2 =
                    new RowGroupIndex.RowGroupEntry(null, k("ccc"), 100, 0);
            assertThat(e2.mightContain(k("x"))).isTrue();

            RowGroupIndex.RowGroupEntry e3 =
                    new RowGroupIndex.RowGroupEntry(k("aaa"), null, 100, 0);
            assertThat(e3.mightContain(k("x"))).isTrue();
        }

        @Test
        @DisplayName("column and column consistency")
        void testSerializeDeserializeConsistency() throws IOException {
            byte[] serialized = rowGroupIndex.serialize();
            RowGroupIndex deserialized = RowGroupIndex.deserialize(serialized);

            assertThat(deserialized.size()).isEqualTo(rowGroupIndex.size());
            assertThat(deserialized.candidateRowGroups(k("bbb"))).containsExactly(0);
            assertThat(deserialized.candidateRowGroups(k("eee"))).containsExactly(1);
            assertThat(deserialized.candidateRowGroups(k("hhh"))).containsExactly(2);
            assertThat(deserialized.candidateRowGroups(k("zzz"))).isEmpty();

            assertThat(deserialized.entries().get(0).rowCount()).isEqualTo(100);
            assertThat(deserialized.entries().get(0).startPos()).isEqualTo(0);
            assertThat(deserialized.entries().get(1).rowCount()).isEqualTo(200);
            assertThat(deserialized.entries().get(1).startPos()).isEqualTo(4096);
        }

        @Test
        @DisplayName("index column and column")
        void testEmptySerializeDeserialize() throws IOException {
            RowGroupIndex empty = RowGroupIndex.empty();
            byte[] serialized = empty.serialize();
            RowGroupIndex deserialized = RowGroupIndex.deserialize(serialized);
            assertThat(deserialized.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("deserialize nullor datareturns emptyindex")
        void testDeserializeNullOrShort() throws IOException {
            assertThat(RowGroupIndex.deserialize(null).isEmpty()).isTrue();
            assertThat(RowGroupIndex.deserialize(new byte[0]).isEmpty()).isTrue();
            assertThat(RowGroupIndex.deserialize(new byte[1]).isEmpty()).isTrue();
        }

        @Test
        @DisplayName("deserialize data IOException")
        void testDeserializeCorruptedData() {
            byte[] corrupted = new byte[] {0, 0, 0, 10};
            try {
                RowGroupIndex.deserialize(corrupted);
            } catch (IOException e) {
                assertThat(e).isInstanceOf(java.io.EOFException.class);
            }
        }

        @Test
        @DisplayName("entries immutablecolumntable")
        void testEntriesReturnsImmutableList() {
            assertThatThrownBy(
                    () ->
                            rowGroupIndex
                                    .entries()
                                    .add(new RowGroupIndex.RowGroupEntry(k("x"), k("y"), 1, 0)),
                    UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("point query returnsmultipleRowGroup")
        void testOverlappingRowGroups() {
            RowGroupIndex overlapping =
                    new RowGroupIndex(
                            List.of(
                                    new RowGroupIndex.RowGroupEntry(k("aaa"), k("eee"), 100, 0),
                                    new RowGroupIndex.RowGroupEntry(
                                            k("ccc"), k("ggg"), 200, 4096)));
            List<Integer> candidates = overlapping.candidateRowGroups(k("ddd"));
            assertThat(candidates).containsExactlyInAnyOrder(0, 1);
        }
    }

    @Nested
    @DisplayName("PrimaryKeyIndex E2Etest")
    class PrimaryKeyIndexTests {

        @Test
        @DisplayName("putandlookup can")
        void testPutAndLookup() {
            primaryKeyIndex.put(k("key1"), "block-1", 0, 0L);
            primaryKeyIndex.put(k("key2"), "block-1", 1, 0L);

            List<PrimaryKeyIndex.PKLocation> loc1 = primaryKeyIndex.lookup(k("key1"));
            assertThat(loc1).hasSize(1);
            assertThat(loc1.get(0).blockId()).isEqualTo("block-1");
            assertThat(loc1.get(0).rowGroupIndex()).isEqualTo(0);
            assertThat(loc1.get(0).snapshotId()).isEqualTo(0L);

            List<PrimaryKeyIndex.PKLocation> loc2 = primaryKeyIndex.lookup(k("key2"));
            assertThat(loc2).hasSize(1);
            assertThat(loc2.get(0).rowGroupIndex()).isEqualTo(1);
        }

        @Test
        @DisplayName("lookupnon-existent keyreturns empty list")
        void testLookupNotFound() {
            List<PrimaryKeyIndex.PKLocation> result = primaryKeyIndex.lookup(k("nonexistent"));
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("mightContain keyis in")
        void testMightContain() {
            primaryKeyIndex.put(k("key1"), "block-1", 0, 0L);
            assertThat(primaryKeyIndex.mightContain(k("key1"))).isTrue();
            assertThat(primaryKeyIndex.mightContain(k("key2"))).isFalse();
        }

        @Test
        @DisplayName("differentbyte[]instances with same contentcancorrectly finds")
        void testDifferentByteArrayInstances() {
            byte[] key1 = new byte[] {1, 2, 3};
            byte[] key2 = new byte[] {1, 2, 3};
            primaryKeyIndex.put(key1, "block-1", 0, 0L);
            assertThat(primaryKeyIndex.lookup(key2)).hasSize(1);
        }

        @Test
        @DisplayName("samekeyindifferentblockin inmultiple")
        void testSameKeyMultipleBlocks() {
            primaryKeyIndex.put(k("key1"), "block-1", 0, 0L);
            primaryKeyIndex.put(k("key1"), "block-2", 1, 1L);

            List<PrimaryKeyIndex.PKLocation> locs = primaryKeyIndex.lookup(k("key1"));
            assertThat(locs).hasSize(2);
            assertThat(locs.stream().map(PrimaryKeyIndex.PKLocation::blockId))
                    .containsExactlyInAnyOrder("block-1", "block-2");
        }

        @Test
        @DisplayName("removeBlockdelete blockofallPK")
        void testRemoveBlock() {
            primaryKeyIndex.put(k("key1"), "block-1", 0, 0L);
            primaryKeyIndex.put(k("key2"), "block-1", 1, 0L);
            primaryKeyIndex.put(k("key1"), "block-2", 0, 1L);

            primaryKeyIndex.removeBlock("block-1");

            List<PrimaryKeyIndex.PKLocation> locs = primaryKeyIndex.lookup(k("key1"));
            assertThat(locs).hasSize(1);
            assertThat(locs.get(0).blockId()).isEqualTo("block-2");

            assertThat(primaryKeyIndex.lookup(k("key2"))).isEmpty();
        }

        @Test
        @DisplayName("removeBlocknon-existent blockId no exception")
        void testRemoveBlockNonexistent() {
            primaryKeyIndex.put(k("key1"), "block-1", 0, 0L);
            primaryKeyIndex.removeBlock("nonexistent");
            assertThat(primaryKeyIndex.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("removeBlockafterkeynoits remove")
        void testRemoveBlockCleansUpEmptyKeys() {
            primaryKeyIndex.put(k("key1"), "block-1", 0, 0L);
            primaryKeyIndex.removeBlock("block-1");
            assertThat(primaryKeyIndex.mightContain(k("key1"))).isFalse();
            assertThat(primaryKeyIndex.size()).isEqualTo(0);
        }

        @Test
        @DisplayName("clear allindex")
        void testClear() {
            primaryKeyIndex.put(k("key1"), "block-1", 0, 0L);
            primaryKeyIndex.put(k("key2"), "block-2", 1, 0L);
            primaryKeyIndex.clear();
            assertThat(primaryKeyIndex.isEmpty()).isTrue();
            assertThat(primaryKeyIndex.size()).isEqualTo(0);
        }

        @Test
        @DisplayName("lookup immutablecolumntable")
        void testLookupReturnsImmutableList() {
            primaryKeyIndex.put(k("key1"), "block-1", 0, 0L);
            List<PrimaryKeyIndex.PKLocation> locs = primaryKeyIndex.lookup(k("key1"));
            assertThatThrownBy(
                    () -> locs.add(new PrimaryKeyIndex.PKLocation("block-2", 0, 0L)),
                    UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("concurrentputandlookupdoes not throw exception")
        void testConcurrentPutAndLookup() throws InterruptedException {
            int threadCount = 8;
            int keysPerThread = 100;
            Thread[] threads = new Thread[threadCount];
            java.util.concurrent.CountDownLatch startLatch =
                    new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch doneLatch =
                    new java.util.concurrent.CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                threads[t] =
                        new Thread(
                                () -> {
                                    try {
                                        startLatch.await();
                                        for (int i = 0; i < keysPerThread; i++) {
                                            byte[] key = k("key-" + threadId + "-" + i);
                                            primaryKeyIndex.put(key, "block-" + threadId, i, 0L);
                                            primaryKeyIndex.mightContain(key);
                                            primaryKeyIndex.lookup(key);
                                        }
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    } finally {
                                        doneLatch.countDown();
                                    }
                                });
                threads[t].start();
            }

            startLatch.countDown();
            doneLatch.await();

            assertThat(primaryKeyIndex.size()).isEqualTo(threadCount * keysPerThread);
        }

        @Test
        @DisplayName("column and column mergeconsistency")
        void testSerializeDeserializeAndMerge() throws IOException {
            PrimaryKeyIndex source = new PrimaryKeyIndex();
            source.put(k("key1"), "block-1", 0, 10L);
            source.put(k("key1"), "block-2", 1, 20L);
            source.put(k("key2"), "block-1", 2, 10L);

            byte[] serialized = source.serialize();

            PrimaryKeyIndex target = new PrimaryKeyIndex();
            int loaded = target.deserializeAndMerge(serialized);
            assertThat(loaded).isEqualTo(2);

            List<PrimaryKeyIndex.PKLocation> locs1 = target.lookup(k("key1"));
            assertThat(locs1).hasSize(2);
            assertThat(locs1.stream().map(PrimaryKeyIndex.PKLocation::blockId))
                    .containsExactlyInAnyOrder("block-1", "block-2");
            assertThat(locs1.stream().map(PrimaryKeyIndex.PKLocation::snapshotId))
                    .containsExactlyInAnyOrder(10L, 20L);

            List<PrimaryKeyIndex.PKLocation> locs2 = target.lookup(k("key2"));
            assertThat(locs2).hasSize(1);
            assertThat(locs2.get(0).blockId()).isEqualTo("block-1");
            assertThat(locs2.get(0).rowGroupIndex()).isEqualTo(2);
        }

        @Test
        @DisplayName("deserializeAndMerge nullor data")
        void testDeserializeAndMergeNullOrEmpty() throws IOException {
            PrimaryKeyIndex pk = new PrimaryKeyIndex();
            assertThat(pk.deserializeAndMerge(null)).isEqualTo(0);
            assertThat(pk.deserializeAndMerge(new byte[0])).isEqualTo(0);
            assertThat(pk.deserializeAndMerge(new byte[1])).isEqualTo(0);
        }

        @Test
        @DisplayName("TableStore registerPrimaryKeyLocationandunregisterBlock")
        void testTableStoreRegisterAndUnregister() {
            io.github.rawwise.engine.storage.table.TableStore tableStore =
                    new io.github.rawwise.engine.storage.table.TableStore(
                            "test.db.pk_table", 1, java.util.List.of("id"));

            byte[] pk1 = k("user-001");
            byte[] pk2 = k("user-002");

            tableStore.registerPrimaryKeyLocation(pk1, "block-1", 0, 5L);
            tableStore.registerPrimaryKeyLocation(pk2, "block-1", 1, 5L);
            tableStore.registerPrimaryKeyLocation(pk1, "block-2", 0, 10L);

            PrimaryKeyIndex pkIndex = tableStore.getPrimaryKeyIndex();
            assertThat(pkIndex.size()).isEqualTo(2);

            List<PrimaryKeyIndex.PKLocation> locs = pkIndex.lookup(pk1);
            assertThat(locs).hasSize(2);

            tableStore.unregisterBlockFromPrimaryKeyIndex("block-1");

            List<PrimaryKeyIndex.PKLocation> afterRemove = pkIndex.lookup(pk1);
            assertThat(afterRemove).hasSize(1);
            assertThat(afterRemove.get(0).blockId()).isEqualTo("block-2");

            assertThat(pkIndex.lookup(pk2)).isEmpty();

            tableStore.close();
        }

        @Test
        @DisplayName("PKLocation snapshotId Time Travelquery")
        void testPKLocationSnapshotId() {
            primaryKeyIndex.put(k("key1"), "block-v1", 0, 100L);
            primaryKeyIndex.put(k("key1"), "block-v2", 0, 200L);

            List<PrimaryKeyIndex.PKLocation> locs = primaryKeyIndex.lookup(k("key1"));
            assertThat(locs).hasSize(2);
            assertThat(locs.get(0).snapshotId()).isEqualTo(100L);
            assertThat(locs.get(1).snapshotId()).isEqualTo(200L);
        }
    }

    private static void assertThatThrownBy(
            Runnable runnable, Class<? extends Exception> expectedType) {
        try {
            runnable.run();
            throw new AssertionError("Expected " + expectedType.getSimpleName() + " to be thrown");
        } catch (AssertionError e) {
            if (e.getMessage().startsWith("Expected ")
                    && e.getMessage().endsWith(" to be thrown")) {
                throw e;
            }
            if (!expectedType.isInstance(e)) {
                throw new AssertionError(
                        "Expected "
                                + expectedType.getSimpleName()
                                + " but got "
                                + e.getClass().getSimpleName());
            }
        } catch (Exception e) {
            if (!expectedType.isInstance(e)) {
                throw new AssertionError(
                        "Expected "
                                + expectedType.getSimpleName()
                                + " but got "
                                + e.getClass().getSimpleName());
            }
        }
    }
}
