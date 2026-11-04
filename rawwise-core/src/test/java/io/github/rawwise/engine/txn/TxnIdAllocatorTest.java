package io.github.rawwise.engine.txn;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TxnIdAllocatorTest {

    private Metastore store;
    private TxnIdAllocator allocator;

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryMetastore();
        allocator = new TxnIdAllocator(store, 100);
    }

    @Test
    void nextTxnId_returnsMonotonicallyIncreasing() throws IOException {
        long first = allocator.nextTxnId();
        long second = allocator.nextTxnId();
        long third = allocator.nextTxnId();
        assertTrue(first < second);
        assertTrue(second < third);
    }

    @Test
    void nextTxnId_acrossSegmentBoundary() throws IOException {
        TxnIdAllocator smallAllocator = new TxnIdAllocator(store, 5);
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            ids.add(smallAllocator.nextTxnId());
        }
        for (int i = 1; i < ids.size(); i++) {
            assertTrue(
                    ids.get(i) > ids.get(i - 1),
                    "IDs must be monotonically increasing: "
                            + ids.get(i - 1)
                            + " -> "
                            + ids.get(i));
        }
    }

    @Test
    void nextTxnId_concurrentAllocation() throws Exception {
        TxnIdAllocator smallAllocator = new TxnIdAllocator(store, 50);
        int threadCount = 8;
        int idsPerThread = 100;
        Set<Long> allIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < idsPerThread; i++) {
                                long id = smallAllocator.nextTxnId();
                                assertTrue(allIds.add(id), "Duplicate txnId: " + id);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
        assertEquals(threadCount * idsPerThread, allIds.size());
        executor.shutdown();
    }

    @Test
    void nextTxnId_segmentExhaustionTriggersNewSegment() throws IOException {
        TxnIdAllocator smallAllocator = new TxnIdAllocator(store, 3);
        long id1 = smallAllocator.nextTxnId();
        long id2 = smallAllocator.nextTxnId();
        long id3 = smallAllocator.nextTxnId();
        long id4 = smallAllocator.nextTxnId();
        assertTrue(id4 > id3, "After segment exhaustion, new segment should provide higher IDs");
    }

    @Test
    void constructor_rejectsNullMetastore() {
        assertThrows(
                IllegalArgumentException.class, () -> new TxnIdAllocator((Metastore) null, 100));
    }

    @Test
    void constructor_rejectsNonPositiveSegmentSize() throws IOException {
        Metastore s = new InMemoryMetastore();
        assertThrows(IllegalArgumentException.class, () -> new TxnIdAllocator(s, 0));
        assertThrows(IllegalArgumentException.class, () -> new TxnIdAllocator(s, -1));
    }
}
