package io.github.rawwise.frontend.ir.routing;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.frontend.backend.BackendEndpoint;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;

@DisplayName("RoutingTable")
class RoutingTableTest {

    private Metastore metastore;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
    }

    // ===============================================================
    // S1: routingtableinitialize + lookup
    // full chain: Metastore -> RoutingTableWriter.initialize() -> RoutingTableManager.load() ->
    // lookup()
    // ===============================================================

    @Test
    void testInitializeCreatesVersionAndEntries() throws IOException {
        String tableId = "default.t";

        RoutingTableWriter writer = new RoutingTableWriter(metastore);
        boolean ok =
                writer.initialize(
                        tableId,
                        List.of(new RoutingEntry(new KeyRange(0, 500), "group-1")),
                        new BackendEndpoint("group-1", "node1", 9090));
        assertThat(ok).isTrue();

        // verify version key in
        assertThat(metastore.get(RoutingKeys.versionKey(tableId))).isPresent();

        // verify default key in
        assertThat(metastore.get(RoutingKeys.defaultKey(tableId))).isPresent();

        // verify entry key in
        assertThat(metastore.get(RoutingKeys.entryKey(tableId, 0))).isPresent();
    }

    @Test
    void testInitializeIsIdempotent() throws IOException {
        String tableId = "default.t";
        RoutingTableWriter writer = new RoutingTableWriter(metastore);

        boolean first =
                writer.initialize(
                        tableId,
                        List.of(new RoutingEntry(new KeyRange(0, 500), "group-1")),
                        new BackendEndpoint("group-1", "node1", 9090));
        assertThat(first).isTrue();

        // No.secondaryinitializeshouldfail (NotExists)
        boolean second =
                writer.initialize(
                        tableId,
                        List.of(new RoutingEntry(new KeyRange(0, 1000), "group-2")),
                        new BackendEndpoint("group-2", "node2", 9090));
        assertThat(second).isFalse();

        // routingtable unchanged
        RoutingTableManager manager = new RoutingTableManager(metastore, 0);
        RoutingTable rt = manager.getRoutingTable(tableId);
        assertThat(rt).isNotNull();
        assertThat(rt.entries()).hasSize(1);
        assertThat(rt.entries().get(0).backendGroupId()).isEqualTo("group-1");
    }

    @Test
    void testLookupReturnsCorrectBackendGroup() throws IOException {
        String tableId = "default.t";
        RoutingTableWriter writer = new RoutingTableWriter(metastore);
        writer.initialize(
                tableId,
                List.of(
                        new RoutingEntry(new KeyRange(0, 500), "group-a"),
                        new RoutingEntry(new KeyRange(500, 1000), "group-b")),
                new BackendEndpoint("group-a", "node1", 9090));

        RoutingTableManager manager = new RoutingTableManager(metastore, 0);
        RoutingTable rt = manager.getRoutingTable(tableId);

        // match
        assertThat(rt.lookup(tableId, 0).groupId()).isEqualTo("group-a");
        assertThat(rt.lookup(tableId, 499).groupId()).isEqualTo("group-a");
        assertThat(rt.lookup(tableId, 500).groupId()).isEqualTo("group-b");
        assertThat(rt.lookup(tableId, 999).groupId()).isEqualTo("group-b");

        // fallback to defaultBackend
        assertThat(rt.lookup(tableId, 1000).groupId()).isEqualTo("group-a");
        assertThat(rt.lookup(tableId, -1).groupId()).isEqualTo("group-a");
    }

    // ===============================================================
    // S2: concurrent CAS entry
    // full chain: twoconcurrent writer -> Metastore CAS retry -> consistent
    // ===============================================================

    @Test
    void testConcurrentAddEntryProducesConsistentRoutingTable() throws Exception {
        String tableId = "default.t";
        RoutingTableWriter writer = new RoutingTableWriter(metastore);
        writer.initialize(
                tableId,
                List.of(new RoutingEntry(new KeyRange(0, 333), "group-1")),
                new BackendEndpoint("group-fallback", "node0", 9090));

        int concurrency = 3;
        CountDownLatch latch = new CountDownLatch(concurrency);
        AtomicBoolean[] successes = new AtomicBoolean[concurrency];

        var executor = Executors.newFixedThreadPool(concurrency);
        try {
            for (int i = 0; i < concurrency; i++) {
                final int idx = i;
                successes[idx] = new AtomicBoolean(false);
                executor.submit(
                        () -> {
                            try {
                                // same entry
                                boolean ok =
                                        new RoutingTableWriter(metastore)
                                                .addEntry(
                                                        tableId,
                                                        new RoutingEntry(
                                                                new KeyRange(
                                                                        333 * (idx + 1),
                                                                        333 * (idx + 2)),
                                                                "group-" + (idx + 2)));
                                successes[idx].set(ok);
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                latch.countDown();
                            }
                        });
            }
            latch.await();
        } finally {
            executor.shutdown();
        }

        // all allshouldsuccess (CAS retry)
        for (AtomicBoolean s : successes) {
            assertThat(s.get()).isTrue();
        }

        // routingtableshouldhas 1 (initial) + 3 (concurrent) = 4 entries
        RoutingTableManager manager = new RoutingTableManager(metastore, 0);
        RoutingTable rt = manager.getRoutingTable(tableId);
        assertThat(rt.entries()).hasSize(4);
        assertThat(rt.version()).isEqualTo(4); // version 4

        // all key-range becover
        for (int k = 0; k < 1332; k++) {
            assertThat(rt.lookup(tableId, k)).isNotNull();
        }
    }

    // ===============================================================
    // S3: routingtable entry updateanddelete
    // ===============================================================

    @Test
    void testUpdateEntryChangesBackendGroup() throws IOException {
        String tableId = "default.t";
        RoutingTableWriter writer = new RoutingTableWriter(metastore);
        writer.initialize(
                tableId,
                List.of(new RoutingEntry(new KeyRange(0, 500), "group-1")),
                new BackendEndpoint("group-1", "node1", 9090));

        // update entry:0 to Group
        boolean ok =
                writer.updateEntry(tableId, 0, new RoutingEntry(new KeyRange(0, 500), "group-new"));
        assertThat(ok).isTrue();

        RoutingTableManager manager = new RoutingTableManager(metastore, 0);
        RoutingTable rt = manager.getRoutingTable(tableId);

        assertThat(rt.entries()).hasSize(1);
        assertThat(rt.entries().get(0).backendGroupId()).isEqualTo("group-new");
        assertThat(rt.version()).isEqualTo(2);
    }

    @Test
    void testRemoveEntryShrinksRoutingTable() throws IOException {
        String tableId = "default.t";
        RoutingTableWriter writer = new RoutingTableWriter(metastore);
        writer.initialize(
                tableId,
                List.of(
                        new RoutingEntry(new KeyRange(0, 500), "group-1"),
                        new RoutingEntry(new KeyRange(500, 1000), "group-2")),
                new BackendEndpoint("group-1", "node1", 9090));

        // delete entry:1
        boolean ok = writer.removeEntry(tableId, 1);
        assertThat(ok).isTrue();

        RoutingTableManager manager = new RoutingTableManager(metastore, 0);
        RoutingTable rt = manager.getRoutingTable(tableId);
        assertThat(rt.entries()).hasSize(1);

        // bedeleteof key-range go through defaultBackend
        assertThat(rt.lookup(tableId, 600).groupId()).isEqualTo("group-1");
    }

    // ===============================================================
    // S4: dropTable delete + boundary conditions
    // ===============================================================

    @Test
    void testDropRoutingTableCleansAllKeys() throws IOException {
        String tableId = "default.t";
        RoutingTableWriter writer = new RoutingTableWriter(metastore);
        writer.initialize(
                tableId,
                List.of(
                        new RoutingEntry(new KeyRange(0, 500), "group-1"),
                        new RoutingEntry(new KeyRange(500, 1000), "group-2")),
                new BackendEndpoint("group-1", "node1", 9090));

        writer.dropRoutingTable(tableId);

        // all routing key alreadydelete
        assertThat(metastore.get(RoutingKeys.versionKey(tableId))).isEmpty();
        assertThat(metastore.get(RoutingKeys.defaultKey(tableId))).isEmpty();
        assertThat(metastore.get(RoutingKeys.entryKey(tableId, 0))).isEmpty();
        assertThat(metastore.get(RoutingKeys.entryKey(tableId, 1))).isEmpty();
    }

    @Test
    void testGetRoutingTableReturnsNullWhenNotInitialized() {
        RoutingTableManager manager = new RoutingTableManager(metastore, 0);
        RoutingTable rt = manager.getRoutingTable("nonexistent.t");
        assertThat(rt).isNull();
    }

    @Test
    void testLookupEmptyEntriesUsesDefaultBackend() throws IOException {
        String tableId = "default.t";
        RoutingTableWriter writer = new RoutingTableWriter(metastore);
        writer.initialize(tableId, List.of(), new BackendEndpoint("only-group", "node1", 9090));

        RoutingTableManager manager = new RoutingTableManager(metastore, 0);
        RoutingTable rt = manager.getRoutingTable(tableId);

        // no entry, all lookup go through default
        assertThat(rt.lookup(tableId, 0).groupId()).isEqualTo("only-group");
        assertThat(rt.lookup(tableId, 9999).groupId()).isEqualTo("only-group");
    }

    // ===============================================================
    // cache
    // ===============================================================

    @Test
    void testCacheRefreshDetectsVersionChange() throws IOException {
        String tableId = "default.t";
        RoutingTableWriter writer = new RoutingTableWriter(metastore);
        writer.initialize(
                tableId,
                List.of(new RoutingEntry(new KeyRange(0, 500), "group-1")),
                new BackendEndpoint("group-1", "node1", 9090));

        // load
        RoutingTableManager manager = new RoutingTableManager(metastore, 0);
        RoutingTable rt1 = manager.getRoutingTable(tableId);
        assertThat(rt1.version()).isEqualTo(1);

        // version (simulateanother item Frontend)
        writer.addEntry(tableId, new RoutingEntry(new KeyRange(500, 1000), "group-2"));

        // cachein is version
        RoutingTable rt2 = manager.getRoutingTable(tableId);
        assertThat(rt2.version()).isEqualTo(1); // cache in

        //
        manager.refreshAll();
        RoutingTable rt3 = manager.getRoutingTable(tableId);
        assertThat(rt3.version()).isEqualTo(2); // afteracquire version
        assertThat(rt3.entries()).hasSize(2);
    }
}
