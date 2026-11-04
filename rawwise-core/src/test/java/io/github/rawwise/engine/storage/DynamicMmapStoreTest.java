package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.model.op.KvWriteBuilder;
import io.github.rawwise.model.record.DynamicTableInfo;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableMeta;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DynamicMmapStoreTest {

    @BeforeAll
    static void registerStorageProvider() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
            // Already registered, ignore
        }
    }

    @TempDir Path tempDir;

    @Test
    void writeCreatesTableScopedInstanceAndLease() throws Exception {
        String tableIdentifier = "catalog.db.tbl";
        DynamicMmapStore store =
                DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        try {
            KvWriteBuilder builder =
                    KvWriteBuilder.builder()
                            .table(new TestTable(tableIdentifier))
                            .key("k1".getBytes(StandardCharsets.UTF_8))
                            .value("v1".getBytes(StandardCharsets.UTF_8))
                            .partitionHint(0);

            RoutingOwnerContext owner = new RoutingOwnerContext("exec-1", 0);
            store.write(builder, owner);

            Path tablePath = tempDir.resolve(tableIdentifier);
            assertThat(Files.exists(tablePath)).isTrue();

            List<Path> children = Files.list(tablePath).toList();
            assertThat(children).isNotEmpty();
            assertThat(
                            children.stream()
                                    .filter(Files::isDirectory)
                                    .filter(
                                            path ->
                                                    path.getFileName()
                                                            .toString()
                                                            .startsWith("inst-"))
                                    .count())
                    .isEqualTo(1);
            assertThat(
                            children.stream()
                                    .filter(
                                            path ->
                                                    path.getFileName()
                                                            .toString()
                                                            .endsWith(".lease"))
                                    .count())
                    .isEqualTo(1);
        } finally {
            store.close();
        }
    }

    @Test
    void writeWithoutOwnerContextSucceeds() throws Exception {
        String tableIdentifier = "catalog.db.tbl.noowner";
        DynamicMmapStore store =
                DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        try {
            KvWriteBuilder builder =
                    KvWriteBuilder.builder()
                            .table(new TestTable(tableIdentifier))
                            .key("k1".getBytes(StandardCharsets.UTF_8))
                            .value("v1".getBytes(StandardCharsets.UTF_8))
                            .partitionHint(0);

            assertThatCode(() -> store.write(builder)).doesNotThrowAnyException();
        } finally {
            store.close();
        }
    }

    @Test
    @org.junit.jupiter.api.Disabled(
            "Flaky: LMDB lease epoch rotation can deadlock under reuseForks; needs dedicated JVM")
    void ownerEpochSwitchRotatesToNewWritableInstanceInsteadOfWaitingForOldLeaseTtl()
            throws Exception {
        String tableIdentifier = "catalog.db.tbl.epoch";
        DynamicMmapStore store =
                DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        try {
            KvWriteBuilder firstWrite =
                    KvWriteBuilder.builder()
                            .table(new TestTable(tableIdentifier))
                            .key("k1".getBytes(StandardCharsets.UTF_8))
                            .value("v1".getBytes(StandardCharsets.UTF_8))
                            .partitionHint(0);
            store.write(firstWrite, new RoutingOwnerContext("exec-owner", 1));

            KvWriteBuilder secondWrite =
                    KvWriteBuilder.builder()
                            .table(new TestTable(tableIdentifier))
                            .key("k2".getBytes(StandardCharsets.UTF_8))
                            .value("v2".getBytes(StandardCharsets.UTF_8))
                            .partitionHint(0);
            store.write(secondWrite, new RoutingOwnerContext("exec-owner", 2));

            List<LmdbInstance> allInstances =
                    store.tableStore(tableIdentifier).getAllInstancesDescending();
            assertThat(allInstances).hasSize(2);
            assertThat(
                            allInstances.stream()
                                    .map(LmdbInstance::currentLease)
                                    .filter(java.util.Objects::nonNull)
                                    .anyMatch(
                                            lease ->
                                                    lease.state()
                                                            == LmdbInstance.LeaseState.READ_ONLY))
                    .isTrue();
            assertThat(
                            allInstances.stream()
                                    .map(LmdbInstance::currentLease)
                                    .filter(java.util.Objects::nonNull)
                                    .anyMatch(
                                            lease ->
                                                    lease.state() == LmdbInstance.LeaseState.ACTIVE
                                                            && lease.ownerId()
                                                                    .equals(
                                                                            LmdbInstance
                                                                                    .routingOwnerId(
                                                                                            "exec-owner",
                                                                                            2))))
                    .isTrue();
        } finally {
            store.close();
        }
    }

    private record TestTable(String uniqId) implements RawWiseTable {
        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void refresh() {}

        @Override
        public TableMeta meta() {
            return null;
        }

        @Override
        public DynamicTableInfo dynamicInfo() {
            return null;
        }

        @Override
        public Snapshot currentSnapshot() {
            return null;
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            return null;
        }

        @Override
        public List<Snapshot> snapshots() {
            return List.of();
        }
    }
}
