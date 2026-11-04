package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.rawwise.engine.storage.table.read.TableReader;
import io.github.rawwise.model.op.Find;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("KVTable API closuretest")
class KVTableApiE2ETest {

    private static final class TestKVTable implements KVTable {
        private final TableReader tableReader;
        private final boolean open;

        TestKVTable(TableReader tableReader, boolean open) {
            this.tableReader = tableReader;
            this.open = open;
        }

        TestKVTable(TableReader tableReader) {
            this(tableReader, true);
        }

        @Override
        public PrimaryKey primaryKey() {
            return PrimaryKey.of("id");
        }

        @Override
        public TableReader tableReader() {
            return tableReader;
        }

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

        @Override
        public void refresh() {}

        @Override
        public int bucket(Map<String, Object> partitionValues) {
            return 0;
        }

        @Override
        public List<String> partitionPaths() {
            return List.of();
        }

        @Override
        public Map<String, Object> parsePartitionPath(String path) {
            return Map.of();
        }

        @Override
        public PartitionSpec partitionSpec() {
            return PartitionSpec.unpartitioned();
        }

        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return !open;
        }

        @Override
        public String getTableIdentifier() {
            return "test.tbl";
        }
    }

    private static final class TestKVTableWithoutReader implements KVTable {
        @Override
        public PrimaryKey primaryKey() {
            return PrimaryKey.none();
        }

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

        @Override
        public void refresh() {}

        @Override
        public int bucket(Map<String, Object> partitionValues) {
            return 0;
        }

        @Override
        public List<String> partitionPaths() {
            return List.of();
        }

        @Override
        public Map<String, Object> parsePartitionPath(String path) {
            return Map.of();
        }

        @Override
        public PartitionSpec partitionSpec() {
            return PartitionSpec.unpartitioned();
        }

        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    private static final class TestAppendOnlyTable implements AppendOnlyTable {

        @Override
        public PrimaryKey primaryKey() {
            return PrimaryKey.none();
        }

        @Override
        public HiddenColumnConfig hiddenColumnConfig() {
            return HiddenColumnConfig.builder().withHiddenPk(true).build();
        }

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

        @Override
        public void refresh() {}

        @Override
        public int bucket(Map<String, Object> partitionValues) {
            return 0;
        }

        @Override
        public List<String> partitionPaths() {
            return List.of();
        }

        @Override
        public Map<String, Object> parsePartitionPath(String path) {
            return Map.of();
        }

        @Override
        public PartitionSpec partitionSpec() {
            return PartitionSpec.unpartitioned();
        }

        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }
    }

    @Nested
    @DisplayName("KVTable.find()")
    class FindTests {

        @Test
        @DisplayName("viaprimary keyfind -> returns correctlyof Record")
        void shouldFindByPrimaryKey() throws IOException {
            Record foundRecord = MapRecord.fromMap(Map.of("id", 1L, "name", "Alice"));
            Find.Result foundResult = Find.Result.found(foundRecord);

            TableReader reader = mock(TableReader.class);
            when(reader.find(any(Find.class))).thenReturn(foundResult);

            KVTable table = new TestKVTable(reader);

            Find.Result result = table.find(Map.of("id", 1L));

            assertThat(result.success()).isTrue();
            assertThat(result.foundRecord()).isPresent();
            assertThat(result.foundRecord().get()).isEqualTo(foundRecord);
        }

        @Test
        @DisplayName("findnon-existent Key -> notFound")
        void shouldReturnNotFoundForMissingKey() throws IOException {
            TableReader reader = mock(TableReader.class);
            when(reader.find(any(Find.class))).thenReturn(Find.Result.notFound());

            KVTable table = new TestKVTable(reader);

            Find.Result result = table.find(Map.of("id", 999L));

            assertThat(result.success()).isTrue();
            assertThat(result.foundRecord()).isEmpty();
            assertThat(result.isFound()).isFalse();
        }

        @Test
        @DisplayName("PK valueis or null -> error")
        void shouldReturnErrorForNullOrEmptyPkValues() {
            KVTable table = new TestKVTable(mock(TableReader.class));

            Find.Result nullResult = table.find(null);
            assertThat(nullResult.success()).isFalse();
            assertThat(nullResult.message())
                    .hasValueSatisfying(
                            msg -> assertThat(msg).contains("Primary key values are required"));

            Find.Result emptyResult = table.find(Map.of());
            assertThat(emptyResult.success()).isFalse();
            assertThat(emptyResult.message())
                    .hasValueSatisfying(
                            msg -> assertThat(msg).contains("Primary key values are required"));
        }

        @Test
        @DisplayName("no TableReader -> error")
        void shouldReturnErrorWhenNoTableReader() {
            KVTable table = new TestKVTableWithoutReader();

            Find.Result result = table.find(Map.of("id", 1L));

            assertThat(result.success()).isFalse();
            assertThat(result.message())
                    .hasValueSatisfying(
                            msg -> assertThat(msg).contains("tableReader() not implemented"));
        }
    }

    @Nested
    @DisplayName("AppendOnlyTable.nextHiddenPkValue()")
    class NextHiddenPkValueTests {

        @Test
        @DisplayName("generate primary key -> ofvalue")
        void shouldGenerateAutoIncrementHiddenPk() {
            AppendOnlyTable table = new TestAppendOnlyTable();

            long first = table.nextHiddenPkValue();
            long second = table.nextHiddenPkValue();
            long third = table.nextHiddenPkValue();

            assertThat(second).isGreaterThan(first);
            assertThat(third).isGreaterThan(second);
        }

        @Test
        @DisplayName("globalcount singlecall")
        void shouldBeMonotonicallyIncreasing() {
            AppendOnlyTable table1 = new TestAppendOnlyTable();
            AppendOnlyTable table2 = new TestAppendOnlyTable();

            long v1 = table1.nextHiddenPkValue();
            long v2 = table2.nextHiddenPkValue();

            assertThat(v2).isGreaterThan(v1);
        }
    }
}
