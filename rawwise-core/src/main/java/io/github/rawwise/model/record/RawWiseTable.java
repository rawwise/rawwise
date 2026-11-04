package io.github.rawwise.model.record;

import io.github.rawwise.model.op.Op;
import io.github.rawwise.model.op.OpResult;
import io.github.rawwise.model.op.Read;
import io.github.rawwise.model.op.ReadBuilder;
import io.github.rawwise.model.op.TableOpBuilder;
import io.github.rawwise.model.op.Write;
import io.github.rawwise.model.op.WriteBuilder;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;

public interface RawWiseTable extends Closeable {

    TableMeta meta();

    default String name() {
        return meta().fullName();
    }

    default String getTableIdentifier() {
        return name();
    }

    default String uniqId() {
        return meta().catalogName()
                + "."
                + meta().databaseName()
                + "."
                + meta().tableName()
                + "."
                + meta().tableId();
    }

    default Schema schema() {
        return meta().schema();
    }

    default long tableId() {
        return meta().tableId();
    }

    default TableMeta.TableType tableType() {
        return meta().tableType();
    }

    default boolean isKVTable() {
        return meta().isPrimaryKeyTable();
    }

    default boolean isAppendOnlyTable() {
        return meta().isAppendOnly();
    }

    default PrimaryKey primaryKey() {
        return meta().primaryKey();
    }

    default PartitionSpec partitionSpec() {
        return meta().partitionSpec();
    }

    default HiddenColumnConfig hiddenColumnConfig() {
        return meta().hiddenColumnConfig();
    }

    DynamicTableInfo dynamicInfo();

    Snapshot currentSnapshot();

    default Optional<Snapshot> currentSnapshotOptional() {
        return Optional.ofNullable(currentSnapshot());
    }

    Snapshot snapshot(long snapshotId);

    List<Snapshot> snapshots();

    default <R extends OpResult> R op(Op<R> op) {
        return op.exec();
    }

    default <R extends OpResult, O extends Op<R>> R op(TableOpBuilder<R, O> builder) {
        return builder.build().exec();
    }

    default Read.Result read(Read read) {
        return read.exec();
    }

    default <R extends Read> Read.Result read(ReadBuilder<R, ?> builder) {
        return read(builder.build());
    }

    default Write.Result write(Write write) {
        return write.exec();
    }

    default <W extends Write> Write.Result write(WriteBuilder<W, ?> builder) {
        return write(builder.build());
    }

    void refresh();

    default OpResult refreshOp() {
        return null;
    }

    boolean isClosed();
}
