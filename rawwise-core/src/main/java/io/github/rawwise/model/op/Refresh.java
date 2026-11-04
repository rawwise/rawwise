package io.github.rawwise.model.op;

import io.github.rawwise.model.record.RawWiseTable;

public class Refresh extends AbstractOp<OpResult> {
    private static final OpType TYPE = OpType.REFRESH;

    private Refresh(RawWiseTable table) {
        super(table);
    }

    @Override
    public OpResult exec() {
        try {
            long snapshotIdBefore = table.dynamicInfo().currentSnapshotId();

            table.dynamicInfo().refresh();

            long snapshotIdAfter = table.dynamicInfo().currentSnapshotId();

            return DdlResult.RefreshResult.ok(snapshotIdBefore, snapshotIdAfter);
        } catch (Exception e) {
            return DdlResult.RefreshResult.error("Refresh failed: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    public static RefreshBuilder builder() {
        return new RefreshBuilder();
    }

    public static class RefreshBuilder implements TableOpBuilder<OpResult, Refresh> {
        private RawWiseTable table;

        public RefreshBuilder withTable(RawWiseTable table) {
            this.table = table;
            return this;
        }

        public RefreshBuilder tableName(RawWiseTable table) {
            return withTable(table);
        }

        @Override
        public RawWiseTable table() {
            return table;
        }

        @Override
        public Refresh build() {
            if (table == null) {
                throw new IllegalStateException("tableName must be set");
            }
            return new Refresh(table);
        }
    }
}
