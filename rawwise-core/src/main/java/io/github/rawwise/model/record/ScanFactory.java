package io.github.rawwise.model.record;

import io.github.rawwise.model.op.BatchScan;
import io.github.rawwise.model.op.KvScan;
import io.github.rawwise.model.op.ReadBuilder;
import io.github.rawwise.model.op.Scan;

public class ScanFactory {
    public enum ScanType {
        KV,
        BATCH,
        INDEX_ONLY,
        EXPRESSION_ONLY
    }

    private final ScanType scanType;

    public ScanFactory(ScanType scanType) {
        this.scanType = scanType;
    }

    public ReadBuilder<? extends Scan, ?> createBuilder() {
        return createBuilder(null);
    }

    public ReadBuilder<? extends Scan, ?> createBuilder(RawWiseTable table) {
        return switch (scanType) {
            case KV -> KvScan.builder().withTable(table);
            case BATCH -> BatchScan.builder().withTable(table);
            case INDEX_ONLY, EXPRESSION_ONLY ->
                    throw new UnsupportedOperationException(
                            "Scan type not implemented: " + scanType);
        };
    }

    public Scan createScan() {
        return createBuilder().build();
    }

    public Scan createScan(RawWiseTable table) {
        return createBuilder(table).build();
    }
}
