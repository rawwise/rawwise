package io.github.rawwise.engine.storage.block;

import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.spi.Storage;
import java.io.Serializable;

public sealed interface DataBlock extends Serializable permits HotDataBlock, ColdDataBlock {

    String blockId();

    String tableId();

    Kind kind();

    default Layer layer() {
        return switch (kind()) {
            case MUTABLE_HOT, IMMUTABLE_HOT -> Layer.HOT;
            case IMMUTABLE_COLD -> Layer.COLD;
        };
    }

    Format format();

    Location location();

    KeyRange keyRange();

    long rowCount();

    long sizeBytes();

    long schemaVersion();

    Visibility visibility();

    long minSequenceNumber();

    long maxSequenceNumber();

    default long getSize() {
        return sizeBytes();
    }

    default KeyRange getRange() {
        return keyRange();
    }

    default Location getFilePath() {
        return location();
    }

    default long getTotalSizeInBytes() {
        return sizeBytes();
    }

    enum Layer {
        HOT,
        COLD
    }

    enum Kind {
        MUTABLE_HOT,
        IMMUTABLE_HOT,
        IMMUTABLE_COLD
    }

    enum Format {
        LMDB,
        PARQUET,
        ORC,
        UNKNOWN
    }

    enum Visibility {
        PENDING,
        ACTIVE,
        FLUSHING,
        ARCHIVED,
        DELETED;

        private static final java.util.Map<Visibility, java.util.Set<Visibility>>
                ALLOWED_TRANSITIONS =
                        java.util.Map.of(
                                PENDING, java.util.Set.of(ACTIVE, DELETED),
                                ACTIVE, java.util.Set.of(FLUSHING, DELETED),
                                FLUSHING, java.util.Set.of(ARCHIVED, ACTIVE, DELETED),
                                ARCHIVED, java.util.Set.of(DELETED),
                                DELETED, java.util.Set.of());

        public boolean canTransitionTo(Visibility target) {
            return ALLOWED_TRANSITIONS.getOrDefault(this, java.util.Set.of()).contains(target);
        }

        public Visibility validateTransitionTo(Visibility target) {
            if (!canTransitionTo(target)) {
                throw new IllegalStateException(
                        "Invalid Visibility transition: "
                                + this
                                + " -> "
                                + target
                                + ". Allowed: "
                                + ALLOWED_TRANSITIONS.getOrDefault(this, java.util.Set.of()));
            }
            return target;
        }
    }

    record Location(String storageName, Storage.StoragePath storagePath, long offset, long length)
            implements Serializable {}
}
