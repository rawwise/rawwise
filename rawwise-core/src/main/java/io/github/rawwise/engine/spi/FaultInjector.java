package io.github.rawwise.engine.spi;

public interface FaultInjector {
    FaultInjector NOOP =
            new FaultInjector() {
                @Override
                public void inject(InjectionPoint point) {}

                @Override
                public boolean isEnabled(InjectionPoint point) {
                    return false;
                }
            };

    enum InjectionPoint {
        AFTER_OCC_VALIDATE_BEFORE_WAL_WRITE,
        AFTER_WAL_WRITE_BEFORE_LMDB_WRITE,
        AFTER_LMDB_TXN_COMMIT_BEFORE_MARKER_PERSIST,
        DURING_WAL_FLUSH,
        DURING_LMDB_WRITE_TXN,
        DURING_HDFS_WRITE,
        DURING_VECTOR_INDEX_PERSIST,
        DURING_METASTORE_GRPC_CALL,
        BEFORE_LMDB_SPACE_CHECK,
        DURING_WAL_FLUSH_SIGTERM
    }

    enum Action {
        CRASH,
        THROW,
        DELAY,
        HANG
    }

    void inject(InjectionPoint point);

    boolean isEnabled(InjectionPoint point);
}
