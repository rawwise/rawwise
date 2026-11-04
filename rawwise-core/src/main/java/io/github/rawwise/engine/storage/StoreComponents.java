package io.github.rawwise.engine.storage;

import io.github.rawwise.backend.wal.WalWriteAdapter;
import io.github.rawwise.engine.storage.flush.FlushCoordinator;
import io.github.rawwise.engine.storage.flush.ParquetHdfsUploader;

public record StoreComponents(
        InstancePool instancePool,
        RecoveryManager recoveryManager,
        WalWriteAdapter walWriteAdapter,
        WritePathExecutor writePathExecutor,
        DeletionManager deletionManager,
        FlushCoordinator flushCoordinator,
        ParquetHdfsUploader parquetHdfsUploader) {}
