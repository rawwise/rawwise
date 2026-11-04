package io.github.rawwise.engine.storage;

import io.github.rawwise.engine.storage.table.StoreState;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;

public record StoreInfrastructure(
        ScheduledExecutorService monitorService,
        ExecutorService flushExecutor,
        Consumer<DynamicMmapStore.FlushedInfo> metadataUpdater,
        Configuration hadoopConf,
        StoreState storeState,
        StorageManager storageManager) {}
