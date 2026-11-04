package io.github.rawwise.governance.config;

import io.github.rawwise.engine.txn.IsolationLevel;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class RawWiseOptions {

    private static final Map<String, ConfigOption<?>> KEY_MAP;

    static ConfigOption<?> findOption(String key) {
        return KEY_MAP.get(key);
    }

    // ========== 4.3.1 Cluster (1 key) ==========

    public static final ConfigOption<String> CLUSTER_ID =
            ConfigOption.string("rawwise.cluster.id", "default");

    // ========== Environment Mode (1 key) ==========

    public static final ConfigOption<String> ENV_MODE =
            ConfigOption.builder("rawwise.env", "dev", String.class)
                    .validate(v -> "dev".equalsIgnoreCase(v) || "production".equalsIgnoreCase(v))
                    .build();

    // ========== 4.3.2 ZooKeeper (6 keys) ==========

    public static final ConfigOption<String> ZOOKEEPER_CONNECT_STRING =
            ConfigOption.string("rawwise.zookeeper.connect-string", "127.0.0.1:2181");

    public static final ConfigOption<Duration> ZOOKEEPER_CONNECTION_TIMEOUT =
            ConfigOption.duration("rawwise.zookeeper.connection-timeout", Duration.ofSeconds(10));

    public static final ConfigOption<Duration> ZOOKEEPER_SESSION_TIMEOUT =
            ConfigOption.duration("rawwise.zookeeper.session-timeout", Duration.ofSeconds(60));

    public static final ConfigOption<String> ZOOKEEPER_NAMESPACE =
            ConfigOption.string("rawwise.zookeeper.namespace", "rawwise");

    public static final ConfigOption<Integer> ZOOKEEPER_RETRY_COUNT =
            ConfigOption.int_("rawwise.zookeeper.retry-count", 3);

    public static final ConfigOption<Long> ZOOKEEPER_RETRY_BASE_SLEEP_MS =
            ConfigOption.long_("rawwise.zookeeper.retry-base-sleep-ms", 1000L);

    // ========== 4.3.3 Storage Core (4 keys) ==========

    public static final ConfigOption<String> STORAGE_MMAP_PATH =
            ConfigOption.string("rawwise.storage.mmap.path", "/tmp/rawwise/mmap");

    public static final ConfigOption<String> STORAGE_STORE_ID =
            ConfigOption.string("rawwise.storage.store-id", "default");

    public static final ConfigOption<Long> STORAGE_MMAP_SIZE =
            ConfigOption.long_("rawwise.storage.mmap.size", 1024L * 1024L * 1024L);

    public static final ConfigOption<String> STORAGE_DURABILITY_LEVEL =
            ConfigOption.builder("rawwise.storage.durability-level", "PROCESS", String.class)
                    .validate(
                            v ->
                                    v == null
                                            || v.isBlank()
                                            || "PROCESS".equalsIgnoreCase(v)
                                            || "NODE".equalsIgnoreCase(v))
                    .build();

    // ========== 4.3.4 Storage LMDB (11 keys) ==========

    public static final ConfigOption<Integer> STORAGE_LMDB_MAP_SIZE_MB =
            ConfigOption.builder("rawwise.storage.lmdb.map-size-mb", 1024, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Integer> STORAGE_LMDB_MAX_DBS =
            ConfigOption.int_("rawwise.storage.lmdb.max-dbs", 256);

    public static final ConfigOption<Long> STORAGE_LMDB_INITIAL_MAP_SIZE_MB =
            ConfigOption.builder("rawwise.storage.lmdb.initial-map-size-mb", 128L, Long.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Long> STORAGE_LMDB_MAX_MAP_SIZE_MB =
            ConfigOption.builder("rawwise.storage.lmdb.max-map-size-mb", 4096L, Long.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Long> STORAGE_LMDB_MMAP_BUDGET_MB =
            ConfigOption.builder("rawwise.storage.lmdb.mmap-budget-mb", 0L, Long.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Double> STORAGE_LMDB_USAGE_THRESHOLD =
            ConfigOption.builder("rawwise.storage.lmdb.usage-threshold", 0.70, Double.class)
                    .validate(v -> v != null && v > 0.0 && v < 1.0)
                    .build();

    public static final ConfigOption<Long> STORAGE_LMDB_EXPANSION_STEP_MB =
            ConfigOption.builder("rawwise.storage.lmdb.expansion-step-mb", 128L, Long.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Long> STORAGE_LMDB_LEASE_TTL_MS =
            ConfigOption.long_("rawwise.storage.lmdb.lease.ttl-ms", 10_000L);

    public static final ConfigOption<Integer> STORAGE_LMDB_KEY_BUFFER_SIZE =
            ConfigOption.int_("rawwise.storage.lmdb.key-buffer-size", 256);

    public static final ConfigOption<Integer> STORAGE_LMDB_VALUE_BUFFER_SIZE =
            ConfigOption.int_("rawwise.storage.lmdb.value-buffer-size", 1024);

    public static final ConfigOption<Integer> STORAGE_LMDB_BUFFER_POOL_MAX_SIZE =
            ConfigOption.int_("rawwise.storage.lmdb.buffer-pool-max-size", 64);

    public static final ConfigOption<Long> STORAGE_LMDB_USAGE_RATIO_REFRESH_INTERVAL_MS =
            ConfigOption.long_("rawwise.storage.lmdb.usage-ratio-refresh-interval-ms", 1000L);

    public static final ConfigOption<Double> STORAGE_LMDB_SPACE_WARN_THRESHOLD =
            ConfigOption.double_("rawwise.storage.lmdb.space-warn-threshold", 0.8);

    public static final ConfigOption<Double> STORAGE_LMDB_SPACE_ERROR_THRESHOLD =
            ConfigOption.double_("rawwise.storage.lmdb.space-error-threshold", 0.9);

    public static final ConfigOption<Integer> STORAGE_WRITABLE_INSTANCES =
            ConfigOption.int_("rawwise.storage.writable-instances", 0);

    public static final ConfigOption<Integer> STORAGE_EMBEDDED_WRITABLE_INSTANCES =
            ConfigOption.int_("rawwise.storage.embedded.writable-instances", 1);

    public static final ConfigOption<Boolean> STORAGE_RECOVERY_FAIL_ON_ERROR =
            ConfigOption.bool("rawwise.storage.recovery.fail-on-error", true);

    public static final ConfigOption<Boolean> RECOVERY_SENTINEL_FULL_SCAN_ON_MISMATCH =
            ConfigOption.bool("rawwise.recovery.sentinel.full-scan-on-mismatch", true);

    public static final ConfigOption<Boolean> OCC_PER_KEY_ENABLED =
            ConfigOption.bool("rawwise.occ.per-key.enabled", false);

    // ========== 4.3.5 Storage Flush (5 keys) ==========

    public static final ConfigOption<Long> STORAGE_FLUSH_THRESHOLD_BYTES =
            ConfigOption.builder(
                            "rawwise.storage.flush.threshold-bytes",
                            256L * 1024L * 1024L,
                            Long.class)
                    .unit(ConfigOption.Unit.BYTES)
                    .build();

    public static final ConfigOption<Integer> STORAGE_FLUSH_PARQUET_BLOCK_SIZE_MB =
            ConfigOption.builder("rawwise.storage.flush.parquet-block-size-mb", 256, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Integer> STORAGE_FLUSH_PARQUET_PAGE_SIZE_KB =
            ConfigOption.builder("rawwise.storage.flush.parquet-page-size-kb", 64, Integer.class)
                    .unit(ConfigOption.Unit.KIB)
                    .build();

    public static final ConfigOption<Boolean> STORAGE_FLUSH_PARQUET_DICTIONARY_ENABLED =
            ConfigOption.bool("rawwise.storage.flush.parquet-dictionary-enabled", true);

    public static final ConfigOption<Long> STORAGE_FLUSH_PARQUET_ROW_GROUP_SIZE_BYTES =
            ConfigOption.builder(
                            "rawwise.storage.flush.parquet-row-group-size-bytes",
                            10L * 1024 * 1024,
                            Long.class)
                    .unit(ConfigOption.Unit.BYTES)
                    .build();

    public static final ConfigOption<Long> STORAGE_FLUSH_DELETION_DELAY_MS =
            ConfigOption.long_("rawwise.storage.flush.deletion-delay-ms", 30000L);

    public static final ConfigOption<String> STORAGE_FLUSH_REMOTE_BASE_PATH =
            ConfigOption.string("rawwise.storage.flush.remote-base-path", "");

    // ========== 4.3.5.1 Checkpoint-LSN Binding (3 keys) ==========

    public static final ConfigOption<Boolean> CHECKPOINT_LSN_BINDING_ENABLED =
            ConfigOption.bool("rawwise.checkpoint.lsn-binding.enabled", false);

    public static final ConfigOption<String> CHECKPOINT_LSN_BINDING_MODE =
            ConfigOption.builder("rawwise.checkpoint.lsn-binding.mode", "ASYNC", String.class)
                    .validate(v -> "SYNC".equals(v) || "ASYNC".equals(v))
                    .build();

    public static final ConfigOption<Boolean> SNAPSHOT_AUTO_GENERATE_ON_CHECKPOINT =
            ConfigOption.bool("rawwise.snapshot.auto-generate-on-checkpoint", true);

    // ========== 4.3.6 WAL Unified (20 keys) ==========

    public static final ConfigOption<Boolean> WAL_ENABLED =
            ConfigOption.bool("rawwise.wal.enabled", true);

    public static final ConfigOption<Integer> WAL_BUFFER_SIZE =
            ConfigOption.int_("rawwise.wal.buffer-size", 8192);

    public static final ConfigOption<Long> WAL_SYNC_INTERVAL_MS =
            ConfigOption.long_("rawwise.wal.sync-interval-ms", 1000L);

    public static final ConfigOption<Long> WAL_MAX_SIZE_BYTES =
            ConfigOption.builder("rawwise.wal.max-size-bytes", 100L * 1024L * 1024L, Long.class)
                    .unit(ConfigOption.Unit.BYTES)
                    .build();

    public static final ConfigOption<String> WAL_HDFS_PATH =
            ConfigOption.string("rawwise.wal.hdfs-path", "");

    public static final ConfigOption<String> WAL_COMPRESSION_CODEC =
            ConfigOption.string("rawwise.wal.compression-codec", "NONE");

    public static final ConfigOption<String> WAL_FILE_FORMAT =
            ConfigOption.string("rawwise.wal.file-format", "BINARY");

    public static final ConfigOption<Long> WAL_APPEND_TIMEOUT_MS =
            ConfigOption.builder("rawwise.wal.append.timeout-ms", 30000L, Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<String> WAL_LOG_DIR =
            ConfigOption.string("rawwise.wal.log-dir", "wal_logs");

    public static final ConfigOption<String> WAL_LOG_FILE_PREFIX =
            ConfigOption.string("rawwise.wal.log-file-prefix", "wal");

    public static final ConfigOption<Integer> WAL_MAX_WRITE_RETRIES =
            ConfigOption.int_("rawwise.wal.max-write-retries", 3);

    public static final ConfigOption<Long> WAL_RETRY_BASE_DELAY_MS =
            ConfigOption.long_("rawwise.wal.retry-base-delay-ms", 100L);

    public static final ConfigOption<Long> WAL_RECOVERY_CHECK_INTERVAL_MS =
            ConfigOption.long_("rawwise.wal.recovery.check-interval-ms", 30000L);

    public static final ConfigOption<Integer> WAL_CIRCUIT_BREAKER_FAILURE_THRESHOLD =
            ConfigOption.int_("rawwise.wal.circuit-breaker.failure-threshold", 10);

    public static final ConfigOption<Long> WAL_CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS =
            ConfigOption.long_("rawwise.wal.circuit-breaker.reset-timeout-seconds", 120L);

    public static final ConfigOption<Integer> WAL_GROUP_COMMIT_MAX_PENDING =
            ConfigOption.int_("rawwise.wal.group-commit.max-pending", 10000);

    public static final ConfigOption<Integer> WAL_TRY_NEXT_MAX_RETRIES =
            ConfigOption.int_("rawwise.wal.try-next.max-retries", 3);

    public static final ConfigOption<Long> WAL_TRY_NEXT_BACKOFF_MS =
            ConfigOption.long_("rawwise.wal.try-next.backoff-ms", 1L);

    public static final ConfigOption<Integer> WAL_READER_MAX_PAYLOAD_SIZE_MB =
            ConfigOption.builder("rawwise.wal.reader.max-payload-size-mb", 256, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Integer> WAL_READER_MAX_HEADER_LENGTH_MB =
            ConfigOption.builder("rawwise.wal.reader.max-header-length-mb", 1, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<String> WAL_FAILURE_POLICY =
            ConfigOption.builder("rawwise.wal.failure-policy", "REJECT", String.class)
                    .validate(v -> "REJECT".equalsIgnoreCase(v) || "DEGRADE".equalsIgnoreCase(v))
                    .build();

    // ========== 4.3.7 Storage Memory Budget (1 key) ==========

    public static final ConfigOption<Double> STORAGE_MEMORY_BUDGET_USAGE_THRESHOLD =
            ConfigOption.double_("rawwise.storage.memory-budget.usage-threshold", 0.9);

    // ========== 4.3.8 Storage Instance Pool (3 keys) ==========

    public static final ConfigOption<Long> STORAGE_INSTANCE_POOL_SPIN_SLEEP_MS =
            ConfigOption.long_("rawwise.storage.instance-pool.spin-sleep-ms", 100L);

    public static final ConfigOption<Integer> STORAGE_INSTANCE_POOL_MAX_SPIN_COUNT =
            ConfigOption.int_("rawwise.storage.instance-pool.max-spin-count", 300);

    public static final ConfigOption<Long> STORAGE_INSTANCE_POOL_MONITOR_INTERVAL_MINUTES =
            ConfigOption.long_("rawwise.storage.instance-pool.monitor-interval-minutes", 5L);

    // ========== 4.3.9 Storage Deletion Manager (1 key) ==========

    public static final ConfigOption<Integer> STORAGE_DELETION_MANAGER_MAX_RETRY_COUNT =
            ConfigOption.int_("rawwise.storage.deletion-manager.max-retry-count", 10);

    // ========== 4.3.10 Metastore (9 keys) ==========

    public static final ConfigOption<String> METASTORE_TYPE =
            ConfigOption.string("rawwise.metastore.type", "inmemory");

    public static final ConfigOption<String> METASTORE_BASE_PATH =
            ConfigOption.string("rawwise.metastore.base-path", "/tmp/rawwise/metastore");

    public static final ConfigOption<Integer> METASTORE_PORT =
            ConfigOption.int_("rawwise.metastore.port", 50051);

    public static final ConfigOption<Integer> METASTORE_GRPC_PORT =
            ConfigOption.int_("rawwise.metastore.grpc-port", 9001);

    public static final ConfigOption<String> METASTORE_GRPC_HOST =
            ConfigOption.string("rawwise.metastore.grpc-host", "localhost");

    public static final ConfigOption<String> METASTORE_GRPC_TARGET =
            ConfigOption.string("rawwise.metastore.grpc-target", "");

    public static final ConfigOption<Integer> METASTORE_GRPC_MAX_INBOUND_MESSAGE_SIZE_MB =
            ConfigOption.builder(
                            "rawwise.metastore.grpc.max-inbound-message-size-mb", 16, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Long> METASTORE_ROCKSDB_WRITE_BUFFER_SIZE_MB =
            ConfigOption.builder("rawwise.metastore.rocksdb.write-buffer-size-mb", 64L, Long.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Integer> METASTORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER =
            ConfigOption.int_("rawwise.metastore.rocksdb.max-write-buffer-number", 4);

    public static final ConfigOption<Integer> METASTORE_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE =
            ConfigOption.int_("rawwise.metastore.rocksdb.min-write-buffer-number-to-merge", 2);

    // ========== 4.3.10.1 Metastore Backup (3 keys) ==========

    public static final ConfigOption<Boolean> METASTORE_BACKUP_ENABLED =
            ConfigOption.bool("rawwise.metastore.backup.enabled", true);

    public static final ConfigOption<String> METASTORE_BACKUP_PATH =
            ConfigOption.string("rawwise.metastore.backup.path", null);

    public static final ConfigOption<Integer> METASTORE_BACKUP_INTERVAL_HOURS =
            ConfigOption.int_("rawwise.metastore.backup.interval-hours", 6);

    // ========== 4.3.11 Catalog (6 keys) ==========

    public static final ConfigOption<String> CATALOG_WAREHOUSE_PATH =
            ConfigOption.string(
                    "rawwise.catalog.warehouse-path",
                    // Resolve at class load time: prefer -Drawwise.warehouse.path or
                    // RAWWISE_WAREHOUSE_PATH
                    // env var to prevent silent /tmp data loss when child processes don't inherit
                    // -D properly.
                    resolveDefaultWarehousePath());

    private static String resolveDefaultWarehousePath() {
        String fromSys = System.getProperty("rawwise.warehouse.path");
        if (fromSys != null && !fromSys.isEmpty()) return fromSys;
        String fromEnv = System.getenv("RAWWISE_WAREHOUSE_PATH");
        if (fromEnv != null && !fromEnv.isEmpty()) return fromEnv;
        return "/tmp/rawwise/warehouse";
    }

    public static final ConfigOption<String> CATALOG_IMPLEMENTATION =
            ConfigOption.string("rawwise.catalog.implementation", "metastore");

    public static final ConfigOption<Integer> CATALOG_CACHE_MAXIMUM_SIZE =
            ConfigOption.int_("rawwise.catalog.cache.maximum-size", 1000);

    public static final ConfigOption<Long> CATALOG_CACHE_EXPIRE_AFTER_WRITE_MINUTES =
            ConfigOption.long_("rawwise.catalog.cache.expire-after-write-minutes", 5L);

    public static final ConfigOption<Long> CATALOG_INTENT_TIMEOUT_MS =
            ConfigOption.long_("rawwise.catalog.intent-timeout-ms", 600000L);

    public static final ConfigOption<Long> CATALOG_INTENT_GC_INTERVAL_MS =
            ConfigOption.long_("rawwise.catalog.intent-gc-interval-ms", 300000L);

    // ========== 4.3.12 Backend Spark (2 keys) ==========

    public static final ConfigOption<String> BACKEND_SPARK_MASTER =
            ConfigOption.string("rawwise.backend.spark.master", "local[*]");

    public static final ConfigOption<String> BACKEND_SPARK_DRIVER_MEMORY =
            ConfigOption.string("rawwise.backend.spark.driver-memory", "4g");

    public static final ConfigOption<String> BACKEND_SPARK_DRIVER_HOST =
            ConfigOption.string("rawwise.backend.spark.driver-host", null);

    public static final ConfigOption<Boolean> BACKEND_SPARK_FAIR_SCHEDULER_ENABLED =
            ConfigOption.bool("rawwise.backend.spark.fair-scheduler.enabled", true);

    // ========== 4.3.13 Routing (15 keys) ==========

    public static final ConfigOption<Integer> ROUTING_SHARD_COUNT =
            ConfigOption.builder("rawwise.routing.shard-count", 1024, Integer.class)
                    .validateRange(1, 65536)
                    .build();

    public static final ConfigOption<String> SHARD_RESOLVER_CLASS =
            ConfigOption.string(
                    "rawwise.shard.resolver-class",
                    "io.github.rawwise.backend.routing.ModHashShardResolver");

    public static final ConfigOption<Integer> ROUTING_REGISTER_MAX_RETRIES =
            ConfigOption.int_("rawwise.routing.register.max-retries", 3);

    public static final ConfigOption<Long> ROUTING_REGISTER_BACKOFF_MS =
            ConfigOption.long_("rawwise.routing.register.backoff-ms", 200L);

    public static final ConfigOption<Boolean> ROUTING_REGISTER_FAIL_OPEN =
            ConfigOption.bool("rawwise.routing.register.fail-open", false);

    public static final ConfigOption<Integer> ROUTING_FORWARD_MAX_RETRIES =
            ConfigOption.int_("rawwise.routing.forward.max-retries", 3);

    public static final ConfigOption<Long> ROUTING_FORWARD_BACKOFF_MS =
            ConfigOption.long_("rawwise.routing.forward.backoff-ms", 100L);

    public static final ConfigOption<Boolean> ROUTING_WRITE_ASSUME_PARTITION_IS_SHARD =
            ConfigOption.bool("rawwise.routing.write.assume-partition-is-shard", false);

    public static final ConfigOption<Integer> ROUTING_WRITE_BATCH_ROWS =
            ConfigOption.int_("rawwise.routing.write.batch-rows", 1024);

    public static final ConfigOption<Boolean> WRITE_LOCAL =
            ConfigOption.bool("rawwise.write.local", false);

    public static final ConfigOption<Long> ROUTING_COMMITTED_CACHE_TTL_MS =
            ConfigOption.long_("rawwise.routing.committed-cache.ttl-ms", 300000L);

    public static final ConfigOption<Long> ROUTING_COMMITTED_CACHE_CLEANUP_INTERVAL_MS =
            ConfigOption.long_("rawwise.routing.committed-cache.cleanup-interval-ms", 60000L);

    public static final ConfigOption<Integer> ROUTING_COMMITTED_CACHE_MAX_SIZE =
            ConfigOption.int_("rawwise.routing.committed-cache.max-size", 10000);

    public static final ConfigOption<Integer> ROUTING_MAX_BATCH_IDS_PER_SHARD =
            ConfigOption.int_("rawwise.routing.max-batch-ids-per-shard", 1024);

    public static final ConfigOption<Integer> ROUTING_VIRTUAL_NODES =
            ConfigOption.int_("rawwise.routing.virtual-nodes", 256);

    // ========== 4.3.14 Compaction (18 keys) ==========

    public static final ConfigOption<String> COMPACTION_STRATEGY_TYPE =
            ConfigOption.string("rawwise.compaction.strategy", "TIERED");

    public static final ConfigOption<Integer> COMPACTION_MAX_CONCURRENT =
            ConfigOption.int_("rawwise.compaction.max-concurrent", 4);

    public static final ConfigOption<Integer> COMPACTION_MAX_RETRIES =
            ConfigOption.int_("rawwise.compaction.max-retries", 3);

    public static final ConfigOption<Long> COMPACTION_TRIGGER_INTERVAL_MS =
            ConfigOption.long_("rawwise.compaction.trigger.interval-ms", 60000L);

    public static final ConfigOption<Integer> COMPACTION_SCHEDULER_PARALLELISM =
            ConfigOption.int_(
                    "rawwise.compaction.scheduler.parallelism",
                    Math.min(4, Runtime.getRuntime().availableProcessors()));

    public static final ConfigOption<Integer> COMPACTION_TIERED_L0_THRESHOLD =
            ConfigOption.int_("rawwise.compaction.tiered.l0-file-count-threshold", 20);

    public static final ConfigOption<Double> COMPACTION_TIERED_SIZE_AMPLIFICATION_FACTOR =
            ConfigOption.double_("rawwise.compaction.tiered.size-amplification-factor", 10.0);

    public static final ConfigOption<Integer> COMPACTION_LEVELED_FILE_COUNT_LIMIT =
            ConfigOption.int_("rawwise.compaction.leveled.file-count-limit", 10);

    public static final ConfigOption<Double> COMPACTION_LEVELED_SIZE_RATIO =
            ConfigOption.double_("rawwise.compaction.leveled.size-ratio", 10.0);

    public static final ConfigOption<Double> COMPACTION_UNIVERSAL_SIZE_RATIO =
            ConfigOption.double_("rawwise.compaction.universal.size-ratio", 5.0);

    public static final ConfigOption<Integer> COMPACTION_UNIVERSAL_MIN_MERGE_WIDTH =
            ConfigOption.int_("rawwise.compaction.universal.min-merge-width", 2);

    public static final ConfigOption<Integer> COMPACTION_MAX_LEVEL =
            ConfigOption.int_("rawwise.compaction.max-level", 3);

    public static final ConfigOption<Long> COMPACTION_GC_MAX_ENTRY_AGE_MS =
            ConfigOption.long_("rawwise.compaction.gc.max-entry-age-ms", 604800000L);

    public static final ConfigOption<Integer> COMPACTION_GC_MAX_PENDING_SIZE =
            ConfigOption.int_("rawwise.compaction.gc.max-pending-size", 10000);

    public static final ConfigOption<Integer> COMPACTION_GC_MAX_DEAD_LETTER_SIZE =
            ConfigOption.int_("rawwise.compaction.gc.max-dead-letter-size", 1000);

    public static final ConfigOption<Long> COMPACTION_GC_DEFERRED_GC_DELAY_SECONDS =
            ConfigOption.long_("rawwise.compaction.gc.deferred-gc-delay-seconds", 60L);

    public static final ConfigOption<Long> COMPACTION_TRIGGER_ENTRY_COUNT_THRESHOLD =
            ConfigOption.long_("rawwise.compaction.trigger.entry-count-threshold", 1000L);

    public static final ConfigOption<Long> COMPACTION_TRIGGER_FILE_COUNT_THRESHOLD =
            ConfigOption.long_("rawwise.compaction.trigger.file-count-threshold", 50L);

    public static final ConfigOption<Long> COMPACTION_TRIGGER_TIME_INTERVAL_MS =
            ConfigOption.long_("rawwise.compaction.trigger.time-interval-ms", 300000L);

    // ========== 4.3.15 Clean (14 keys) ==========

    public static final ConfigOption<Boolean> CLEAN_ENABLED =
            ConfigOption.bool("rawwise.clean.enabled", true);

    public static final ConfigOption<Long> CLEAN_ORPHAN_FILE_MIN_AGE_MS =
            ConfigOption.long_("rawwise.clean.orphan-file.min-age-ms", 3600000L);

    public static final ConfigOption<Long> CLEAN_SNAPSHOT_RETENTION_MS =
            ConfigOption.long_("rawwise.clean.snapshot.retention-ms", 86400000L);

    public static final ConfigOption<Integer> CLEAN_SNAPSHOT_MIN_RETENTION =
            ConfigOption.int_("rawwise.clean.snapshot.min-retention", 1);

    public static final ConfigOption<Integer> CLEAN_TTL_DAYS =
            ConfigOption.int_("rawwise.clean.ttl-days", 30);

    public static final ConfigOption<String> CLEAN_TTL_EXPIRE_COLUMN =
            ConfigOption.string("rawwise.clean.ttl.expire-column", "");

    public static final ConfigOption<Integer> CLEAN_MAX_RETRIES =
            ConfigOption.int_("rawwise.clean.max-retries", 3);

    public static final ConfigOption<Long> CLEAN_RETRY_BACKOFF_BASE_MS =
            ConfigOption.long_("rawwise.clean.retry.backoff-base-ms", 100L);

    public static final ConfigOption<Long> CLEAN_RETRY_BACKOFF_MAX_MS =
            ConfigOption.long_("rawwise.clean.retry.backoff-max-ms", 5000L);

    public static final ConfigOption<Long> CLEAN_EXECUTOR_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS =
            ConfigOption.long_("rawwise.clean.executor.graceful-shutdown-timeout-seconds", 30L);

    public static final ConfigOption<Long> CLEAN_EXECUTOR_FORCED_SHUTDOWN_TIMEOUT_SECONDS =
            ConfigOption.long_("rawwise.clean.executor.forced-shutdown-timeout-seconds", 10L);

    public static final ConfigOption<Integer> CLEAN_EXECUTOR_CORE_POOL_SIZE =
            ConfigOption.int_("rawwise.clean.executor.core-pool-size", 0);

    public static final ConfigOption<Integer> CLEAN_EXECUTOR_MAX_POOL_MULTIPLIER =
            ConfigOption.int_("rawwise.clean.executor.max-pool-multiplier", 2);

    public static final ConfigOption<Integer> CLEAN_EXECUTOR_QUEUE_CAPACITY =
            ConfigOption.int_("rawwise.clean.executor.queue-capacity", 256);

    // ========== 4.3.16 Transaction (8 keys) ==========

    public static final ConfigOption<Long> TXN_DEFAULT_TIMEOUT_MS =
            ConfigOption.long_("rawwise.txn.default-timeout-ms", 30 * 60 * 1000L);

    public static final ConfigOption<Long> TXN_OLTP_TIMEOUT_MS =
            ConfigOption.long_("rawwise.txn.oltp-timeout-ms", 60_000L);

    public static final ConfigOption<Long> TXN_INGESTION_TIMEOUT_MS =
            ConfigOption.long_("rawwise.txn.ingestion-timeout-ms", 5 * 60 * 1000L);

    public static final ConfigOption<Long> TXN_READ_CACHE_MAX_BYTES =
            ConfigOption.builder(
                            "rawwise.txn.read-cache-max-bytes", 32L * 1024L * 1024L, Long.class)
                    .unit(ConfigOption.Unit.BYTES)
                    .build();

    public static final ConfigOption<Integer> TXN_CLEANUP_INTERVAL_SECONDS =
            ConfigOption.int_("rawwise.txn.cleanup-interval-seconds", 60);

    public static final ConfigOption<Long> TXN_EVICTION_SHUTDOWN_TIMEOUT_MS =
            ConfigOption.long_("rawwise.txn.eviction.shutdown-timeout-ms", 5000L);

    public static final ConfigOption<Long> TXN_EVICTION_TIMEOUT_MS =
            ConfigOption.long_("rawwise.txn.eviction.timeout-ms", 30000L);

    public static final ConfigOption<Integer> TXN_FALLBACK_SHARD_COUNT =
            ConfigOption.int_("rawwise.txn.fallback-shard-count", 1024);

    // ========== 4.3.17 Fast Path Transaction (3 keys) ==========

    public static final ConfigOption<Integer> FAST_PATH_TXN_MAX_RETRY =
            ConfigOption.int_("rawwise.fast-path.txn.max-retry", 3);

    public static final ConfigOption<Long> FAST_PATH_TXN_CONFLICT_BACKOFF_MS =
            ConfigOption.long_("rawwise.fast-path.txn.conflict-backoff-ms", 10L);

    public static final ConfigOption<Boolean> FAST_PATH_TXN_CAS_ENABLED =
            ConfigOption.bool("rawwise.fast-path.txn.cas-enabled", true);

    // ========== 4.3.18 Ingestion (7 keys) ==========

    public static final ConfigOption<String> INGESTION_TXN_ISOLATION_LEVEL =
            ConfigOption.builder(
                            "rawwise.ingestion.txn.isolation-level", "READ_COMMITTED", String.class)
                    .validate(
                            value -> {
                                try {
                                    IsolationLevel.fromString(value);
                                    return true;
                                } catch (IllegalArgumentException e) {
                                    return false;
                                }
                            })
                    .build();

    public static final ConfigOption<String> INGESTION_TXN_MODE =
            ConfigOption.builder("rawwise.ingestion.txn.mode", "DEFERRED", String.class)
                    .validate(
                            value -> {
                                try {
                                    io.github.rawwise.engine.txn.TxnMode.fromString(value);
                                    return true;
                                } catch (IllegalArgumentException e) {
                                    return false;
                                }
                            })
                    .build();

    public static final ConfigOption<Long> INGESTION_WRITE_BUFFER_MAX_BYTES =
            ConfigOption.builder(
                            "rawwise.ingestion.write.buffer-max-bytes",
                            64L * 1024 * 1024,
                            Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Integer> MAX_PENDING_INSTANCES =
            ConfigOption.builder("rawwise.ingestion.max.pending.instances", 128, Integer.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Integer> MAX_PENDING_BATCH_SIZE =
            ConfigOption.builder("rawwise.ingestion.max.pending.batch.size", 10000, Integer.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Long> MAX_PENDING_BATCH_BYTES =
            ConfigOption.builder(
                            "rawwise.ingestion.max.pending.batch.bytes",
                            100L * 1024L * 1024L,
                            Long.class)
                    .validatePositive()
                    .unit(ConfigOption.Unit.BYTES)
                    .build();

    public static final ConfigOption<Integer> INGESTION_BATCH_INITIAL_CAPACITY =
            ConfigOption.int_("rawwise.ingestion.batch.initial-capacity", 128);

    // ========== 4.3.18.1 Ingestion Sink (10 keys) ==========

    public static final ConfigOption<String> INGESTION_SINK_TYPE =
            ConfigOption.builder("rawwise.ingestion.sink.type", "LMDB", String.class)
                    .validate(v -> "LMDB".equalsIgnoreCase(v) || "PARQUET".equalsIgnoreCase(v))
                    .build();

    public static final ConfigOption<String> INGESTION_SINK_PARQUET_HDFS_BASE_PATH =
            ConfigOption.string("rawwise.ingestion.sink.parquet.hdfs-base-path", "/rawwise");

    public static final ConfigOption<Long> INGESTION_SINK_PARQUET_ROW_GROUP_SIZE_BYTES =
            ConfigOption.builder(
                            "rawwise.ingestion.sink.parquet.row-group-size-bytes",
                            128L * 1024 * 1024,
                            Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<String> INGESTION_SINK_PARQUET_COMPRESSION_CODEC =
            ConfigOption.builder(
                            "rawwise.ingestion.sink.parquet.compression-codec",
                            "ZSTD",
                            String.class)
                    .validate(
                            v ->
                                    "UNCOMPRESSED".equalsIgnoreCase(v)
                                            || "SNAPPY".equalsIgnoreCase(v)
                                            || "ZSTD".equalsIgnoreCase(v)
                                            || "GZIP".equalsIgnoreCase(v))
                    .build();

    public static final ConfigOption<Long> INGESTION_SINK_PARQUET_MAX_PENDING_BYTES =
            ConfigOption.builder(
                            "rawwise.ingestion.sink.parquet.max-pending-bytes",
                            1024L * 1024 * 1024,
                            Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Long> INGESTION_SINK_PARQUET_MAX_BATCH_BYTES =
            ConfigOption.builder(
                            "rawwise.ingestion.sink.parquet.max-batch-bytes",
                            512L * 1024 * 1024,
                            Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Long> INGESTION_SINK_PARQUET_MIN_BATCH_BYTES =
            ConfigOption.builder("rawwise.ingestion.sink.parquet.min-batch-bytes", 0L, Long.class)
                    .validate(v -> v != null && v >= 0)
                    .build();

    public static final ConfigOption<Long> INGESTION_SINK_PARQUET_ORPHAN_AGE_THRESHOLD_MS =
            ConfigOption.builder(
                            "rawwise.ingestion.sink.parquet.orphan-age-threshold-ms",
                            3600_000L,
                            Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Integer> INGESTION_SINK_LMDB_MAX_PENDING_INSTANCES =
            ConfigOption.builder(
                            "rawwise.ingestion.sink.lmdb.max-pending-instances", 128, Integer.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Long> INGESTION_SINK_LMDB_INSTANCE_MAPPED_BYTES =
            ConfigOption.builder(
                            "rawwise.ingestion.sink.lmdb.instance-mapped-bytes",
                            64L * 1024 * 1024,
                            Long.class)
                    .validatePositive()
                    .build();

    // ========== 4.3.19 Read (3 keys) ==========

    public static final ConfigOption<Integer> READ_BATCH_SIZE =
            ConfigOption.int_("rawwise.read.batch-size", 1024);

    public static final ConfigOption<Integer> READ_PARTITION_BATCH_SIZE =
            ConfigOption.int_("rawwise.read.partition.batch-size", 4096);

    public static final ConfigOption<Integer> READ_BULK_READER_INITIAL_CAPACITY =
            ConfigOption.int_("rawwise.read.bulk-reader.initial-capacity", 1024);

    // ========== 4.3.20 Write (2 keys) ==========

    public static final ConfigOption<Long> WRITE_TIMEOUT_MS =
            ConfigOption.long_("rawwise.write.timeout-ms", 30000L);

    public static final ConfigOption<Long> WRITE_SPIN_BACKOFF_NS =
            ConfigOption.long_("rawwise.write.spin-backoff-ns", 100000100L);

    public static final ConfigOption<Long> STORAGE_WRITE_TIMEOUT_MS =
            ConfigOption.long_("rawwise.storage.write-timeout-ms", 30000L);

    // ========== 4.3.21 UpdateLog (8 keys) ==========

    public static final ConfigOption<Integer> UPDATELOG_READER_MAX_PAYLOAD_SIZE_MB =
            ConfigOption.builder("rawwise.updatelog.reader.max-payload-size-mb", 256, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Integer> UPDATELOG_READER_MAX_HEADER_LENGTH_MB =
            ConfigOption.builder("rawwise.updatelog.reader.max-header-length-mb", 1, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Integer> UPDATELOG_READER_MAX_STRING_FIELD_LENGTH_KB =
            ConfigOption.builder(
                            "rawwise.updatelog.reader.max-string-field-length-kb",
                            64,
                            Integer.class)
                    .unit(ConfigOption.Unit.KIB)
                    .build();

    public static final ConfigOption<Integer> UPDATELOG_READER_MAX_BINARY_FIELD_LENGTH_MB =
            ConfigOption.builder(
                            "rawwise.updatelog.reader.max-binary-field-length-mb", 4, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    public static final ConfigOption<Integer> UPDATELOG_READER_MAX_RECORD_COUNT =
            ConfigOption.int_("rawwise.updatelog.reader.max-record-count", 1000000);

    public static final ConfigOption<Integer> UPDATELOG_MAX_ENTRY_COUNT =
            ConfigOption.int_("rawwise.updatelog.max-entry-count", 10000);

    public static final ConfigOption<Integer> UPDATELOG_MAX_COLUMN_NAME_LENGTH_KB =
            ConfigOption.builder("rawwise.updatelog.max-column-name-length-kb", 64, Integer.class)
                    .unit(ConfigOption.Unit.KIB)
                    .build();

    public static final ConfigOption<Integer> UPDATELOG_MAX_VALUE_LENGTH_KB =
            ConfigOption.builder("rawwise.updatelog.max-value-length-kb", 64, Integer.class)
                    .unit(ConfigOption.Unit.KIB)
                    .build();

    // ========== 4.3.22 Recovery (3 keys) ==========

    public static final ConfigOption<Integer> RECOVERY_MAX_RETRIES =
            ConfigOption.int_("rawwise.recovery.max-retries", 3);

    public static final ConfigOption<Long> RECOVERY_BACKOFF_MS =
            ConfigOption.long_("rawwise.recovery.backoff-ms", 5000L);

    public static final ConfigOption<Long> RECOVERY_TIMEOUT_MS =
            ConfigOption.long_("rawwise.recovery.timeout-ms", 300000L);

    // ========== 4.3.23 Metrics (8 keys) ==========

    public static final ConfigOption<String> METRICS_CLUSTER_ID =
            ConfigOption.string("rawwise.metrics.cluster-id", "default");

    public static final ConfigOption<Integer> METRICS_PROMETHEUS_PORT =
            ConfigOption.int_("rawwise.metrics.prometheus-port", 9190);

    public static final ConfigOption<String> METRICS_PROMETHEUS_PATH =
            ConfigOption.string("rawwise.metrics.prometheus-path", "/metrics");

    public static final ConfigOption<Boolean> METRICS_ENABLE_SYSTEM =
            ConfigOption.bool("rawwise.metrics.enable-system", true);

    public static final ConfigOption<Boolean> METRICS_ENABLE_JMX =
            ConfigOption.bool("rawwise.metrics.enable-jmx", false);

    public static final ConfigOption<String> METRICS_JMX_DOMAIN =
            ConfigOption.string("rawwise.metrics.jmx-domain", "rawwise.metrics");

    public static final ConfigOption<Integer> METRICS_SAMPLING_INTERVAL_SECONDS =
            ConfigOption.int_("rawwise.metrics.sampling-interval-seconds", 10);

    public static final ConfigOption<Boolean> METRICS_ENABLE_EXECUTOR_AGGREGATION =
            ConfigOption.bool("rawwise.metrics.enable-executor-aggregation", true);

    // ========== 4.3.24 Backpressure (13 keys) ==========

    public static final ConfigOption<Double> BACKPRESSURE_WARNING_THRESHOLD =
            ConfigOption.double_("rawwise.backpressure.warning-threshold", 0.7);

    public static final ConfigOption<Double> BACKPRESSURE_THROTTLE_THRESHOLD =
            ConfigOption.double_("rawwise.backpressure.throttle-threshold", 0.9);

    public static final ConfigOption<Double> BACKPRESSURE_REJECT_THRESHOLD =
            ConfigOption.double_("rawwise.backpressure.reject-threshold", 0.95);

    public static final ConfigOption<Integer> BACKPRESSURE_QUEUE_THRESHOLD =
            ConfigOption.int_("rawwise.backpressure.queue-threshold", 100);

    public static final ConfigOption<Double> BACKPRESSURE_CPU_THRESHOLD =
            ConfigOption.double_("rawwise.backpressure.cpu-threshold", 0.9);

    public static final ConfigOption<Double> BACKPRESSURE_MEMORY_THRESHOLD =
            ConfigOption.double_("rawwise.backpressure.memory-threshold", 0.9);

    public static final ConfigOption<Double> BACKPRESSURE_DISK_THRESHOLD =
            ConfigOption.double_("rawwise.backpressure.disk-threshold", 0.8);

    public static final ConfigOption<Double> BACKPRESSURE_NETWORK_THRESHOLD =
            ConfigOption.double_("rawwise.backpressure.network-threshold", 0.8);

    public static final ConfigOption<Double> BACKPRESSURE_QUEUE_WEIGHT =
            ConfigOption.double_("rawwise.backpressure.queue-weight", 0.5);

    public static final ConfigOption<Double> BACKPRESSURE_CPU_WEIGHT =
            ConfigOption.double_("rawwise.backpressure.cpu-weight", 0.15);

    public static final ConfigOption<Double> BACKPRESSURE_MEMORY_WEIGHT =
            ConfigOption.double_("rawwise.backpressure.memory-weight", 0.15);

    public static final ConfigOption<Double> BACKPRESSURE_DISK_WEIGHT =
            ConfigOption.double_("rawwise.backpressure.disk-weight", 0.1);

    public static final ConfigOption<Double> BACKPRESSURE_NETWORK_WEIGHT =
            ConfigOption.double_("rawwise.backpressure.network-weight", 0.1);

    public static final ConfigOption<Integer> BACKPRESSURE_INTERACTIVE_PERMITS =
            ConfigOption.int_("rawwise.backend.backpressure.interactive.permits", 10);

    public static final ConfigOption<Long> BACKPRESSURE_INTERACTIVE_TIMEOUT_MS =
            ConfigOption.long_("rawwise.backend.backpressure.interactive.timeout-ms", 5000L);

    public static final ConfigOption<Integer> BACKPRESSURE_BATCH_PERMITS =
            ConfigOption.int_("rawwise.backend.backpressure.batch.permits", 5);

    public static final ConfigOption<Long> BACKPRESSURE_BATCH_TIMEOUT_MS =
            ConfigOption.long_("rawwise.backend.backpressure.batch.timeout-ms", 10000L);

    public static final ConfigOption<Integer> BACKPRESSURE_TRAINING_PERMITS =
            ConfigOption.int_("rawwise.backend.backpressure.training.permits", 20);

    public static final ConfigOption<Long> BACKPRESSURE_TRAINING_TIMEOUT_MS =
            ConfigOption.long_("rawwise.backend.backpressure.training.timeout-ms", 30000L);

    public static final ConfigOption<Double> BACKPRESSURE_TRAINING_DEGRADE_THRESHOLD =
            ConfigOption.double_("rawwise.backend.backpressure.training.degrade-threshold", 0.85);

    public static final ConfigOption<Integer> BACKPRESSURE_INFERENCE_PERMITS =
            ConfigOption.int_("rawwise.backend.backpressure.inference.permits", 50);

    public static final ConfigOption<Long> BACKPRESSURE_INFERENCE_TIMEOUT_MS =
            ConfigOption.long_("rawwise.backend.backpressure.inference.timeout-ms", 1000L);

    public static final ConfigOption<Integer> BACKEND_BACKPRESSURE_THROTTLE_PERMITS =
            ConfigOption.int_("rawwise.backend.backpressure.throttle-permits", 50);

    // ========== 4.3.25 Frontend (9 keys) ==========

    public static final ConfigOption<Integer> FRONTEND_PORT =
            ConfigOption.int_("rawwise.frontend.port", 8080);

    public static final ConfigOption<String> FRONTEND_DEPLOYMENT_MODE =
            ConfigOption.string("rawwise.frontend.deployment.mode", "standalone");

    public static final ConfigOption<String> FRONTEND_GRPC_BACKEND_HOST =
            ConfigOption.string("rawwise.frontend.grpc.backend.host", "localhost");

    public static final ConfigOption<Integer> FRONTEND_GRPC_BACKEND_PORT =
            ConfigOption.int_("rawwise.frontend.grpc.backend.port", 9090);

    public static final ConfigOption<Integer> FRONTEND_ROUTING_REFRESH_SECONDS =
            ConfigOption.int_("rawwise.frontend.routing.refresh-seconds", 30);

    public static final ConfigOption<String> FRONTEND_GRPC_BACKEND_GROUPS =
            ConfigOption.string("rawwise.frontend.grpc.backend.groups", "");

    public static final ConfigOption<Long> FRONTEND_QUERY_TIMEOUT_SECONDS =
            ConfigOption.long_("rawwise.frontend.query.timeout-seconds", 30L);

    public static final ConfigOption<Integer> FRONTEND_SESSION_TTL_MINUTES =
            ConfigOption.int_("rawwise.frontend.session.ttl-minutes", 30);

    public static final ConfigOption<Integer> FRONTEND_SESSION_CLEANUP_INTERVAL_MINUTES =
            ConfigOption.int_("rawwise.frontend.session.cleanup-interval-minutes", 10);

    public static final ConfigOption<Integer> FRONTEND_SQL_MAX_LENGTH =
            ConfigOption.int_("rawwise.frontend.sql.max-length", 65536);

    public static final ConfigOption<Integer> FRONTEND_GRPC_CHANNEL_SHUTDOWN_TIMEOUT_SECONDS =
            ConfigOption.int_("rawwise.frontend.grpc.channel.shutdown-timeout-seconds", 5);

    public static final ConfigOption<Boolean> FRONTEND_IR_ENABLED =
            ConfigOption.bool("rawwise.frontend.ir.enabled", false);

    public static final ConfigOption<Integer> FRONTEND_RATE_LIMIT_PER_IP =
            ConfigOption.int_("rawwise.frontend.rate-limit.per-ip", 100);

    public static final ConfigOption<Integer> FRONTEND_RATE_LIMIT_GLOBAL =
            ConfigOption.int_("rawwise.frontend.rate-limit.global", 1000);

    public static final ConfigOption<Boolean> FRONTEND_GRPC_TLS_ENABLED =
            ConfigOption.bool("rawwise.frontend.grpc.tls.enabled", false);
    public static final ConfigOption<String> FRONTEND_GRPC_TLS_TRUSTSTORE_PATH =
            ConfigOption.string("rawwise.frontend.grpc.tls.truststore.path", null);
    public static final ConfigOption<String> FRONTEND_GRPC_TLS_TRUSTSTORE_PASSWORD =
            ConfigOption.string("rawwise.frontend.grpc.tls.truststore.password", null);
    public static final ConfigOption<String> FRONTEND_GRPC_TLS_TRUSTSTORE_TYPE =
            ConfigOption.string("rawwise.frontend.grpc.tls.truststore.type", "JKS");
    public static final ConfigOption<String> FRONTEND_GRPC_TLS_KEYSTORE_PATH =
            ConfigOption.string("rawwise.frontend.grpc.tls.keystore.path", null);
    public static final ConfigOption<String> FRONTEND_GRPC_TLS_KEYSTORE_PASSWORD =
            ConfigOption.string("rawwise.frontend.grpc.tls.keystore.password", null);
    public static final ConfigOption<String> FRONTEND_GRPC_TLS_KEYSTORE_TYPE =
            ConfigOption.string("rawwise.frontend.grpc.tls.keystore.type", "JKS");
    public static final ConfigOption<Boolean> FRONTEND_GRPC_MTLS_ENABLED =
            ConfigOption.bool("rawwise.frontend.grpc.mtls.enabled", false);
    public static final ConfigOption<String> FRONTEND_GRPC_TLS_MIN_PROTOCOL =
            ConfigOption.string("rawwise.frontend.grpc.tls.min-protocol", "TLSv1.2");
    public static final ConfigOption<Boolean> FRONTEND_GRPC_TLS_HOSTNAME_VERIFICATION =
            ConfigOption.bool("rawwise.frontend.grpc.tls.hostname-verification", true);
    public static final ConfigOption<String> FRONTEND_GRPC_TLS_CIPHER_SUITES =
            ConfigOption.string(
                    "rawwise.frontend.grpc.tls.cipher-suites",
                    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,"
                            + "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,"
                            + "TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_CHACHA20_POLY1305_SHA256");

    public static final ConfigOption<Integer> FRONTEND_MAX_RESULT_ROWS =
            ConfigOption.int_("rawwise.frontend.query.max.result.rows", 10000);
    public static final ConfigOption<Integer> FRONTEND_MAX_RESULT_BYTES =
            ConfigOption.int_("rawwise.frontend.query.max.result.bytes", 16777216);
    public static final ConfigOption<Integer> FRONTEND_MAX_TRAINING_RESULT_BYTES =
            ConfigOption.int_("rawwise.frontend.query.max.training.result.bytes", 268435456);
    public static final ConfigOption<Long> FRONTEND_MAX_TOTAL_RESULT_BYTES =
            ConfigOption.long_("rawwise.frontend.query.max.total-result-bytes", 268435456L);

    // ========== 4.3.26 Backend Server (9 keys) ==========

    public static final ConfigOption<Integer> SERVER_GRPC_THREAD_POOL_SIZE =
            ConfigOption.int_("rawwise.server.grpc-thread-pool-size", 10);

    public static final ConfigOption<Integer> SERVER_JOB_SCHEDULER_MAX_COMPLETED_JOBS =
            ConfigOption.int_("rawwise.server.job-scheduler.max-completed-jobs", 1000);

    public static final ConfigOption<Integer> SERVER_JOB_SCHEDULER_MAX_COLLECT_ROWS =
            ConfigOption.int_("rawwise.server.job-scheduler.max-collect-rows", 10000);

    public static final ConfigOption<Integer> SERVER_JOB_SCHEDULER_QUEUE_CAPACITY =
            ConfigOption.int_("rawwise.server.job-scheduler.queue-capacity", 100);

    public static final ConfigOption<Integer> SERVER_JOB_SCHEDULER_MONITOR_INTERVAL_SECONDS =
            ConfigOption.int_("rawwise.server.job-scheduler.monitor-interval-seconds", 5);

    public static final ConfigOption<Long> SERVER_JOB_SCHEDULER_HISTORY_RETENTION_MS =
            ConfigOption.long_("rawwise.server.job-scheduler.history-retention-ms", 3600000L);

    public static final ConfigOption<Boolean> BACKEND_TLS_ENABLED =
            ConfigOption.bool("rawwise.backend.tls.enabled", false);

    public static final ConfigOption<String> BACKEND_TLS_CERT_PATH =
            ConfigOption.string("rawwise.backend.tls.cert-path", "");

    public static final ConfigOption<String> BACKEND_TLS_KEY_PATH =
            ConfigOption.string("rawwise.backend.tls.key-path", "");

    // ========== 4.3.27 Container Memory (8 keys) ==========

    public static final ConfigOption<Long> CONTAINER_MEMORY_CACHE_TTL_MS =
            ConfigOption.long_("rawwise.container-memory.cache-ttl-ms", 1000L);

    public static final ConfigOption<Double> CONTAINER_MEMORY_SAFETY_MARGIN_RATIO =
            ConfigOption.double_("rawwise.container-memory.safety-margin-ratio", 0.15);

    public static final ConfigOption<Long> CONTAINER_MEMORY_MIN_SAFETY_MARGIN_BYTES =
            ConfigOption.long_("rawwise.container-memory.min-safety-margin-bytes", 2147483648L);

    public static final ConfigOption<Long> STORAGE_NATIVE_MEMORY_LIMIT_MB =
            ConfigOption.long_("rawwise.storage.native-memory-limit-mb", -1L);

    public static final ConfigOption<Double> CONTAINER_MEMORY_WARNING_THRESHOLD_RATIO =
            ConfigOption.double_("rawwise.container-memory.warning-threshold-ratio", 0.15);

    public static final ConfigOption<Double> CONTAINER_MEMORY_CRITICAL_THRESHOLD_RATIO =
            ConfigOption.double_("rawwise.container-memory.critical-threshold-ratio", 0.05);

    public static final ConfigOption<Long> CONTAINER_MEMORY_MIN_WARNING_BYTES =
            ConfigOption.long_("rawwise.container-memory.min-warning-bytes", 536870912L);

    public static final ConfigOption<Long> CONTAINER_MEMORY_MIN_CRITICAL_BYTES =
            ConfigOption.long_("rawwise.container-memory.min-critical-bytes", 268435456L);

    // ========== 4.3.28 Codec (1 key) ==========

    public static final ConfigOption<Integer> CODEC_MAX_VAR_FIELD_SIZE_MB =
            ConfigOption.builder("rawwise.codec.max-var-field-size-mb", 16, Integer.class)
                    .unit(ConfigOption.Unit.MIB)
                    .build();

    // ========== 4.3.28.1 Fast Path Row Cache (3 keys) ==========

    public static final ConfigOption<Long> FAST_PATH_ROW_CACHE_MAX_SIZE =
            ConfigOption.builder("rawwise.fast-path.row-cache.max-size", 10000L, Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Long> FAST_PATH_ROW_CACHE_TTL_MINUTES =
            ConfigOption.builder("rawwise.fast-path.row-cache.ttl-minutes", 30L, Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Boolean> FAST_PATH_ROW_CACHE_ENABLED =
            ConfigOption.bool("rawwise.fast-path.row-cache.enabled", true);

    // ========== 4.3.29 REPL (4 keys) ==========

    public static final ConfigOption<Integer> REPL_CONNECT_TIMEOUT_SECONDS =
            ConfigOption.int_("rawwise.repl.connect-timeout-seconds", 10);

    public static final ConfigOption<Integer> REPL_QUERY_TIMEOUT_SECONDS =
            ConfigOption.int_("rawwise.repl.query-timeout-seconds", 300);

    public static final ConfigOption<Integer> REPL_SHELL_TIMEOUT_SECONDS =
            ConfigOption.int_("rawwise.repl.shell-timeout-seconds", 30);

    public static final ConfigOption<Integer> REPL_MAX_COLUMN_WIDTH =
            ConfigOption.int_("rawwise.repl.max-column-width", 50);

    // ========== 4.3.30 Config System (15 keys) ==========

    public static final ConfigOption<Long> CONFIG_FILE_DEBOUNCE_MS =
            ConfigOption.long_("rawwise.config.file.debounce-ms", 3000L);

    public static final ConfigOption<Long> CONFIG_FILE_POLL_INTERVAL_MS =
            ConfigOption.long_("rawwise.config.file.poll-interval-ms", 5000L);

    public static final ConfigOption<Long> CONFIG_RELOAD_MIN_INTERVAL_MS =
            ConfigOption.long_("rawwise.config.reload.min-interval-ms", 5000L);

    public static final ConfigOption<Integer> CONFIG_HISTORY_MAX_VERSIONS =
            ConfigOption.int_("rawwise.config.history.max-versions", 20);

    public static final ConfigOption<Boolean> CONFIG_AUDIT_ENABLED =
            ConfigOption.bool("rawwise.config.audit.enabled", true);

    public static final ConfigOption<Integer> CONFIG_AUDIT_MAX_FILES =
            ConfigOption.int_("rawwise.config.audit.max-files", 100);

    public static final ConfigOption<Boolean> CONFIG_ADMIN_ENABLED =
            ConfigOption.bool("rawwise.config.admin.enabled", false);

    public static final ConfigOption<String> CONFIG_ADMIN_BEARER_TOKEN =
            ConfigOption.string("rawwise.config.admin.bearer-token", "");

    public static final ConfigOption<Boolean> CONFIG_STRICT_MODE =
            ConfigOption.bool("rawwise.config.strict-mode", false);

    public static final ConfigOption<String> CONFIG_PROPAGATION_MODE =
            ConfigOption.builder("rawwise.config.propagation.mode", "QUORUM", String.class)
                    .validate(
                            v -> {
                                try {
                                    PropagationMode.fromString(v);
                                    return true;
                                } catch (IllegalArgumentException e) {
                                    return false;
                                }
                            })
                    .build();

    public static final ConfigOption<Long> CONFIG_PROPAGATION_TIMEOUT_BASE_MS =
            ConfigOption.long_("rawwise.config.propagation.timeout-base-ms", 5000L);

    public static final ConfigOption<Long> CONFIG_PROPAGATION_TIMEOUT_PER_EXECUTOR_MS =
            ConfigOption.long_("rawwise.config.propagation.timeout-per-executor-ms", 500L);

    public static final ConfigOption<Long> CONFIG_HEARTBEAT_INTERVAL_MS =
            ConfigOption.long_("rawwise.config.heartbeat.interval-ms", 30000L);

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final ConfigOption<List<String>> CONFIG_ADMIN_ALLOWED_CIDRS =
            (ConfigOption)
                    ConfigOption.builder(
                                    "rawwise.config.admin.allowed-cidrs",
                                    List.of("127.0.0.1/32"),
                                    List.class)
                            .build();

    // ========== 4.3.31 MVCC (4 keys) ==========

    public static final ConfigOption<Long> MVCC_RETENTION_VERSIONS =
            ConfigOption.long_("rawwise.mvcc.retention.versions", 100L);

    public static final ConfigOption<Long> MVCC_RETENTION_MINUTES =
            ConfigOption.long_("rawwise.mvcc.retention.minutes", 60L);

    public static final ConfigOption<Long> MVCC_STORE_MAP_SIZE_MB =
            ConfigOption.long_("rawwise.mvcc.store.map-size-mb", 8192L);

    public static final ConfigOption<Long> MVCC_MEMORY_STORE_MAX_ENTRIES =
            ConfigOption.long_("rawwise.mvcc.memory-store.max-entries", 10000L);

    // ========== 4.3.32 Auth RBAC (6 keys) ==========

    public static final ConfigOption<Boolean> AUTH_ENABLED =
            ConfigOption.bool("rawwise.auth.enabled", true);

    public static final ConfigOption<Integer> AUTH_CACHE_MAX_SIZE =
            ConfigOption.int_("rawwise.auth.cache.max-size", 1000);

    public static final ConfigOption<Integer> AUTH_CACHE_EXPIRE_SECONDS =
            ConfigOption.int_("rawwise.auth.cache.expire-seconds", 30);

    public static final ConfigOption<Integer> AUTH_TOKEN_CACHE_EXPIRE_SECONDS =
            ConfigOption.int_("rawwise.auth.token-cache.expire-seconds", 300);

    public static final ConfigOption<Boolean> AUTH_UNAUTHENTICATED_DENY =
            ConfigOption.bool("rawwise.auth.unauthenticated-deny", true);

    public static final ConfigOption<Boolean> AUTH_AUDIT_ENABLED =
            ConfigOption.bool("rawwise.auth.audit.enabled", true);

    public static final ConfigOption<String> AUTH_VALIDATOR_TYPE =
            ConfigOption.builder("rawwise.auth.validator", "simple", String.class)
                    .validate(v -> "simple".equalsIgnoreCase(v) || "metastore".equalsIgnoreCase(v))
                    .build();

    public static final ConfigOption<String> AUTH_INTERNAL_TOKEN =
            ConfigOption.string("rawwise.auth.internal-token", "");

    public static final ConfigOption<Boolean> BACKEND_EXECUTOR_TLS_ENABLED =
            ConfigOption.bool("rawwise.backend.executor.tls.enabled", false);

    public static final ConfigOption<Boolean> BACKEND_TLS_EXPLICITLY_UNSAFE_DISABLE =
            ConfigOption.bool("rawwise.backend.tls.explicitly-unsafe-disable", false);

    public static final ConfigOption<Integer> BACKEND_HEALTH_PORT =
            ConfigOption.int_("rawwise.backend.health.port", 9099);

    public static final ConfigOption<Boolean> BACKEND_HA_ENABLED =
            ConfigOption.bool("rawwise.backend.ha.enabled", false);

    public static final ConfigOption<String> BACKEND_HA_OWNER_ID =
            ConfigOption.string("rawwise.backend.ha.owner-id", null);

    public static final ConfigOption<String> BACKEND_HA_ADVERTISE_ADDRESS =
            ConfigOption.string("rawwise.backend.ha.advertise-address", null);

    public static final ConfigOption<Long> BACKEND_HA_LEASE_TTL_MS =
            ConfigOption.long_("rawwise.backend.ha.lease-ttl-ms", 10_000L);

    public static final ConfigOption<Long> BACKEND_HA_RENEW_INTERVAL_MS =
            ConfigOption.long_("rawwise.backend.ha.renew-interval-ms", 3_000L);

    public static final ConfigOption<Long> BACKEND_HA_STANDBY_POLL_MS =
            ConfigOption.long_("rawwise.backend.ha.standby-poll-ms", 3_000L);

    public static final ConfigOption<String> LOGGING_SQL_LEVEL =
            ConfigOption.builder("rawwise.logging.sql-level", "DEBUG", String.class)
                    .validate(v -> "DEBUG".equalsIgnoreCase(v) || "INFO".equalsIgnoreCase(v))
                    .build();

    public static final ConfigOption<String> LOGGING_FORMAT =
            ConfigOption.builder("rawwise.logging.format", "TEXT", String.class)
                    .validate(v -> "JSON".equalsIgnoreCase(v) || "TEXT".equalsIgnoreCase(v))
                    .build();

    // ========== 4.3.33 AI Native Context (5 keys) ==========

    public static final ConfigOption<Boolean> CONTEXT_ENABLED =
            ConfigOption.bool("rawwise.context.enabled", false);

    public static final ConfigOption<Long> CONTEXT_SESSION_TTL_MS =
            ConfigOption.long_("rawwise.context.session.ttl-ms", 1800000L);

    public static final ConfigOption<Integer> CONTEXT_SESSION_HISTORY_VERSIONS =
            ConfigOption.int_("rawwise.context.session.history-versions", 10);

    public static final ConfigOption<Long> CONTEXT_SESSION_CLEANUP_INTERVAL_MS =
            ConfigOption.long_("rawwise.context.session.cleanup-interval-ms", 600000L);

    public static final ConfigOption<Integer> CONTEXT_KNOWLEDGE_MAX_ENTRIES =
            ConfigOption.int_("rawwise.context.knowledge.max-entries", 10000);

    // ========== 4.3.34 AI Native Execution (3 keys) ==========

    public static final ConfigOption<Boolean> EXECUTION_ENABLED =
            ConfigOption.bool("rawwise.execution.enabled", false);

    public static final ConfigOption<Integer> EXECUTION_PLAN_MAX_STEPS =
            ConfigOption.int_("rawwise.execution.plan.max-steps", 100);

    public static final ConfigOption<Integer> EXECUTION_RETRY_DEFAULT_MAX_ATTEMPTS =
            ConfigOption.int_("rawwise.execution.retry.default-max-attempts", 3);

    // ========== 4.3.35 AI Native Axiom (16 keys) ==========

    public static final ConfigOption<Boolean> AXIOM_ENABLED =
            ConfigOption.bool("rawwise.axiom.enabled", true);

    public static final ConfigOption<Boolean> AXIOM_GRAPH_ENABLED =
            ConfigOption.bool("rawwise.memory.semantic.enabled", true);

    // Coord merged into Orchestrator — use AXIOM_ORCHESTRATOR_ENABLED

    public static final ConfigOption<Boolean> AXIOM_ORCHESTRATOR_ENABLED =
            ConfigOption.bool("rawwise.orchestrator.enabled", true);

    public static final ConfigOption<Integer> AXIOM_MAX_GRAPH_NODES =
            ConfigOption.int_("rawwise.memory.semantic.max-nodes", 10000);

    public static final ConfigOption<Integer> AXIOM_MAX_GRAPH_EDGES =
            ConfigOption.int_("rawwise.memory.semantic.max-edges", 50000);

    public static final ConfigOption<Integer> AXIOM_ORCHESTRATOR_MAX_WORKFLOW_STEPS =
            ConfigOption.int_("rawwise.orchestrator.max-workflow-steps", 50);

    public static final ConfigOption<Integer> AXIOM_ORCHESTRATOR_MAX_CONCURRENT_INSTANCES =
            ConfigOption.int_("rawwise.orchestrator.max-concurrent-instances", 500);

    public static final ConfigOption<Integer>
            AXIOM_ORCHESTRATOR_MAX_CONCURRENT_INSTANCES_PER_AGENT =
                    ConfigOption.int_(
                            "rawwise.orchestrator.max-concurrent-instances-per-agent", 20);

    public static final ConfigOption<Integer> AXIOM_ORCHESTRATOR_MAX_STEP_INPUT_BYTES =
            ConfigOption.int_("rawwise.orchestrator.max-step-input-bytes", 256000);

    public static final ConfigOption<Integer> AXIOM_ORCHESTRATOR_MAX_STEP_OUTPUT_BYTES =
            ConfigOption.int_("rawwise.orchestrator.max-step-output-bytes", 256000);

    public static final ConfigOption<Integer> AXIOM_ORCHESTRATOR_MAX_WORKFLOW_DEPTH =
            ConfigOption.int_("rawwise.orchestrator.max-workflow-depth", 10);

    public static final ConfigOption<Integer> AXIOM_ORCHESTRATOR_MAX_WORKFLOW_FAN_OUT =
            ConfigOption.int_("rawwise.orchestrator.max-workflow-fan-out", 20);

    public static final ConfigOption<Long> AXIOM_ORCHESTRATOR_TIMEOUT_CHECK_INTERVAL_MS =
            ConfigOption.long_("rawwise.orchestrator.timeout-check-interval-ms", 5000L);

    public static final ConfigOption<Long> AXIOM_ORCHESTRATOR_RECOVERY_SCAN_INTERVAL_MS =
            ConfigOption.long_("rawwise.orchestrator.recovery-scan-interval-ms", 5000L);

    public static final ConfigOption<Integer> AXIOM_ORCHESTRATOR_PIPELINE_MAX_PENDING_MESSAGES =
            ConfigOption.int_("rawwise.orchestrator.pipeline-max-pending-messages", 1000);

    // ========== 4.3.36 Vector Search (15 keys) ==========

    public static final ConfigOption<String> VECTOR_ENGINE =
            ConfigOption.string("rawwise.vector.engine", "jvector");

    public static final ConfigOption<String> VECTOR_JVECTOR_MODE =
            ConfigOption.string("rawwise.vector.jvector.mode", "disk");

    public static final ConfigOption<Integer> VECTOR_JVECTOR_DIMENSION =
            ConfigOption.int_("rawwise.vector.jvector.dimension", 768);

    public static final ConfigOption<Integer> VECTOR_JVECTOR_GRAPH_DEGREE =
            ConfigOption.int_("rawwise.vector.jvector.graph-degree", 32);

    public static final ConfigOption<Integer> VECTOR_JVECTOR_SEARCH_TOP_K =
            ConfigOption.int_("rawwise.vector.jvector.search-top-k", 10);

    public static final ConfigOption<String> VECTOR_JVECTOR_BASE_PATH =
            ConfigOption.string("rawwise.vector.jvector.base-path", "/tmp/rawwise/vector");

    public static final ConfigOption<Integer> VECTOR_JVECTOR_BUILD_THRESHOLD =
            ConfigOption.int_("rawwise.vector.jvector.build-threshold", 10000);

    public static final ConfigOption<String> VECTOR_EMBEDDING_ENDPOINT =
            ConfigOption.string("rawwise.vector.embedding.endpoint", "");

    public static final ConfigOption<String> VECTOR_EMBEDDING_MODEL =
            ConfigOption.string("rawwise.vector.embedding.model", "text-embedding-3-small");

    public static final ConfigOption<Long> VECTOR_EMBEDDING_TIMEOUT_MS =
            ConfigOption.long_("rawwise.vector.embedding.timeout-ms", 5000L);

    public static final ConfigOption<Integer> VECTOR_EMBEDDING_MAX_RETRIES =
            ConfigOption.int_("rawwise.vector.embedding.max-retries", 3);

    public static final ConfigOption<Integer> VECTOR_EMBEDDING_CIRCUIT_BREAKER_THRESHOLD =
            ConfigOption.int_("rawwise.vector.embedding.circuit-breaker.threshold", 5);

    public static final ConfigOption<Long> VECTOR_EMBEDDING_CIRCUIT_BREAKER_RESET_MS =
            ConfigOption.long_("rawwise.vector.embedding.circuit-breaker.reset-ms", 30000L);

    public static final ConfigOption<String> VECTOR_EMBEDDING_API_KEY =
            ConfigOption.string("rawwise.vector.embedding.api-key", "");

    public static final ConfigOption<Integer> VECTOR_EMBEDDING_BATCH_SIZE =
            ConfigOption.int_("rawwise.vector.embedding.batch-size", 32);

    public static final ConfigOption<Boolean> VECTOR_WRITE_ASYNC =
            ConfigOption.bool("rawwise.vector.write.async", true);

    public static final ConfigOption<Boolean> VECTOR_RECOVERY_ON_STARTUP =
            ConfigOption.bool("rawwise.vector.recovery.on-startup", true);

    public static final ConfigOption<Boolean> VECTOR_COLD_STORAGE_APPROXIMATE =
            ConfigOption.bool("rawwise.vector.cold-storage.approximate", true);

    public static final ConfigOption<Integer> VECTOR_COLD_STORAGE_IVF_BUILD_THRESHOLD =
            ConfigOption.int_("rawwise.vector.cold-storage.ivf-build-threshold", 1000);

    public static final ConfigOption<Integer> VECTOR_COLD_STORAGE_IVF_NPROBE =
            ConfigOption.int_("rawwise.vector.cold-storage.ivf-nprobe", 8);

    // ========== 4.3.37 AI Namespace Reservation (2 keys) ==========

    public static final ConfigOption<Boolean> AI_NAMESPACE_RESERVED =
            ConfigOption.bool("rawwise.ai._reserved", false);

    public static final ConfigOption<Boolean> CACHE_VECTOR_NAMESPACE_RESERVED =
            ConfigOption.bool("rawwise.cache.vector._reserved", false);

    // ========== 4.3.38 Sensitive Credentials (3 keys) ==========

    public static final ConfigOption<String> STORAGE_S3_ACCESS_KEY_ID =
            ConfigOption.builder("rawwise.storage.s3.access-key-id", "", String.class).build();

    public static final ConfigOption<String> STORAGE_S3_SECRET_ACCESS_KEY =
            ConfigOption.builder("rawwise.storage.s3.secret-access-key", "", String.class).build();

    public static final ConfigOption<String> WAL_HDFS_DELEGATION_TOKEN =
            ConfigOption.builder("rawwise.wal.hdfs.delegation-token", "", String.class).build();

    // ========== 4.3.39 Flink Sink (4 keys) ==========

    public static final ConfigOption<Integer> FLINK_SINK_MAX_BATCH_ROWS =
            ConfigOption.builder("rawwise.flink.sink.max-batch-rows", 10000, Integer.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Long> FLINK_SINK_MAX_BATCH_BYTES =
            ConfigOption.builder(
                            "rawwise.flink.sink.max-batch-bytes", 64L * 1024 * 1024, Long.class)
                    .validatePositive()
                    .build();

    public static final ConfigOption<Boolean> FLINK_SINK_AUTO_FLUSH =
            ConfigOption.bool("rawwise.flink.sink.auto-flush", true);

    public static final ConfigOption<String> FLINK_TABLE_ID =
            ConfigOption.string("rawwise.flink.table-id", "");

    // ========== Deprecated aliases for backward compatibility ==========

    public static final ConfigOption<Boolean> STORAGE_WAL_ENABLED = WAL_ENABLED;
    public static final ConfigOption<Integer> STORAGE_WAL_BUFFER_SIZE = WAL_BUFFER_SIZE;
    public static final ConfigOption<Long> STORAGE_WAL_SYNC_INTERVAL_MS = WAL_SYNC_INTERVAL_MS;
    public static final ConfigOption<Long> STORAGE_WAL_MAX_SIZE_BYTES = WAL_MAX_SIZE_BYTES;
    public static final ConfigOption<String> STORAGE_WAL_HDFS_PATH = WAL_HDFS_PATH;
    public static final ConfigOption<String> STORAGE_WAL_COMPRESSION_CODEC = WAL_COMPRESSION_CODEC;
    public static final ConfigOption<String> STORAGE_WAL_FILE_FORMAT = WAL_FILE_FORMAT;
    public static final ConfigOption<Long> STORAGE_WAL_APPEND_TIMEOUT_MS = WAL_APPEND_TIMEOUT_MS;
    public static final ConfigOption<Long> LMDB_LEASE_TTL_MS = STORAGE_LMDB_LEASE_TTL_MS;

    // ========== ConfigMetadata Registration ==========

    static {
        // Build KEY_MAP before any registration logic
        Map<String, ConfigOption<?>> map = new HashMap<>();
        try {
            for (Field f : RawWiseOptions.class.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())
                        && ConfigOption.class.isAssignableFrom(f.getType())) {
                    ConfigOption<?> opt = (ConfigOption<?>) f.get(null);
                    if (opt != null) {
                        ConfigOption<?> existing = map.putIfAbsent(opt.key(), opt);
                        if (existing != null && existing != opt) {
                            throw new ExceptionInInitializerError(
                                    "Duplicate key '"
                                            + opt.key()
                                            + "' with different ConfigOption instances");
                        }
                    }
                }
            }
        } catch (IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
        KEY_MAP = Collections.unmodifiableMap(map);

        // 4.3.1 Cluster
        ConfigMetadata.register(CLUSTER_ID.key(), false, false, TransitionPolicy.IMMEDIATE);

        // Environment Mode
        ConfigMetadata.register(ENV_MODE.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.2 ZooKeeper
        ConfigMetadata.register(
                ZOOKEEPER_CONNECT_STRING.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ZOOKEEPER_CONNECTION_TIMEOUT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ZOOKEEPER_SESSION_TIMEOUT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ZOOKEEPER_NAMESPACE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ZOOKEEPER_RETRY_COUNT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ZOOKEEPER_RETRY_BASE_SLEEP_MS.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.3 Storage Core
        ConfigMetadata.register(STORAGE_MMAP_PATH.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(STORAGE_STORE_ID.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(STORAGE_MMAP_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_DURABILITY_LEVEL.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_RECOVERY_FAIL_ON_ERROR.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                RECOVERY_SENTINEL_FULL_SCAN_ON_MISMATCH.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                OCC_PER_KEY_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.4 Storage LMDB
        ConfigMetadata.register(
                STORAGE_LMDB_MAP_SIZE_MB.key(), false, false, TransitionPolicy.DATA_VERSION_SAFE);
        ConfigMetadata.register(
                STORAGE_LMDB_MAX_DBS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_INITIAL_MAP_SIZE_MB.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_MAX_MAP_SIZE_MB.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_USAGE_THRESHOLD.key(),
                true,
                false,
                TransitionPolicy.DATA_VERSION_SAFE);
        ConfigMetadata.register(
                STORAGE_LMDB_EXPANSION_STEP_MB.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_LEASE_TTL_MS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_KEY_BUFFER_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_VALUE_BUFFER_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_BUFFER_POOL_MAX_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_USAGE_RATIO_REFRESH_INTERVAL_MS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_SPACE_WARN_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_LMDB_SPACE_ERROR_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_WRITABLE_INSTANCES.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.5 Storage Flush
        ConfigMetadata.register(
                STORAGE_FLUSH_THRESHOLD_BYTES.key(),
                true,
                false,
                TransitionPolicy.DATA_VERSION_SAFE);
        ConfigMetadata.register(
                STORAGE_FLUSH_PARQUET_BLOCK_SIZE_MB.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_FLUSH_PARQUET_PAGE_SIZE_KB.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_FLUSH_PARQUET_DICTIONARY_ENABLED.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_FLUSH_DELETION_DELAY_MS.key(), true, false, TransitionPolicy.IMMEDIATE);

        // 4.3.5.1 Checkpoint-LSN Binding
        ConfigMetadata.register(
                CHECKPOINT_LSN_BINDING_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CHECKPOINT_LSN_BINDING_MODE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                SNAPSHOT_AUTO_GENERATE_ON_CHECKPOINT.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.6 WAL
        ConfigMetadata.register(WAL_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(WAL_BUFFER_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_SYNC_INTERVAL_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(WAL_MAX_SIZE_BYTES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(WAL_HDFS_PATH.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_COMPRESSION_CODEC.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(WAL_FILE_FORMAT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_APPEND_TIMEOUT_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(WAL_LOG_DIR.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_LOG_FILE_PREFIX.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_MAX_WRITE_RETRIES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_RETRY_BASE_DELAY_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_RECOVERY_CHECK_INTERVAL_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                WAL_CIRCUIT_BREAKER_FAILURE_THRESHOLD.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_GROUP_COMMIT_MAX_PENDING.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_TRY_NEXT_MAX_RETRIES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_TRY_NEXT_BACKOFF_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_READER_MAX_PAYLOAD_SIZE_MB.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_READER_MAX_HEADER_LENGTH_MB.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(WAL_FAILURE_POLICY.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.7 Storage Memory Budget
        ConfigMetadata.register(
                STORAGE_MEMORY_BUDGET_USAGE_THRESHOLD.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.8 Storage Instance Pool
        ConfigMetadata.register(
                STORAGE_INSTANCE_POOL_SPIN_SLEEP_MS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_INSTANCE_POOL_MAX_SPIN_COUNT.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_INSTANCE_POOL_MONITOR_INTERVAL_MINUTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.9 Storage Deletion Manager
        ConfigMetadata.register(
                STORAGE_DELETION_MANAGER_MAX_RETRY_COUNT.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.10 Metastore
        ConfigMetadata.register(METASTORE_TYPE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_BASE_PATH.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(METASTORE_PORT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_GRPC_PORT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_GRPC_HOST.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_GRPC_TARGET.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_GRPC_MAX_INBOUND_MESSAGE_SIZE_MB.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_ROCKSDB_WRITE_BUFFER_SIZE_MB.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.10.1 Metastore Backup
        ConfigMetadata.register(
                METASTORE_BACKUP_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_BACKUP_PATH.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METASTORE_BACKUP_INTERVAL_HOURS.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.11 Catalog
        ConfigMetadata.register(
                CATALOG_WAREHOUSE_PATH.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CATALOG_IMPLEMENTATION.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CATALOG_CACHE_MAXIMUM_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CATALOG_CACHE_EXPIRE_AFTER_WRITE_MINUTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CATALOG_INTENT_TIMEOUT_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CATALOG_INTENT_GC_INTERVAL_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);

        // 4.3.12 Backend Spark
        ConfigMetadata.register(
                BACKEND_SPARK_MASTER.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_SPARK_DRIVER_MEMORY.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_SPARK_DRIVER_HOST.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.13 Routing
        ConfigMetadata.register(
                ROUTING_SHARD_COUNT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                SHARD_RESOLVER_CLASS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_REGISTER_MAX_RETRIES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_REGISTER_BACKOFF_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_REGISTER_FAIL_OPEN.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_FORWARD_MAX_RETRIES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_FORWARD_BACKOFF_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_WRITE_ASSUME_PARTITION_IS_SHARD.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_WRITE_BATCH_ROWS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_COMMITTED_CACHE_TTL_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                ROUTING_COMMITTED_CACHE_CLEANUP_INTERVAL_MS.key(),
                true,
                false,
                TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                ROUTING_COMMITTED_CACHE_MAX_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_MAX_BATCH_IDS_PER_SHARD.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                ROUTING_VIRTUAL_NODES.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.14 Compaction
        ConfigMetadata.register(
                COMPACTION_STRATEGY_TYPE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_MAX_CONCURRENT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_MAX_RETRIES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_TRIGGER_INTERVAL_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                COMPACTION_SCHEDULER_PARALLELISM.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_TIERED_L0_THRESHOLD.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_TIERED_SIZE_AMPLIFICATION_FACTOR.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_LEVELED_FILE_COUNT_LIMIT.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_LEVELED_SIZE_RATIO.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_UNIVERSAL_SIZE_RATIO.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_UNIVERSAL_MIN_MERGE_WIDTH.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_MAX_LEVEL.key(), false, false, TransitionPolicy.DATA_VERSION_SAFE);
        ConfigMetadata.register(
                COMPACTION_GC_MAX_ENTRY_AGE_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_GC_MAX_PENDING_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_GC_MAX_DEAD_LETTER_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_GC_DEFERRED_GC_DELAY_SECONDS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_TRIGGER_ENTRY_COUNT_THRESHOLD.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_TRIGGER_FILE_COUNT_THRESHOLD.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                COMPACTION_TRIGGER_TIME_INTERVAL_MS.key(),
                true,
                false,
                TransitionPolicy.NEXT_CYCLE);

        // 4.3.15 Clean
        ConfigMetadata.register(CLEAN_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_ORPHAN_FILE_MIN_AGE_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_SNAPSHOT_RETENTION_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_SNAPSHOT_MIN_RETENTION.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(CLEAN_TTL_DAYS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_TTL_EXPIRE_COLUMN.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(CLEAN_MAX_RETRIES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_RETRY_BACKOFF_BASE_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_RETRY_BACKOFF_MAX_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_EXECUTOR_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_EXECUTOR_FORCED_SHUTDOWN_TIMEOUT_SECONDS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_EXECUTOR_CORE_POOL_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_EXECUTOR_MAX_POOL_MULTIPLIER.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CLEAN_EXECUTOR_QUEUE_CAPACITY.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.16 Transaction
        ConfigMetadata.register(
                TXN_DEFAULT_TIMEOUT_MS.key(), true, false, TransitionPolicy.GRACEFUL);
        ConfigMetadata.register(TXN_OLTP_TIMEOUT_MS.key(), true, false, TransitionPolicy.GRACEFUL);
        ConfigMetadata.register(
                TXN_INGESTION_TIMEOUT_MS.key(), true, false, TransitionPolicy.GRACEFUL);
        ConfigMetadata.register(
                TXN_READ_CACHE_MAX_BYTES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                TXN_CLEANUP_INTERVAL_SECONDS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                TXN_EVICTION_SHUTDOWN_TIMEOUT_MS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                TXN_EVICTION_TIMEOUT_MS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                TXN_FALLBACK_SHARD_COUNT.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.17 Fast Path Transaction
        ConfigMetadata.register(
                FAST_PATH_TXN_MAX_RETRY.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FAST_PATH_TXN_CONFLICT_BACKOFF_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FAST_PATH_TXN_CAS_ENABLED.key(), true, false, TransitionPolicy.IMMEDIATE);

        // 4.3.18 Ingestion
        ConfigMetadata.register(
                INGESTION_TXN_ISOLATION_LEVEL.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(INGESTION_TXN_MODE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_WRITE_BUFFER_MAX_BYTES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                MAX_PENDING_INSTANCES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                MAX_PENDING_BATCH_SIZE.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                MAX_PENDING_BATCH_BYTES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_BATCH_INITIAL_CAPACITY.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.18.1 Ingestion Sink
        ConfigMetadata.register(
                INGESTION_SINK_TYPE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_PARQUET_HDFS_BASE_PATH.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_PARQUET_ROW_GROUP_SIZE_BYTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_PARQUET_COMPRESSION_CODEC.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_PARQUET_MAX_PENDING_BYTES.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_PARQUET_MAX_BATCH_BYTES.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_PARQUET_MIN_BATCH_BYTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_PARQUET_ORPHAN_AGE_THRESHOLD_MS.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_LMDB_MAX_PENDING_INSTANCES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                INGESTION_SINK_LMDB_INSTANCE_MAPPED_BYTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.19 Read
        ConfigMetadata.register(READ_BATCH_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                READ_PARTITION_BATCH_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                READ_BULK_READER_INITIAL_CAPACITY.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.20 Write
        ConfigMetadata.register(WRITE_TIMEOUT_MS.key(), true, false, TransitionPolicy.GRACEFUL);
        ConfigMetadata.register(
                WRITE_SPIN_BACKOFF_NS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_WRITE_TIMEOUT_MS.key(), true, false, TransitionPolicy.IMMEDIATE);

        // 4.3.21 UpdateLog
        ConfigMetadata.register(
                UPDATELOG_READER_MAX_PAYLOAD_SIZE_MB.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                UPDATELOG_READER_MAX_HEADER_LENGTH_MB.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                UPDATELOG_READER_MAX_STRING_FIELD_LENGTH_KB.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                UPDATELOG_READER_MAX_BINARY_FIELD_LENGTH_MB.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                UPDATELOG_READER_MAX_RECORD_COUNT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                UPDATELOG_MAX_ENTRY_COUNT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                UPDATELOG_MAX_COLUMN_NAME_LENGTH_KB.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                UPDATELOG_MAX_VALUE_LENGTH_KB.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.22 Recovery
        ConfigMetadata.register(
                RECOVERY_MAX_RETRIES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(RECOVERY_BACKOFF_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(RECOVERY_TIMEOUT_MS.key(), true, false, TransitionPolicy.GRACEFUL);

        // 4.3.23 Metrics
        ConfigMetadata.register(METRICS_CLUSTER_ID.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METRICS_PROMETHEUS_PORT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METRICS_PROMETHEUS_PATH.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METRICS_ENABLE_SYSTEM.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(METRICS_ENABLE_JMX.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(METRICS_JMX_DOMAIN.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                METRICS_SAMPLING_INTERVAL_SECONDS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                METRICS_ENABLE_EXECUTOR_AGGREGATION.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.24 Backpressure
        ConfigMetadata.register(
                BACKPRESSURE_WARNING_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_THROTTLE_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_REJECT_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_QUEUE_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_CPU_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_MEMORY_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_DISK_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_NETWORK_THRESHOLD.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_QUEUE_WEIGHT.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_CPU_WEIGHT.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_MEMORY_WEIGHT.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_DISK_WEIGHT.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_NETWORK_WEIGHT.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_INTERACTIVE_PERMITS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_INTERACTIVE_TIMEOUT_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_BATCH_PERMITS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_BATCH_TIMEOUT_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_TRAINING_PERMITS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_TRAINING_TIMEOUT_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_TRAINING_DEGRADE_THRESHOLD.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_INFERENCE_PERMITS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKPRESSURE_INFERENCE_TIMEOUT_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_BACKPRESSURE_THROTTLE_PERMITS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.25 Frontend
        ConfigMetadata.register(FRONTEND_PORT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_DEPLOYMENT_MODE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_BACKEND_HOST.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_BACKEND_PORT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_ROUTING_REFRESH_SECONDS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_BACKEND_GROUPS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_QUERY_TIMEOUT_SECONDS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_SESSION_TTL_MINUTES.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                FRONTEND_SESSION_CLEANUP_INTERVAL_MINUTES.key(),
                true,
                false,
                TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                FRONTEND_SQL_MAX_LENGTH.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_CHANNEL_SHUTDOWN_TIMEOUT_SECONDS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_TRUSTSTORE_PATH.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_TRUSTSTORE_PASSWORD.key(),
                false,
                true,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_TRUSTSTORE_TYPE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_KEYSTORE_PATH.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_KEYSTORE_PASSWORD.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_KEYSTORE_TYPE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_MTLS_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_MIN_PROTOCOL.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_HOSTNAME_VERIFICATION.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_GRPC_TLS_CIPHER_SUITES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_MAX_RESULT_ROWS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_MAX_RESULT_BYTES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_MAX_TRAINING_RESULT_BYTES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_MAX_TOTAL_RESULT_BYTES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_IR_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_RATE_LIMIT_PER_IP.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FRONTEND_RATE_LIMIT_GLOBAL.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.26 Backend Server
        ConfigMetadata.register(
                SERVER_GRPC_THREAD_POOL_SIZE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                SERVER_JOB_SCHEDULER_MAX_COMPLETED_JOBS.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                SERVER_JOB_SCHEDULER_MAX_COLLECT_ROWS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                SERVER_JOB_SCHEDULER_QUEUE_CAPACITY.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                SERVER_JOB_SCHEDULER_MONITOR_INTERVAL_SECONDS.key(),
                true,
                false,
                TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                SERVER_JOB_SCHEDULER_HISTORY_RETENTION_MS.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_TLS_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_TLS_CERT_PATH.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_TLS_KEY_PATH.key(), false, true, TransitionPolicy.IMMEDIATE);

        // 4.3.27 Container Memory
        ConfigMetadata.register(
                CONTAINER_MEMORY_CACHE_TTL_MS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTAINER_MEMORY_SAFETY_MARGIN_RATIO.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTAINER_MEMORY_MIN_SAFETY_MARGIN_BYTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_NATIVE_MEMORY_LIMIT_MB.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTAINER_MEMORY_WARNING_THRESHOLD_RATIO.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTAINER_MEMORY_CRITICAL_THRESHOLD_RATIO.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTAINER_MEMORY_MIN_WARNING_BYTES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTAINER_MEMORY_MIN_CRITICAL_BYTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.28 Codec
        ConfigMetadata.register(
                CODEC_MAX_VAR_FIELD_SIZE_MB.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.28.1 Fast Path Row Cache
        ConfigMetadata.register(
                FAST_PATH_ROW_CACHE_MAX_SIZE.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FAST_PATH_ROW_CACHE_TTL_MINUTES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FAST_PATH_ROW_CACHE_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.29 REPL
        ConfigMetadata.register(
                REPL_CONNECT_TIMEOUT_SECONDS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                REPL_QUERY_TIMEOUT_SECONDS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                REPL_SHELL_TIMEOUT_SECONDS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                REPL_MAX_COLUMN_WIDTH.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.30 Config System
        ConfigMetadata.register(
                CONFIG_FILE_DEBOUNCE_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                CONFIG_FILE_POLL_INTERVAL_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                CONFIG_RELOAD_MIN_INTERVAL_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                CONFIG_HISTORY_MAX_VERSIONS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                CONFIG_AUDIT_ENABLED.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONFIG_AUDIT_MAX_FILES.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                CONFIG_ADMIN_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONFIG_ADMIN_BEARER_TOKEN.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(CONFIG_STRICT_MODE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONFIG_PROPAGATION_MODE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONFIG_PROPAGATION_TIMEOUT_BASE_MS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONFIG_PROPAGATION_TIMEOUT_PER_EXECUTOR_MS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONFIG_HEARTBEAT_INTERVAL_MS.key(), true, false, TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                CONFIG_ADMIN_ALLOWED_CIDRS.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.31 MVCC
        ConfigMetadata.register(
                MVCC_RETENTION_VERSIONS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                MVCC_RETENTION_MINUTES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                MVCC_STORE_MAP_SIZE_MB.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                MVCC_MEMORY_STORE_MAX_ENTRIES.key(), true, false, TransitionPolicy.IMMEDIATE);

        // 4.3.32 Auth RBAC
        ConfigMetadata.register(AUTH_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(AUTH_CACHE_MAX_SIZE.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AUTH_CACHE_EXPIRE_SECONDS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AUTH_TOKEN_CACHE_EXPIRE_SECONDS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AUTH_UNAUTHENTICATED_DENY.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(AUTH_AUDIT_ENABLED.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AUTH_VALIDATOR_TYPE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(AUTH_INTERNAL_TOKEN.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_EXECUTOR_TLS_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_TLS_EXPLICITLY_UNSAFE_DISABLE.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_HEALTH_PORT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(BACKEND_HA_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_HA_OWNER_ID.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_HA_ADVERTISE_ADDRESS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_HA_LEASE_TTL_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_HA_RENEW_INTERVAL_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                BACKEND_HA_STANDBY_POLL_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(LOGGING_SQL_LEVEL.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(LOGGING_FORMAT.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.33 AI Native Context
        ConfigMetadata.register(CONTEXT_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTEXT_SESSION_TTL_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTEXT_SESSION_HISTORY_VERSIONS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CONTEXT_SESSION_CLEANUP_INTERVAL_MS.key(),
                true,
                false,
                TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                CONTEXT_KNOWLEDGE_MAX_ENTRIES.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.34 AI Native Execution
        ConfigMetadata.register(EXECUTION_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                EXECUTION_PLAN_MAX_STEPS.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                EXECUTION_RETRY_DEFAULT_MAX_ATTEMPTS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.35 AI Native Axiom
        ConfigMetadata.register(AXIOM_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_GRAPH_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_ENABLED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_MAX_GRAPH_NODES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_MAX_GRAPH_EDGES.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_MAX_WORKFLOW_STEPS.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_MAX_CONCURRENT_INSTANCES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_MAX_CONCURRENT_INSTANCES_PER_AGENT.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_MAX_STEP_INPUT_BYTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_MAX_STEP_OUTPUT_BYTES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_MAX_WORKFLOW_DEPTH.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_MAX_WORKFLOW_FAN_OUT.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_TIMEOUT_CHECK_INTERVAL_MS.key(),
                true,
                false,
                TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_RECOVERY_SCAN_INTERVAL_MS.key(),
                true,
                false,
                TransitionPolicy.NEXT_CYCLE);
        ConfigMetadata.register(
                AXIOM_ORCHESTRATOR_PIPELINE_MAX_PENDING_MESSAGES.key(),
                false,
                false,
                TransitionPolicy.IMMEDIATE);

        // 4.3.36 Vector Search
        ConfigMetadata.register(VECTOR_ENGINE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_JVECTOR_MODE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_JVECTOR_DIMENSION.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_JVECTOR_GRAPH_DEGREE.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_JVECTOR_SEARCH_TOP_K.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_JVECTOR_BASE_PATH.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_JVECTOR_BUILD_THRESHOLD.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_EMBEDDING_ENDPOINT.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_EMBEDDING_MODEL.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_EMBEDDING_TIMEOUT_MS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_EMBEDDING_MAX_RETRIES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_EMBEDDING_CIRCUIT_BREAKER_THRESHOLD.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_EMBEDDING_CIRCUIT_BREAKER_RESET_MS.key(),
                true,
                false,
                TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_EMBEDDING_API_KEY.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_EMBEDDING_BATCH_SIZE.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(VECTOR_WRITE_ASYNC.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                VECTOR_RECOVERY_ON_STARTUP.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.37 AI Namespace Reservation
        ConfigMetadata.register(
                AI_NAMESPACE_RESERVED.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                CACHE_VECTOR_NAMESPACE_RESERVED.key(), false, false, TransitionPolicy.IMMEDIATE);

        // 4.3.38 Sensitive Credentials
        ConfigMetadata.register(
                STORAGE_S3_ACCESS_KEY_ID.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                STORAGE_S3_SECRET_ACCESS_KEY.key(), false, true, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                WAL_HDFS_DELEGATION_TOKEN.key(), false, true, TransitionPolicy.IMMEDIATE);

        // 4.3.39 Flink Sink
        ConfigMetadata.register(
                FLINK_SINK_MAX_BATCH_ROWS.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FLINK_SINK_MAX_BATCH_BYTES.key(), true, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(
                FLINK_SINK_AUTO_FLUSH.key(), false, false, TransitionPolicy.IMMEDIATE);
        ConfigMetadata.register(FLINK_TABLE_ID.key(), false, false, TransitionPolicy.IMMEDIATE);

        ConfigMetadata.markRegistered();
    }

    public static Set<String> allKeys() {
        return KEY_MAP.keySet();
    }

    private RawWiseOptions() {}
}
