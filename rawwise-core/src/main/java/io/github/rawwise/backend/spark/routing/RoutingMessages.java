package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.engine.txn.ConflictReport;
import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class RoutingMessages {
    private RoutingMessages() {}

    private static void validateNotBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static void validateShardTxnCommon(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String taskAttemptId) {
        validateNotBlank(basePath, "basePath");
        validateNotBlank(storeId, "storeId");
        validateNotBlank(tableIdentifier, "tableIdentifier");
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must be non-negative");
        }
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be non-negative");
        }
        if (taskAttemptId != null && taskAttemptId.isBlank()) {
            throw new IllegalArgumentException("taskAttemptId must not be blank when provided");
        }
    }

    public record RegisterExecutorMessage(String executorId, String nodeId, int slotIndex)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public RegisterExecutorMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId must not be blank");
            }
            if (slotIndex < 0) {
                throw new IllegalArgumentException("slotIndex must be non-negative");
            }
        }
    }

    public record ExecutorLostMessage(String executorId) implements Serializable {
        private static final long serialVersionUID = 1L;

        public ExecutorLostMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
        }
    }

    public record ExecutorReadyMessage(String executorId) implements Serializable {
        private static final long serialVersionUID = 1L;

        public ExecutorReadyMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
        }
    }

    public record LookupShardOwnerMessage(int shardId) implements Serializable {
        private static final long serialVersionUID = 1L;

        public LookupShardOwnerMessage {
            if (shardId < 0) {
                throw new IllegalArgumentException("shardId must be non-negative");
            }
        }
    }

    public record LookupShardOwnersMessage(Set<Integer> shardIds) implements Serializable {
        private static final long serialVersionUID = 1L;

        public LookupShardOwnersMessage {
            if (shardIds == null) {
                throw new IllegalArgumentException("shardIds must not be null");
            }
            if (shardIds.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("shardIds must not contain null");
            }
            if (shardIds.stream().anyMatch(id -> id < 0)) {
                throw new IllegalArgumentException("shardIds must be non-negative");
            }
            shardIds = Set.copyOf(new LinkedHashSet<>(shardIds));
        }
    }

    public record RegisterExecutorEndpointMessage(
            String executorId, String host, int port, String endpointName) implements Serializable {
        private static final long serialVersionUID = 1L;

        public RegisterExecutorEndpointMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            if (port <= 0) {
                throw new IllegalArgumentException("port must be positive");
            }
            if (endpointName == null || endpointName.isBlank()) {
                throw new IllegalArgumentException("endpointName must not be blank");
            }
        }
    }

    public record LookupExecutorEndpointMessage(String executorId) implements Serializable {
        private static final long serialVersionUID = 1L;

        public LookupExecutorEndpointMessage {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
        }
    }

    public record ExecutorEndpointInfo(
            String executorId, String host, int port, String endpointName) implements Serializable {
        private static final long serialVersionUID = 1L;

        public ExecutorEndpointInfo {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            if (port <= 0) {
                throw new IllegalArgumentException("port must be positive");
            }
            if (endpointName == null || endpointName.isBlank()) {
                throw new IllegalArgumentException("endpointName must not be blank");
            }
        }
    }

    public record ShardWriteRequest(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String batchId,
            byte[][] keys,
            byte[][] values)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ShardWriteRequest {
            if (basePath == null || basePath.isBlank()) {
                throw new IllegalArgumentException("basePath must not be blank");
            }
            if (storeId == null || storeId.isBlank()) {
                throw new IllegalArgumentException("storeId must not be blank");
            }
            if (tableIdentifier == null || tableIdentifier.isBlank()) {
                throw new IllegalArgumentException("tableIdentifier must not be blank");
            }
            if (shardId < 0) {
                throw new IllegalArgumentException("shardId must be non-negative");
            }
            if (epoch < 0) {
                throw new IllegalArgumentException("epoch must be non-negative");
            }
            if (batchId == null || batchId.isBlank()) {
                throw new IllegalArgumentException("batchId must not be blank");
            }
            if (keys == null || values == null) {
                throw new IllegalArgumentException("keys/values must not be null");
            }
            if (keys.length != values.length) {
                throw new IllegalArgumentException("keys/values must have the same length");
            }
        }
    }

    public record MultiShardWriteRequest(
            String basePath,
            String storeId,
            String tableIdentifier,
            int[] shardIds,
            long[] epochs,
            String batchId,
            byte[][] keys,
            byte[][] values,
            String taskAttemptId)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public MultiShardWriteRequest {
            if (basePath == null || basePath.isBlank()) {
                throw new IllegalArgumentException("basePath must not be blank");
            }
            if (storeId == null || storeId.isBlank()) {
                throw new IllegalArgumentException("storeId must not be blank");
            }
            if (tableIdentifier == null || tableIdentifier.isBlank()) {
                throw new IllegalArgumentException("tableIdentifier must not be blank");
            }
            if (batchId == null || batchId.isBlank()) {
                throw new IllegalArgumentException("batchId must not be blank");
            }
            if (shardIds == null || epochs == null || keys == null || values == null) {
                throw new IllegalArgumentException("shardIds/epochs/keys/values must not be null");
            }
            if (shardIds.length != keys.length
                    || shardIds.length != values.length
                    || shardIds.length != epochs.length) {
                throw new IllegalArgumentException(
                        "shardIds/epochs/keys/values must have the same length");
            }
            for (int i = 0; i < shardIds.length; i++) {
                if (shardIds[i] < 0) {
                    throw new IllegalArgumentException("shardIds must be non-negative");
                }
                if (epochs[i] < 0) {
                    throw new IllegalArgumentException("epochs must be non-negative");
                }
            }
            if (taskAttemptId != null && taskAttemptId.isBlank()) {
                throw new IllegalArgumentException("taskAttemptId must not be blank when provided");
            }
        }

        public MultiShardWriteRequest(
                String basePath,
                String storeId,
                String tableIdentifier,
                int[] shardIds,
                long[] epochs,
                String batchId,
                byte[][] keys,
                byte[][] values) {
            this(basePath, storeId, tableIdentifier, shardIds, epochs, batchId, keys, values, null);
        }
    }

    public record ShardWriteForward(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String batchId,
            byte[][] keys,
            byte[][] values,
            String taskAttemptId)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ShardWriteForward {
            if (basePath == null || basePath.isBlank()) {
                throw new IllegalArgumentException("basePath must not be blank");
            }
            if (storeId == null || storeId.isBlank()) {
                throw new IllegalArgumentException("storeId must not be blank");
            }
            if (tableIdentifier == null || tableIdentifier.isBlank()) {
                throw new IllegalArgumentException("tableIdentifier must not be blank");
            }
            if (shardId < 0) {
                throw new IllegalArgumentException("shardId must be non-negative");
            }
            if (epoch < 0) {
                throw new IllegalArgumentException("epoch must be non-negative");
            }
            if (batchId == null || batchId.isBlank()) {
                throw new IllegalArgumentException("batchId must not be blank");
            }
            if (keys == null || values == null) {
                throw new IllegalArgumentException("keys/values must not be null");
            }
            if (keys.length != values.length) {
                throw new IllegalArgumentException("keys/values must have the same length");
            }
            if (taskAttemptId != null && taskAttemptId.isBlank()) {
                throw new IllegalArgumentException("taskAttemptId must not be blank when provided");
            }
        }

        public ShardWriteForward(
                String basePath,
                String storeId,
                String tableIdentifier,
                int shardId,
                long epoch,
                String batchId,
                byte[][] keys,
                byte[][] values) {
            this(basePath, storeId, tableIdentifier, shardId, epoch, batchId, keys, values, null);
        }
    }

    public enum WriteAckStatus {
        OK,
        RETRY,
        STALE_EPOCH,
        ERROR
    }

    public record WriteAck(WriteAckStatus status, String message) implements Serializable {
        private static final long serialVersionUID = 1L;

        public WriteAck {
            if (status == null) {
                throw new IllegalArgumentException("status must not be null");
            }
            if (message == null) {
                throw new IllegalArgumentException("message must not be null");
            }
        }

        public static WriteAck ok() {
            return new WriteAck(WriteAckStatus.OK, "");
        }

        public static WriteAck retry(String message) {
            return new WriteAck(WriteAckStatus.RETRY, message);
        }

        public static WriteAck staleEpoch(String message) {
            return new WriteAck(WriteAckStatus.STALE_EPOCH, message);
        }

        public static WriteAck error(String message) {
            return new WriteAck(WriteAckStatus.ERROR, message);
        }
    }

    public record ShardPrepareTxn(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String taskAttemptId)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ShardPrepareTxn {
            validateShardTxnCommon(
                    basePath, storeId, tableIdentifier, shardId, epoch, taskAttemptId);
        }
    }

    public record ShardPrepareTxnResult(
            boolean success,
            long txnId,
            ConflictReport.ConflictType conflictType,
            byte[] conflictKey,
            long expectedVersion,
            long currentVersion,
            String errorMessage)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ShardPrepareTxnResult {
            if (txnId < 0) {
                throw new IllegalArgumentException("txnId must be non-negative");
            }
            if (success) {
                if (txnId <= 0) {
                    throw new IllegalArgumentException("txnId must be positive when success=true");
                }
                if (conflictType != null) {
                    throw new IllegalArgumentException(
                            "conflictType must be null when success=true");
                }
                if (conflictKey != null) {
                    throw new IllegalArgumentException(
                            "conflictKey must be null when success=true");
                }
                if (errorMessage != null) {
                    throw new IllegalArgumentException(
                            "errorMessage must be null when success=true");
                }
            } else {
                if (errorMessage == null && conflictType == null) {
                    throw new IllegalArgumentException(
                            "failure must have errorMessage or conflictType");
                }
                if (conflictType != null
                        && conflictKey == null
                        && conflictType != ConflictReport.ConflictType.PHANTOM) {
                    throw new IllegalArgumentException("conflictKey required for " + conflictType);
                }
            }
            if (conflictKey != null) {
                conflictKey = conflictKey.clone();
            }
        }

        public byte[] conflictKey() {
            return conflictKey != null ? conflictKey.clone() : null;
        }

        public static ShardPrepareTxnResult ok(long txnId) {
            return new ShardPrepareTxnResult(true, txnId, null, null, 0, 0, null);
        }

        public static ShardPrepareTxnResult conflict(
                long txnId,
                ConflictReport.ConflictType conflictType,
                byte[] conflictKey,
                long expectedVersion,
                long currentVersion) {
            if (conflictType == null) {
                throw new IllegalArgumentException(
                        "conflictType must not be null for conflict result");
            }
            if (conflictKey == null && conflictType != ConflictReport.ConflictType.PHANTOM) {
                throw new IllegalArgumentException(
                        "conflictKey must not be null for " + conflictType + " conflict");
            }
            return new ShardPrepareTxnResult(
                    false, txnId, conflictType, conflictKey, expectedVersion, currentVersion, null);
        }

        public static ShardPrepareTxnResult error(long txnId, String errorMessage) {
            if (errorMessage == null || errorMessage.isBlank()) {
                throw new IllegalArgumentException(
                        "errorMessage must not be blank for error result");
            }
            return new ShardPrepareTxnResult(false, txnId, null, null, 0, 0, errorMessage);
        }
    }

    public record ShardCommitTxn(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String taskAttemptId,
            long txnId)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ShardCommitTxn {
            validateShardTxnCommon(
                    basePath, storeId, tableIdentifier, shardId, epoch, taskAttemptId);
            if (txnId <= 0) {
                throw new IllegalArgumentException("txnId must be positive");
            }
        }
    }

    public record ShardCommitTxnResult(
            boolean success, long txnId, long commitTs, String errorMessage)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ShardCommitTxnResult {
            if (success) {
                if (txnId <= 0) {
                    throw new IllegalArgumentException("txnId must be positive when success=true");
                }
                if (commitTs <= 0) {
                    throw new IllegalArgumentException(
                            "commitTs must be positive when success=true");
                }
                if (errorMessage != null) {
                    throw new IllegalArgumentException(
                            "errorMessage must be null when success=true");
                }
            } else {
                if (txnId <= 0) {
                    throw new IllegalArgumentException("txnId must be positive when success=false");
                }
                if (errorMessage == null || errorMessage.isBlank()) {
                    throw new IllegalArgumentException(
                            "errorMessage must not be blank when success=false");
                }
            }
        }

        public static ShardCommitTxnResult ok(long txnId, long commitTs) {
            return new ShardCommitTxnResult(true, txnId, commitTs, null);
        }

        public static ShardCommitTxnResult error(long txnId, String errorMessage) {
            if (txnId <= 0) {
                throw new IllegalArgumentException(
                        "txnId must be positive for commit error result");
            }
            if (errorMessage == null || errorMessage.isBlank()) {
                throw new IllegalArgumentException(
                        "errorMessage must not be blank for error result");
            }
            return new ShardCommitTxnResult(false, txnId, 0, errorMessage);
        }
    }

    public record ShardAbortTxn(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String taskAttemptId,
            long txnId)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public ShardAbortTxn {
            validateShardTxnCommon(
                    basePath, storeId, tableIdentifier, shardId, epoch, taskAttemptId);
            if (txnId < 0) {
                throw new IllegalArgumentException(
                        "txnId must be non-negative (0 means unknown before prepare)");
            }
        }
    }
}
