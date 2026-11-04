package io.github.rawwise.backend.routing;

import java.io.Serializable;

public record RecoveryCompleteMessage(
        ShardId shardId,
        RoutingEpoch epoch,
        boolean success,
        String errorMessage,
        RecoveryFailureType failureType)
        implements Serializable {
    public RecoveryCompleteMessage {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (epoch == null) {
            throw new IllegalArgumentException("epoch must not be null");
        }
        if (errorMessage == null) {
            errorMessage = "";
        }
        if (failureType == null) {
            failureType = success ? null : RecoveryFailureType.UNKNOWN;
        }
    }

    public static RecoveryCompleteMessage success(ShardId shardId, RoutingEpoch epoch) {
        return new RecoveryCompleteMessage(shardId, epoch, true, "", null);
    }

    public static RecoveryCompleteMessage failure(
            ShardId shardId,
            RoutingEpoch epoch,
            String errorMessage,
            RecoveryFailureType failureType) {
        return new RecoveryCompleteMessage(shardId, epoch, false, errorMessage, failureType);
    }
}
