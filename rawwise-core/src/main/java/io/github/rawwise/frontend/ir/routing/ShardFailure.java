package io.github.rawwise.frontend.ir.routing;

public record ShardFailure(int shardId, String errorCode, String message) {}
