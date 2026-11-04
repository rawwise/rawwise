package io.github.rawwise.benchmark;

import com.fasterxml.jackson.annotation.JsonProperty;

public record LatencyStats(
        @JsonProperty("median") double median,
        @JsonProperty("p95") double p95,
        @JsonProperty("p99") double p99) {}
