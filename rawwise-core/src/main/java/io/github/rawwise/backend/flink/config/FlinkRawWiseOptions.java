package io.github.rawwise.backend.flink.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class FlinkRawWiseOptions {

    private FlinkRawWiseOptions() {}

    public static final ConfigOption<Integer> SINK_MAX_BATCH_ROWS =
            ConfigOptions.key("rawwise.flink.sink.max-batch-rows")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Max rows per batch before flush");

    public static final ConfigOption<Long> SINK_MAX_BATCH_BYTES =
            ConfigOptions.key("rawwise.flink.sink.max-batch-bytes")
                    .longType()
                    .defaultValue(64L * 1024 * 1024)
                    .withDescription("Max bytes per batch before flush (default 64MB)");

    public static final ConfigOption<Boolean> SINK_AUTO_FLUSH =
            ConfigOptions.key("rawwise.flink.sink.auto-flush")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Auto-flush when batch size threshold reached. "
                                    + "When false, data is only flushed at checkpoint boundaries. "
                                    + "Disabling auto-flush reduces I/O but increases memory usage.");

    public static final ConfigOption<String> TABLE_ID =
            ConfigOptions.key("rawwise.flink.table-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RawWise table ID to write to");
}
