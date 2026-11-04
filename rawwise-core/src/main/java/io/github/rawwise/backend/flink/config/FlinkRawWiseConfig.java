package io.github.rawwise.backend.flink.config;

import io.github.rawwise.governance.config.ConfigFactory;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;

public class FlinkRawWiseConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int maxBatchRows;
    private final long maxBatchBytes;
    private final boolean autoFlush;
    private final String tableId;
    private final Map<String, String> configMap;

    private transient volatile RawWiseConfig rawwiseConfig;

    public FlinkRawWiseConfig(Configuration flinkConfig) {
        this.configMap = new HashMap<>(flinkConfig.toMap());

        RawWiseConfig nc = rawwiseConfig();

        this.maxBatchRows =
                resolveFromRawWiseConfig(
                        nc,
                        RawWiseOptions.FLINK_SINK_MAX_BATCH_ROWS,
                        flinkConfig.get(FlinkRawWiseOptions.SINK_MAX_BATCH_ROWS));
        this.maxBatchBytes =
                resolveFromRawWiseConfig(
                        nc,
                        RawWiseOptions.FLINK_SINK_MAX_BATCH_BYTES,
                        flinkConfig.get(FlinkRawWiseOptions.SINK_MAX_BATCH_BYTES));
        this.autoFlush =
                resolveFromRawWiseConfig(
                        nc,
                        RawWiseOptions.FLINK_SINK_AUTO_FLUSH,
                        flinkConfig.get(FlinkRawWiseOptions.SINK_AUTO_FLUSH));
        this.tableId =
                resolveFromRawWiseConfig(
                        nc,
                        RawWiseOptions.FLINK_TABLE_ID,
                        flinkConfig.get(FlinkRawWiseOptions.TABLE_ID));
    }

    public RawWiseConfig rawwiseConfig() {
        if (rawwiseConfig == null) {
            synchronized (this) {
                if (rawwiseConfig == null) {
                    Map<String, String> filtered =
                            configMap.entrySet().stream()
                                    .filter(e -> e.getKey().startsWith("rawwise."))
                                    .collect(
                                            Collectors.toMap(
                                                    Map.Entry::getKey, Map.Entry::getValue));
                    rawwiseConfig = ConfigFactory.fromMap(filtered);
                }
            }
        }
        return rawwiseConfig;
    }

    private <T> T resolveFromRawWiseConfig(
            RawWiseConfig nc,
            io.github.rawwise.governance.config.ConfigOption<T> option,
            T flinkFallback) {
        T value = nc.get(option);
        if (value == null || (value instanceof String s && s.isEmpty())) {
            return flinkFallback;
        }
        return value;
    }

    public int maxBatchRows() {
        return maxBatchRows;
    }

    public long maxBatchBytes() {
        return maxBatchBytes;
    }

    public boolean autoFlush() {
        return autoFlush;
    }

    public String tableId() {
        return tableId;
    }
}
