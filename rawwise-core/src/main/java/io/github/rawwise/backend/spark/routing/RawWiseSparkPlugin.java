package io.github.rawwise.backend.spark.routing;

import java.util.Objects;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

public final class RawWiseSparkPlugin implements SparkPlugin {
    private final DriverPlugin driverPlugin;
    private final ExecutorPlugin executorPlugin;

    public RawWiseSparkPlugin() {
        this(new RawWiseDriverRoutingPlugin(), new RawWiseExecutorRoutingPlugin());
    }

    RawWiseSparkPlugin(DriverPlugin driverPlugin, ExecutorPlugin executorPlugin) {
        this.driverPlugin = Objects.requireNonNull(driverPlugin, "driverPlugin must not be null");
        this.executorPlugin =
                Objects.requireNonNull(executorPlugin, "executorPlugin must not be null");
    }

    @Override
    public DriverPlugin driverPlugin() {
        return driverPlugin;
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return executorPlugin;
    }
}
