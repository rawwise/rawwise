package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.backend.routing.RoutingCoordinator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;

@Slf4j
public class RawWiseExecutorLossListener extends SparkListener {
    private final RoutingCoordinator routingCoordinator;

    public RawWiseExecutorLossListener(RoutingCoordinator routingCoordinator) {
        this.routingCoordinator = routingCoordinator;
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        String executorId = executorRemoved.executorId();
        log.info("Executor removed detected by SparkListener: {}", executorId);
        try {
            routingCoordinator.onExecutorDown(executorId);
        } catch (Exception e) {
            log.error("Failed to handle executor removal for {}", executorId, e);
        }
    }
}
