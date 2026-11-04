package io.github.rawwise.backend.flink;

import io.github.rawwise.engine.storage.ExecutorIdProvider;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkTaskManagerExecutorIdProvider implements ExecutorIdProvider {

    @Override
    public String resolveExecutorId() {
        String tmId = System.getenv("FLINK_TM_ID");
        if (tmId != null && !tmId.isBlank()) {
            return "tm-" + tmId;
        }

        String containerId = System.getenv("CONTAINER_ID");
        if (containerId != null && !containerId.isBlank()) {
            return "tm-" + containerId;
        }

        return "tm-unknown-" + ProcessHandle.current().pid();
    }

    @Override
    public String resolveNodeId() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            String host = System.getenv("FLINK_TM_HOST");
            if (host != null && !host.isBlank()) {
                return host;
            }
            return "node-unknown-" + ProcessHandle.current().pid();
        }
    }
}
