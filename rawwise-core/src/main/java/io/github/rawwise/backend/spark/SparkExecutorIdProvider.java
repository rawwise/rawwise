package io.github.rawwise.backend.spark;

import io.github.rawwise.engine.storage.ExecutorIdProvider;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkExecutorIdProvider implements ExecutorIdProvider {

    @Override
    public String resolveExecutorId() {
        try {
            Class<?> tcClass = Class.forName("org.apache.spark.TaskContext");
            Object ctx = tcClass.getMethod("get").invoke(null);
            if (ctx != null) {
                String eid =
                        (String)
                                tcClass.getMethod("getLocalProperty", String.class)
                                        .invoke(ctx, "executorId");
                if (eid != null && !eid.isBlank()) {
                    return "executor-" + eid;
                }
            }
        } catch (Exception e) {
            log.debug("Failed to get executorId from TaskContext, using fallback", e);
        }
        return "executor-unknown-" + ProcessHandle.current().pid();
    }

    @Override
    public String resolveNodeId() {
        try {
            Class<?> sparkEnvClass = Class.forName("org.apache.spark.SparkEnv");
            Object env = sparkEnvClass.getMethod("get").invoke(null);
            if (env != null) {
                Object rpcEnv = sparkEnvClass.getMethod("rpcEnv").invoke(env);
                if (rpcEnv != null) {
                    Object address = rpcEnv.getClass().getMethod("address").invoke(rpcEnv);
                    if (address != null) {
                        String host = (String) address.getClass().getMethod("host").invoke(address);
                        if (host != null && !host.isBlank()) {
                            return host;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Failed to get nodeId from SparkEnv, using fallback", e);
        }
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "node-unknown-" + ProcessHandle.current().pid();
        }
    }
}
