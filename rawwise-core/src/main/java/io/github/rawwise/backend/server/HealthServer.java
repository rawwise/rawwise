package io.github.rawwise.backend.server;

import io.javalin.Javalin;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class HealthServer {

    private final Javalin app;
    private final SparkSession spark;

    public HealthServer(int port, SparkSession spark) {
        this.spark = spark;
        this.app =
                Javalin.create()
                        .get(
                                "/health",
                                ctx -> {
                                    var checks = new java.util.LinkedHashMap<String, Object>();
                                    boolean healthy = true;

                                    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
                                    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
                                    long heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
                                    long heapMax = memoryBean.getHeapMemoryUsage().getMax();
                                    checks.put(
                                            "jvm",
                                            Map.of(
                                                    "heapUsedMb", heapUsed / (1024 * 1024),
                                                    "heapMaxMb", heapMax / (1024 * 1024),
                                                    "threadCount", threadBean.getThreadCount()));

                                    if (spark != null) {
                                        try {
                                            spark.sparkContext().isStopped();
                                            checks.put("spark", "UP");
                                        } catch (Exception e) {
                                            checks.put(
                                                    "spark",
                                                    Map.of(
                                                            "status",
                                                            "DOWN",
                                                            "reason",
                                                            e.getMessage()));
                                            healthy = false;
                                        }
                                    } else {
                                        checks.put("spark", "DISABLED");
                                    }

                                    ctx.json(
                                            Map.of(
                                                    "status",
                                                    healthy ? "UP" : "DEGRADED",
                                                    "checks",
                                                    checks));
                                })
                        .get(
                                "/ready",
                                ctx -> {
                                    var checks = new java.util.LinkedHashMap<String, Object>();
                                    boolean ready = true;

                                    if (spark != null) {
                                        try {
                                            spark.sparkContext().isStopped();
                                            checks.put("spark", "UP");
                                        } catch (Exception e) {
                                            checks.put(
                                                    "spark",
                                                    Map.of(
                                                            "status",
                                                            "DOWN",
                                                            "reason",
                                                            e.getMessage()));
                                            ready = false;
                                        }
                                    }

                                    if (!ready) {
                                        ctx.status(503)
                                                .json(Map.of("status", "DOWN", "checks", checks));
                                    } else {
                                        ctx.json(Map.of("status", "UP", "checks", checks));
                                    }
                                });
        app.start(port);
        log.info("Backend health server started on port {}", port);
    }

    public void stop() {
        app.stop();
    }

    public int getPort() {
        return app.port();
    }
}
