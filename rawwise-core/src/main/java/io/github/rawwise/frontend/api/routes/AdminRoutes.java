package io.github.rawwise.frontend.api.routes;

import io.javalin.Javalin;
import java.lang.management.ManagementFactory;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdminRoutes {

    public static void register(Javalin app) {
        app.get(
                "/api/admin/config",
                ctx -> {
                    var config = new LinkedHashMap<String, String>();
                    System.getProperties().stringPropertyNames().stream()
                            .filter(k -> k.startsWith("rawwise."))
                            .sorted()
                            .forEach(k -> config.put(k, desensitize(k, System.getProperty(k))));
                    if (config.isEmpty()) {
                        config.put("rawwise.config", "no-properties");
                    }
                    ctx.json(config);
                });

        app.get(
                "/api/admin/threads",
                ctx -> {
                    var threadMXBean = ManagementFactory.getThreadMXBean();
                    var info =
                            Map.of(
                                    "threadCount", threadMXBean.getThreadCount(),
                                    "peakThreadCount", threadMXBean.getPeakThreadCount(),
                                    "daemonThreadCount", threadMXBean.getDaemonThreadCount());
                    ctx.json(info);
                });

        app.get(
                "/api/admin/cache",
                ctx -> {
                    ctx.json(Map.of("message", "cache stats endpoint"));
                });

        app.get(
                "/api/admin/sessions",
                ctx -> {
                    ctx.json(Map.of("message", "sessions endpoint"));
                });

        app.get(
                "/api/admin/health/detail",
                ctx -> {
                    var runtime = Runtime.getRuntime();
                    var health =
                            Map.of(
                                    "heap",
                                            Map.of(
                                                    "used",
                                                            runtime.totalMemory()
                                                                    - runtime.freeMemory(),
                                                    "max", runtime.maxMemory(),
                                                    "free", runtime.freeMemory()),
                                    "processors", runtime.availableProcessors());
                    ctx.json(health);
                });
    }

    private static String desensitize(String key, String value) {
        if (key.contains("password")
                || key.contains("secret")
                || key.contains("token")
                || key.contains("key")) {
            return "********";
        }
        return value;
    }
}
