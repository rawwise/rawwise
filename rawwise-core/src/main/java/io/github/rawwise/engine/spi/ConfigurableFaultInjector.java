package io.github.rawwise.engine.spi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigurableFaultInjector implements FaultInjector {
    private final Map<InjectionPoint, Action> actions = new ConcurrentHashMap<>();
    private final Map<InjectionPoint, Long> params = new ConcurrentHashMap<>();

    public ConfigurableFaultInjector() {
        String envMode = System.getProperty("rawwise.env", "dev");
        if ("production".equalsIgnoreCase(envMode)) {
            log.info("Fault injection disabled in production mode");
            return;
        }
        if (!Boolean.parseBoolean(System.getProperty("rawwise.fault.injection.enabled", "false"))) {
            return;
        }
        String prefix = "rawwise.fault.injection.";
        System.getProperties().stringPropertyNames().stream()
                .filter(k -> k.startsWith(prefix))
                .forEach(
                        k -> {
                            String pointName = k.substring(prefix.length());
                            String value = System.getProperty(k);
                            try {
                                InjectionPoint point = InjectionPoint.valueOf(pointName);
                                String[] parts = value.split(":");
                                Action action = Action.valueOf(parts[0]);
                                long param = parts.length > 1 ? Long.parseLong(parts[1]) : 0;
                                actions.put(point, action);
                                params.put(point, param);
                                log.warn(
                                        "FAULT INJECTION ENABLED: {} -> {} (param={})",
                                        point,
                                        action,
                                        param);
                            } catch (IllegalArgumentException e) {
                                log.error("Invalid fault injection config: {}={}", k, value);
                            }
                        });
    }

    @Override
    public void inject(InjectionPoint point) {
        if (!isEnabled(point)) return;
        Action action = actions.get(point);
        long param = params.getOrDefault(point, 0L);
        log.warn("FAULT INJECTION TRIGGERED: {} -> {}", point, action);
        switch (action) {
            case CRASH -> Runtime.getRuntime().halt(1);
            case THROW -> throw new FaultInjectionException(point);
            case DELAY -> {
                try {
                    Thread.sleep(param);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            case HANG -> {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public boolean isEnabled(InjectionPoint point) {
        return actions.containsKey(point);
    }

    public static class FaultInjectionException extends RuntimeException {
        public FaultInjectionException(InjectionPoint point) {
            super("Fault injection at " + point);
        }
    }
}
