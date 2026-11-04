package io.github.rawwise.test;

import io.github.rawwise.engine.spi.ConfigurableFaultInjector;
import io.github.rawwise.engine.spi.FaultInjector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractFaultInjectionTest {
    protected ConfigurableFaultInjector faultInjector;

    @BeforeEach
    void setUpFaultInjector() {
        faultInjector = new ConfigurableFaultInjector();
    }

    @AfterEach
    void tearDownFaultInjector() {
        System.getProperties().stringPropertyNames().stream()
                .filter(k -> k.startsWith("rawwise.fault.injection."))
                .forEach(System::clearProperty);
    }

    protected void enableInjection(
            FaultInjector.InjectionPoint point, FaultInjector.Action action) {
        enableInjection(point, action, 0);
    }

    protected void enableInjection(
            FaultInjector.InjectionPoint point, FaultInjector.Action action, long param) {
        String key = "rawwise.fault.injection." + point.name();
        String value = param > 0 ? action.name() + ":" + param : action.name();
        System.setProperty(key, value);
        faultInjector = new ConfigurableFaultInjector();
    }

    protected void disableInjection(FaultInjector.InjectionPoint point) {
        System.clearProperty("rawwise.fault.injection." + point.name());
        faultInjector = new ConfigurableFaultInjector();
    }
}
