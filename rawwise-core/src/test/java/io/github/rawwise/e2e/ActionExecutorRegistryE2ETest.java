package io.github.rawwise.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.axiom.pipeline.cognition.ActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.ActionExecutorRegistry;
import io.github.rawwise.axiom.pipeline.cognition.ActionPlan;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.DefaultActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.ExecutionContext;
import io.github.rawwise.model.record.Action;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ActionExecutorRegistryE2ETest {

    @Test
    void registerAndResolveByActionType() {
        ActionExecutorRegistry registry = new ActionExecutorRegistry();
        ActionExecutor executor =
                new StubExecutor("video-executor", Set.of(Action.ActionType.CORRECT));

        registry.register(Action.ActionType.CORRECT, executor);

        ActionExecutor resolved = registry.resolve(Action.ActionType.CORRECT);
        assertNotNull(resolved);
        assertEquals("video-executor", resolved.executorId());
    }

    @Test
    void resolveReturnsNullForUnregisteredType() {
        ActionExecutorRegistry registry = new ActionExecutorRegistry();
        ActionExecutor resolved = registry.resolve(Action.ActionType.RETRIEVE);
        assertNull(resolved);
    }

    @Test
    void registerOverridesPreviousExecutor() {
        ActionExecutorRegistry registry = new ActionExecutorRegistry();
        ActionExecutor executor1 =
                new StubExecutor("executor-1", Set.of(Action.ActionType.CORRECT));
        ActionExecutor executor2 =
                new StubExecutor("executor-2", Set.of(Action.ActionType.CORRECT));

        registry.register(Action.ActionType.CORRECT, executor1);
        registry.register(Action.ActionType.CORRECT, executor2);

        assertEquals("executor-2", registry.resolve(Action.ActionType.CORRECT).executorId());
    }

    @Test
    void listRegisteredTypes() {
        ActionExecutorRegistry registry = new ActionExecutorRegistry();
        registry.register(
                Action.ActionType.CORRECT,
                new StubExecutor("e1", Set.of(Action.ActionType.CORRECT)));
        registry.register(
                Action.ActionType.NOTIFY, new StubExecutor("e2", Set.of(Action.ActionType.NOTIFY)));

        Set<Action.ActionType> types = registry.registeredTypes();
        assertEquals(2, types.size());
        assertTrue(types.contains(Action.ActionType.CORRECT));
        assertTrue(types.contains(Action.ActionType.NOTIFY));
    }

    @Test
    void unregisterRemovesExecutor() {
        ActionExecutorRegistry registry = new ActionExecutorRegistry();
        registry.register(
                Action.ActionType.CORRECT,
                new StubExecutor("e1", Set.of(Action.ActionType.CORRECT)));

        registry.unregister(Action.ActionType.CORRECT);

        assertNull(registry.resolve(Action.ActionType.CORRECT));
    }

    @Test
    void registerRejectsNull() {
        ActionExecutorRegistry registry = new ActionExecutorRegistry();
        assertThrows(
                IllegalArgumentException.class,
                () -> registry.register(null, new StubExecutor("e", Set.of())));
        assertThrows(
                IllegalArgumentException.class,
                () -> registry.register(Action.ActionType.CORRECT, null));
    }

    @Test
    void productionWiring_registryResolvesCorrectToDefaultActionExecutor() {
        ActionExecutorRegistry registry = new ActionExecutorRegistry();
        DefaultActionExecutor defaultExecutor = new DefaultActionExecutor("default-test");
        registry.register(Action.ActionType.CORRECT, defaultExecutor);

        ActionExecutor resolved = registry.resolve(Action.ActionType.CORRECT);
        assertNotNull(resolved);
        assertTrue(resolved instanceof DefaultActionExecutor);
    }

    record StubExecutor(String id, Set<Action.ActionType> actions) implements ActionExecutor {
        @Override
        public String executorId() {
            return id;
        }

        @Override
        public Set<Action.ActionType> supportedActions() {
            return actions;
        }

        @Override
        public CognitionEngine.ActionResult execute(Action action, ExecutionContext context) {
            return null;
        }

        @Override
        public CognitionEngine.PlanResult executePlan(ActionPlan plan, ExecutionContext context) {
            return null;
        }
    }
}
