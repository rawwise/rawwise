package io.github.rawwise.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.axiom.orchestrator.InProcessCoordEngine;
import io.github.rawwise.axiom.pipeline.cognition.ActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.ActionPlan;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.DefaultActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.DefaultExecutionContext;
import io.github.rawwise.axiom.pipeline.cognition.ExecutionContext;
import io.github.rawwise.model.record.Action;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ActionPlanActivation - ActionPlan activationand InProcessCoordEngine end-to-end test")
class ActionPlanActivationTest {

    private InProcessCoordEngine coordEngine;
    private DefaultActionExecutor actionExecutor;
    private ExecutionContext context;

    @BeforeEach
    void setUp() {
        coordEngine = new InProcessCoordEngine();
        actionExecutor = new DefaultActionExecutor();
        context =
                new DefaultExecutionContext(null, List.of(), List.of(), Optional.empty(), Map.of());
    }

    @AfterEach
    void tearDown() {
        if (coordEngine != null) {
            try {
                coordEngine.close();
            } catch (Exception e) {
                /* tearDown best-effort */
            }
        }
    }

    private Action notifyAction(String actionId, String content) {
        return new Action(
                actionId,
                List.of(),
                Action.ActionType.NOTIFY,
                content.getBytes(StandardCharsets.UTF_8),
                true);
    }

    private ActionPlan chainPlan(String planId, List<Action> actions) {
        return new ActionPlan(planId, actions, CognitionEngine.PlanType.CHAIN, Map.of());
    }

    private ActionPlan dagPlan(String planId, List<Action> actions, Map<String, Set<String>> deps) {
        return new ActionPlan(planId, actions, CognitionEngine.PlanType.DAG, deps);
    }

    @Nested
    @DisplayName("S1-HappyPath - CHAIN ActionPlan sequential execution")
    class HappyPath {

        @Test
        @DisplayName("3 item NOTIFY action sequential executionall succeed")
        void chainPlanAllActionsSucceed() {
            Action a1 = notifyAction("act-1", "notification one");
            Action a2 = notifyAction("act-2", "notification two");
            Action a3 = notifyAction("act-3", "notification three");
            ActionPlan plan = chainPlan("plan-001", List.of(a1, a2, a3));

            CognitionEngine.PlanResult result =
                    coordEngine.executeActionPlan(plan, actionExecutor, context);

            assertThat(result).isNotNull();
            assertThat(result.success()).isTrue();
            assertThat(result.stepResults()).hasSize(3);
            assertThat(result.stepResults()).allSatisfy(r -> assertThat(r.success()).isTrue());
            assertThat(result.summary()).contains("plan-001");
        }

        @Test
        @DisplayName("single action CHAIN plan execute")
        void singleActionChainPlanSucceeds() {
            ActionPlan plan = chainPlan("plan-002", List.of(notifyAction("act-solo", "solo")));

            CognitionEngine.PlanResult result =
                    coordEngine.executeActionPlan(plan, actionExecutor, context);

            assertThat(result.success()).isTrue();
            assertThat(result.stepResults()).hasSize(1);
        }
    }

    @Nested
    @DisplayName("S3-FaultRecovery - action executefaildoes not block")
    class FaultRecovery {

        @Test
        @DisplayName("CHAIN plan insecond action fail stop executionsubsequent action")
        void chainPlanStopsOnFirstFailure() {
            Action a1 = notifyAction("act-ok", "first success");
            Action a2 =
                    new Action(
                            "act-fail", List.of(), Action.ActionType.RETRIEVE, new byte[0], true);
            Action a3 = notifyAction("act-skip", "should be skipped");
            ActionPlan plan = chainPlan("plan-fault-001", List.of(a1, a2, a3));

            ActionExecutor failingExecutor =
                    new StoppingActionExecutor(actionExecutor, Set.of("act-fail"));

            CognitionEngine.PlanResult result =
                    coordEngine.executeActionPlan(plan, failingExecutor, context);

            assertThat(result.success()).isFalse();
            assertThat(result.stepResults()).hasSize(2);
            assertThat(result.stepResults().get(0).success()).isTrue();
            assertThat(result.stepResults().get(1).success()).isFalse();
        }

        @Test
        @DisplayName("item action fail CHAIN plan package 1 item failure result")
        void chainPlanFirstActionFailureStopsImmediately() {
            Action a1 =
                    new Action(
                            "act-fail-first",
                            List.of(),
                            Action.ActionType.RETRIEVE,
                            new byte[0],
                            true);
            Action a2 = notifyAction("act-skip", "should be skipped");
            ActionPlan plan = chainPlan("plan-fault-002", List.of(a1, a2));

            ActionExecutor failingExecutor =
                    new StoppingActionExecutor(actionExecutor, Set.of("act-fail-first"));

            CognitionEngine.PlanResult result =
                    coordEngine.executeActionPlan(plan, failingExecutor, context);

            assertThat(result.success()).isFalse();
            assertThat(result.stepResults()).hasSize(1);
            assertThat(result.stepResults().get(0).success()).isFalse();
        }
    }

    @Nested
    @DisplayName("S4-Boundary - DAG andboundary conditions")
    class Boundary {

        @Test
        @DisplayName("DAG ActionPlan UnsupportedOperationException")
        void dagPlanThrowsUnsupportedOperationException() {
            Action a1 = notifyAction("act-dag-1", "dag action");
            ActionPlan plan = dagPlan("plan-dag-001", List.of(a1), Map.of("act-dag-1", Set.of()));

            assertThatThrownBy(() -> coordEngine.executeActionPlan(plan, actionExecutor, context))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("DAG");
        }

        @Test
        @DisplayName("action columntableof CHAIN plan returns success")
        void emptyChainPlanReturnsSuccess() {
            ActionPlan plan = chainPlan("plan-empty", List.of());

            CognitionEngine.PlanResult result =
                    coordEngine.executeActionPlan(plan, actionExecutor, context);

            assertThat(result.success()).isTrue();
            assertThat(result.stepResults()).isEmpty();
        }
    }

    static class StoppingActionExecutor implements ActionExecutor {
        private final ActionExecutor delegate;
        private final Set<String> failActionIds;

        StoppingActionExecutor(ActionExecutor delegate, Set<String> failActionIds) {
            this.delegate = delegate;
            this.failActionIds = failActionIds;
        }

        @Override
        public String executorId() {
            return delegate.executorId();
        }

        @Override
        public Set<Action.ActionType> supportedActions() {
            return delegate.supportedActions();
        }

        @Override
        public CognitionEngine.ActionResult execute(Action action, ExecutionContext context) {
            if (failActionIds.contains(action.actionId())) {
                return new CognitionEngine.ActionResult(
                        action.actionId(),
                        false,
                        "simulated failure",
                        Map.of(),
                        System.currentTimeMillis());
            }
            return delegate.execute(action, context);
        }

        @Override
        public CognitionEngine.PlanResult executePlan(ActionPlan plan, ExecutionContext context) {
            return delegate.executePlan(plan, context);
        }
    }
}
