package io.github.rawwise.execution;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.governance.context.ContextEntry;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.governance.context.DefaultContextManager;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExecutionE2ETest {

    private InMemoryMetastore metastore;
    private ContextManager contextManager;
    private ExecutionManager executionManager;
    private String sessionId;
    private final String agentId = "agent-1";
    private final String userId = "user-1";

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        contextManager = new DefaultContextManager(metastore, 10);
        executionManager = new DefaultExecutionManager(metastore, contextManager);
        sessionId = contextManager.createSession(agentId, userId, 3600000L).sessionId();
    }

    private List<ExecutionStep> createSteps(int count) {
        List<ExecutionStep> steps = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            steps.add(
                    new ExecutionStep(
                            "step-temp-" + i,
                            "dummy-plan",
                            i,
                            "Step " + i,
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-" + i,
                            Map.of("param", "value" + i),
                            null,
                            RetryPolicy.NO_RETRY,
                            i == 0
                                    ? new CompensationAction(
                                            "comp-" + i,
                                            CompensationAction.CompensationType.MARK_ONLY,
                                            "rollback-tool",
                                            Map.of("id", "${step.toolInput.param}"),
                                            "Rollback step " + i)
                                    : null,
                            "ctx:step-" + i,
                            List.of(),
                            0,
                            3,
                            0,
                            0,
                            List.of()));
        }
        return steps;
    }

    @Nested
    @DisplayName("HappyPath")
    class HappyPath {

        @Test
        @DisplayName("createexecute")
        void createPlan() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "test plan",
                            createSteps(2),
                            RetryPolicy.NO_RETRY);

            assertThat(plan.planId()).startsWith("plan-");
            assertThat(plan.status()).isEqualTo(ExecutionPlan.PlanStatus.PENDING);
            assertThat(plan.steps()).hasSize(2);
        }

        @Test
        @DisplayName("acquireexecute")
        void getPlan() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "test plan",
                            createSteps(2),
                            RetryPolicy.NO_RETRY);

            Optional<ExecutionPlan> retrieved = executionManager.getPlan(plan.planId());

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().planId()).isEqualTo(plan.planId());
            assertThat(retrieved.get().status()).isEqualTo(ExecutionPlan.PlanStatus.PENDING);
            assertThat(retrieved.get().steps()).hasSize(2);
        }

        @Test
        @DisplayName("Agentcolumn")
        void listPlansByAgent() throws IOException {
            executionManager.createPlan(
                    agentId, userId, sessionId, "plan 1", createSteps(1), RetryPolicy.NO_RETRY);
            executionManager.createPlan(
                    agentId, userId, sessionId, "plan 2", createSteps(1), RetryPolicy.NO_RETRY);

            List<ExecutionPlan> plans = executionManager.listPlansByAgent(agentId);

            assertThat(plans).hasSize(2);
        }

        @Test
        @DisplayName("will column")
        void listPlansBySession() throws IOException {
            executionManager.createPlan(
                    agentId, userId, sessionId, "plan 1", createSteps(1), RetryPolicy.NO_RETRY);
            executionManager.createPlan(
                    agentId, userId, sessionId, "plan 2", createSteps(1), RetryPolicy.NO_RETRY);

            List<ExecutionPlan> plans = executionManager.listPlansBySession(sessionId);

            assertThat(plans).hasSize(2);
        }

        @Test
        @DisplayName("")
        void cancelPlan() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "test plan",
                            createSteps(2),
                            RetryPolicy.NO_RETRY);

            boolean cancelled = executionManager.cancelPlan(plan.planId());

            assertThat(cancelled).isTrue();
            ExecutionPlan retrieved = executionManager.getPlan(plan.planId()).orElseThrow();
            assertThat(retrieved.status()).isEqualTo(ExecutionPlan.PlanStatus.CANCELLED);
        }

        @Test
        @DisplayName("execute (start->complete)")
        void stepStartToComplete() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "test plan",
                            createSteps(2),
                            RetryPolicy.NO_RETRY);

            ExecutionStep started = executionManager.startStep(plan.planId(), 0);

            assertThat(started.status()).isEqualTo(ExecutionStep.StepStatus.RUNNING);
            assertThat(started.attemptCount()).isEqualTo(1);
            ExecutionPlan planAfterStart = executionManager.getPlan(plan.planId()).orElseThrow();
            assertThat(planAfterStart.status()).isEqualTo(ExecutionPlan.PlanStatus.RUNNING);

            ExecutionStep completed =
                    executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("done"));

            assertThat(completed.status()).isEqualTo(ExecutionStep.StepStatus.SUCCEEDED);
        }

        @Test
        @DisplayName("fail")
        void stepFailure() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "test plan",
                            createSteps(1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            ExecutionStep failed =
                    executionManager.failStep(plan.planId(), 0, "something went wrong");

            assertThat(failed.status()).isEqualTo(ExecutionStep.StepStatus.FAILED);
            assertThat(failed.result()).isNotNull();
            assertThat(failed.result().error()).isEqualTo("something went wrong");

            ExecutionPlan planAfterFail = executionManager.getPlan(plan.planId()).orElseThrow();
            assertThat(planAfterFail.status()).isEqualTo(ExecutionPlan.PlanStatus.FAILED);
        }

        @Test
        @DisplayName("skip")
        void stepSkip() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "test plan",
                            createSteps(2),
                            RetryPolicy.NO_RETRY);

            ExecutionStep skipped = executionManager.skipStep(plan.planId(), 0, "not needed");

            assertThat(skipped.status()).isEqualTo(ExecutionStep.StepStatus.SKIPPED);
            assertThat(skipped.logs()).anyMatch(log -> log.message().contains("not needed"));
        }

        @Test
        @DisplayName("write backcontext")
        void stepResultWritesToContext() throws IOException {
            ExecutionStep step =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("param", "value0"),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "test-key",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "context test",
                            List.of(step),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("hello"));

            Optional<ContextEntry> entry = contextManager.getSessionEntry(sessionId, "test-key");
            assertThat(entry).isPresent();
            assertThat(entry.get().value()).isEqualTo("hello");
        }

        @Test
        @DisplayName("")
        void stepLogs() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "log test",
                            createSteps(1),
                            RetryPolicy.NO_RETRY);

            executionManager.appendStepLog(plan.planId(), 0, "INFO", "processing started");

            List<StepLog> logs = executionManager.getStepLogs(plan.planId(), 0);
            assertThat(logs).hasSize(1);
            assertThat(logs.get(0).level()).isEqualTo("INFO");
            assertThat(logs.get(0).message()).isEqualTo("processing started");
        }

        @Test
        @DisplayName("all succeed->SUCCEEDED")
        void planAllSucceed() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "success plan",
                            createSteps(2),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("ok"));
            executionManager.startStep(plan.planId(), 1);
            executionManager.completeStep(plan.planId(), 1, StepResult.ofSuccess("ok"));

            ExecutionPlan finalPlan = executionManager.getPlan(plan.planId()).orElseThrow();
            assertThat(finalPlan.status()).isEqualTo(ExecutionPlan.PlanStatus.SUCCEEDED);
            assertThat(finalPlan.finishedAt()).isGreaterThan(0);
        }

        @Test
        @DisplayName("fail->FAILED")
        void planFailOnStepFailure() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "fail plan",
                            createSteps(2),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("ok"));
            executionManager.startStep(plan.planId(), 1);
            executionManager.failStep(plan.planId(), 1, "step failed");

            ExecutionPlan finalPlan = executionManager.getPlan(plan.planId()).orElseThrow();
            assertThat(finalPlan.status()).isEqualTo(ExecutionPlan.PlanStatus.FAILED);
        }

        @Test
        @DisplayName("retryPolicy")
        void retryPolicyInheritance() throws IOException {
            RetryPolicy defaultPolicy =
                    new RetryPolicy(5, 1000, 2.0, 60000, RetryPolicy.RetryOn.ON_FAILURE);

            ExecutionStep stepNoPolicy =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of(),
                            null,
                            null,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionStep stepWithPolicy =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            new RetryPolicy(2, 500, 1.5, 10000, RetryPolicy.RetryOn.ON_FAILURE),
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "retry test",
                            List.of(stepNoPolicy, stepWithPolicy),
                            defaultPolicy);

            assertThat(plan.steps().get(0).maxAttempts()).isEqualTo(5);
            assertThat(plan.steps().get(1).maxAttempts()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Compensation")
    class Compensation {

        @Test
        @DisplayName("single MARK_ONLY")
        void compensateStepMarkOnly() throws IOException {
            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("param", "value0"),
                            null,
                            RetryPolicy.NO_RETRY,
                            new CompensationAction(
                                    "comp-0",
                                    CompensationAction.CompensationType.MARK_ONLY,
                                    "",
                                    Map.of(),
                                    ""),
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "comp test",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("done"));
            executionManager.failStep(plan.planId(), 1, "trigger compensation");

            ExecutionStep compensated = executionManager.compensateStep(plan.planId(), 0);

            assertThat(compensated.status()).isEqualTo(ExecutionStep.StepStatus.COMPENSATED);
        }

        @Test
        @DisplayName("single TOOL_CALL")
        void compensateStepToolCall() throws IOException {
            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("sql", "SELECT * FROM t"),
                            null,
                            RetryPolicy.NO_RETRY,
                            new CompensationAction(
                                    "comp-0",
                                    CompensationAction.CompensationType.TOOL_CALL,
                                    "cleanup",
                                    Map.of("cleanupCmd", "DELETE FROM t"),
                                    ""),
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "comp test",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("done"));
            executionManager.failStep(plan.planId(), 1, "trigger compensation");

            ExecutionStep compensated = executionManager.compensateStep(plan.planId(), 0);

            assertThat(compensated.status()).isEqualTo(ExecutionStep.StepStatus.COMPENSATED);
            assertThat(compensated.result().metrics()).containsKey("compensation.cleanupCmd");
        }

        @Test
        @DisplayName("single MANUAL")
        void compensateStepManual() throws IOException {
            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("param", "value0"),
                            null,
                            RetryPolicy.NO_RETRY,
                            new CompensationAction(
                                    "comp-0",
                                    CompensationAction.CompensationType.MANUAL,
                                    "",
                                    Map.of(),
                                    "manual approval required"),
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "comp test",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("done"));
            executionManager.failStep(plan.planId(), 1, "trigger compensation");

            ExecutionStep compensated = executionManager.compensateStep(plan.planId(), 0);

            assertThat(compensated.status()).isEqualTo(ExecutionStep.StepStatus.COMPENSATING);
        }

        @Test
        @DisplayName("")
        void compensatePlanReverseOrder() throws IOException {
            CompensationAction comp0 =
                    new CompensationAction(
                            "comp-0",
                            CompensationAction.CompensationType.MARK_ONLY,
                            "",
                            Map.of(),
                            "");

            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("param", "value0"),
                            null,
                            RetryPolicy.NO_RETRY,
                            comp0,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of("param", "value1"),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "comp plan test",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("ok"));
            executionManager.failStep(plan.planId(), 1, "step 1 failed");

            ExecutionPlan compensatedPlan = executionManager.compensatePlan(plan.planId(), 1);

            ExecutionStep compensatedStep0 = compensatedPlan.steps().get(0);
            assertThat(compensatedStep0.status()).isEqualTo(ExecutionStep.StepStatus.COMPENSATED);
        }

        @Test
        @DisplayName("")
        void compensationVariableSubstitution() throws IOException {
            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("sql", "SELECT * FROM orders"),
                            null,
                            RetryPolicy.NO_RETRY,
                            new CompensationAction(
                                    "comp-0",
                                    CompensationAction.CompensationType.TOOL_CALL,
                                    "rollback",
                                    Map.of("rollbackSql", "${step.toolInput.sql}"),
                                    ""),
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "comp var test",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(
                    plan.planId(), 0, StepResult.ofSuccess("5 rows inserted", Map.of("rows", "5")));
            executionManager.failStep(plan.planId(), 1, "trigger compensation");

            ExecutionStep compensated = executionManager.compensateStep(plan.planId(), 0);

            assertThat(compensated.result().metrics())
                    .containsEntry("compensation.rollbackSql", "SELECT * FROM orders");
        }

        @Test
        @DisplayName("")
        void crossStepVariableSubstitution() throws IOException {
            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            new CompensationAction(
                                    "comp-1",
                                    CompensationAction.CompensationType.TOOL_CALL,
                                    "cleanup",
                                    Map.of("refOutput", "${plan.steps[0].result.output}"),
                                    ""),
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step2 =
                    new ExecutionStep(
                            "step-temp-2",
                            "dummy-plan",
                            2,
                            "Step 2",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-2",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "cross step test",
                            List.of(step0, step1, step2),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("abc123"));
            executionManager.startStep(plan.planId(), 1);
            executionManager.completeStep(plan.planId(), 1, StepResult.ofSuccess("ok"));
            executionManager.failStep(plan.planId(), 2, "trigger compensation");

            ExecutionStep compensated = executionManager.compensateStep(plan.planId(), 1);

            assertThat(compensated.result().metrics())
                    .containsEntry("compensation.refOutput", "abc123");
        }

        @Test
        @DisplayName("")
        void compensationVariableInjectionProtection() throws IOException {
            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("sql", "SELECT * FROM orders"),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            new CompensationAction(
                                    "comp-1",
                                    CompensationAction.CompensationType.TOOL_CALL,
                                    "rollback",
                                    Map.of("injected", "${step.result.output}"),
                                    ""),
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "injection test",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 1);
            executionManager.completeStep(
                    plan.planId(), 1, StepResult.ofSuccess("${step.toolInput.sql}"));
            executionManager.failStep(plan.planId(), 0, "trigger compensation");

            ExecutionStep compensated = executionManager.compensateStep(plan.planId(), 1);

            assertThat(compensated.result().metrics())
                    .containsEntry("compensation.injected", "${step.toolInput.sql}");
        }

        @Test
        @DisplayName("non-sql toolInput")
        void nonSqlToolInputVariableSubstitution() throws IOException {
            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("tableName", "my_table"),
                            null,
                            RetryPolicy.NO_RETRY,
                            new CompensationAction(
                                    "comp-0",
                                    CompensationAction.CompensationType.TOOL_CALL,
                                    "cleanup",
                                    Map.of("targetTable", "${step.toolInput.tableName}"),
                                    ""),
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "non-sql test",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("done"));
            executionManager.failStep(plan.planId(), 1, "trigger compensation");

            ExecutionStep compensated = executionManager.compensateStep(plan.planId(), 0);

            assertThat(compensated.result().metrics())
                    .containsEntry("compensation.targetTable", "my_table");
        }
    }

    @Nested
    @DisplayName("Retry")
    class Retry {

        @Test
        @DisplayName("retry+ calculate")
        void stepRetryWithBackoff() throws IOException {
            RetryPolicy retryPolicy =
                    new RetryPolicy(3, 1000L, 2.0, 30000L, RetryPolicy.RetryOn.ON_FAILURE);

            ExecutionStep step =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of(),
                            null,
                            retryPolicy,
                            null,
                            "",
                            List.of(),
                            0,
                            3,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "retry test",
                            List.of(step),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.failStep(plan.planId(), 0, "error");

            ExecutionStep retried = executionManager.retryStep(plan.planId(), 0);

            assertThat(retried.status()).isEqualTo(ExecutionStep.StepStatus.RETRYING);

            long delay = retryPolicy.computeDelay(2);
            assertThat(delay).isEqualTo(1000L);
        }

        @Test
        @DisplayName("retry")
        void retryExceedsMaxAttempts() throws IOException {
            ExecutionStep step =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of(),
                            null,
                            new RetryPolicy(2, 100, 1.0, 1000, RetryPolicy.RetryOn.ON_FAILURE),
                            null,
                            "",
                            List.of(),
                            0,
                            2,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "retry limit test",
                            List.of(step),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.failStep(plan.planId(), 0, "error");
            executionManager.retryStep(plan.planId(), 0);
            executionManager.startStep(plan.planId(), 0);
            executionManager.failStep(plan.planId(), 0, "error again");

            assertThatThrownBy(() -> executionManager.retryStep(plan.planId(), 0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Max attempts");
        }
    }

    @Nested
    @DisplayName("ConcurrencyAndResilience")
    class ConcurrencyAndResilience {

        @Test
        @DisplayName("concurrentCASconflict(startStepconcurrent)")
        void concurrentCasConflict() throws Exception {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "concurrent test",
                            createSteps(1),
                            RetryPolicy.NO_RETRY);

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> error = new AtomicReference<>();

            Runnable task =
                    () -> {
                        try {
                            latch.await();
                            boolean success = false;
                            while (!success) {
                                try {
                                    executionManager.startStep(plan.planId(), 0);
                                    success = true;
                                } catch (IOException e) {
                                    if (e.getMessage() != null
                                            && e.getMessage().contains("CAS failed")) {
                                        continue;
                                    }
                                    throw e;
                                }
                            }
                        } catch (IllegalStateException e) {
                            if (e.getMessage() != null
                                    && e.getMessage().contains("status RUNNING")) {
                                return;
                            }
                            error.set(e);
                        } catch (Exception e) {
                            error.set(e);
                        }
                    };

            Thread t1 = new Thread(task);
            Thread t2 = new Thread(task);
            t1.start();
            t2.start();
            latch.countDown();
            t1.join(5000);
            t2.join(5000);

            assertThat(error.get()).isNull();

            ExecutionPlan finalPlan = executionManager.getPlan(plan.planId()).orElseThrow();
            assertThat(finalPlan.steps().get(0).attemptCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("recovery(resumePlan)")
        void resumePlan() throws IOException {
            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "resume test",
                            createSteps(3),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("ok"));
            executionManager.startStep(plan.planId(), 1);
            executionManager.failStep(plan.planId(), 1, "error");

            ExecutionPlan resumed = executionManager.resumePlan(plan.planId(), 1);

            assertThat(resumed.status()).isEqualTo(ExecutionPlan.PlanStatus.RUNNING);
            ExecutionStep resetStep = resumed.steps().get(1);
            assertThat(resetStep.status()).isEqualTo(ExecutionStep.StepStatus.PENDING);
            assertThat(resetStep.attemptCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("compensatePlanidempotent")
        void compensatePlanIdempotency() throws IOException {
            CompensationAction comp0 =
                    new CompensationAction(
                            "comp-0",
                            CompensationAction.CompensationType.MARK_ONLY,
                            "",
                            Map.of(),
                            "");

            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of("param", "value0"),
                            null,
                            RetryPolicy.NO_RETRY,
                            comp0,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of("param", "value1"),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "idempotent test",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("ok"));
            executionManager.startStep(plan.planId(), 1);
            executionManager.completeStep(plan.planId(), 1, StepResult.ofSuccess("ok"));
            executionManager.failStep(plan.planId(), 1, "trigger compensation");

            ExecutionPlan firstCompensation = executionManager.compensatePlan(plan.planId(), 1);
            assertThat(firstCompensation.status()).isEqualTo(ExecutionPlan.PlanStatus.COMPENSATED);

            assertThatThrownBy(() -> executionManager.compensatePlan(plan.planId(), 1))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Cannot compensate plan");
        }

        @Test
        @DisplayName("afterrecovery scenario")
        void compensateThenResume() throws IOException {
            ExecutionStep step0 =
                    new ExecutionStep(
                            "step-temp-0",
                            "dummy-plan",
                            0,
                            "Step 0",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-0",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());
            ExecutionStep step1 =
                    new ExecutionStep(
                            "step-temp-1",
                            "dummy-plan",
                            1,
                            "Step 1",
                            ExecutionStep.StepType.TOOL_CALL,
                            ExecutionStep.StepStatus.PENDING,
                            "tool-1",
                            Map.of(),
                            null,
                            RetryPolicy.NO_RETRY,
                            null,
                            "",
                            List.of(),
                            0,
                            1,
                            0,
                            0,
                            List.of());

            ExecutionPlan plan =
                    executionManager.createPlan(
                            agentId,
                            userId,
                            sessionId,
                            "compensate then resume",
                            List.of(step0, step1),
                            RetryPolicy.NO_RETRY);

            executionManager.startStep(plan.planId(), 0);
            executionManager.completeStep(plan.planId(), 0, StepResult.ofSuccess("ok"));
            executionManager.failStep(plan.planId(), 1, "step 1 failed");

            executionManager.compensatePlan(plan.planId(), 1);

            ExecutionPlan resumed = executionManager.resumePlan(plan.planId(), 1);

            assertThat(resumed.status()).isEqualTo(ExecutionPlan.PlanStatus.RUNNING);
            assertThat(resumed.steps().get(1).status()).isEqualTo(ExecutionStep.StepStatus.PENDING);
            assertThat(resumed.steps().get(1).attemptCount()).isEqualTo(0);
        }
    }
}
