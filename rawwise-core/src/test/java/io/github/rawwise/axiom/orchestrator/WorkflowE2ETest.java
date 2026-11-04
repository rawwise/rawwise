package io.github.rawwise.axiom.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.axiom.memory.exception.WorkflowDefinitionInUseException;
import io.github.rawwise.axiom.memory.exception.WorkflowNotFoundException;
import io.github.rawwise.axiom.memory.exception.WorkflowQuotaExceededException;
import io.github.rawwise.axiom.memory.exception.WorkflowStepInvalidStateException;
import io.github.rawwise.axiom.memory.exception.WorkflowStepNotFoundException;
import io.github.rawwise.axiom.pipeline.state.DefaultStateContract;
import io.github.rawwise.axiom.pipeline.state.DefaultStateEngine;
import io.github.rawwise.axiom.pipeline.state.StateEngine;
import io.github.rawwise.governance.auth.Principal;
import io.github.rawwise.governance.context.DefaultContextManager;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Workflow E2E - Workflow Engine full-chain business scenario test")
class WorkflowE2ETest {

    private AgentOrchestrator orchestrator;
    private Metastore metastore;

    private final Principal.AgentPrincipal agent1 =
            new Principal.AgentPrincipal("agent-1", "user-1", "system");
    private final Principal.AgentPrincipal agent2 =
            new Principal.AgentPrincipal("agent-2", "user-2", "system");

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        var contextManager = new DefaultContextManager(metastore, 10);
        StateEngine stateEngine = new DefaultStateEngine(metastore, new DefaultStateContract());
        DefaultWorkflowCoordinator coordinator = new DefaultWorkflowCoordinator();
        AxiomOrchestratorOptions options = new AxiomOrchestratorOptions(10, 5, 50, 10, 10, 30_000);
        orchestrator = new DefaultAgentOrchestrator(coordinator, stateEngine, options);
        coordinator.setOrchestrator(orchestrator);
    }

    private WorkflowStepDef step(String stepId, String name) {
        return new WorkflowStepDef(
                stepId,
                name,
                agent1,
                StepType.TOOL_CALL,
                new StepConfig.ToolCallConfig("tool-" + stepId, Map.of()),
                30_000,
                RetryPolicy.NO_RETRY,
                null,
                Map.of());
    }

    private WorkflowStepDef stepWithRetry(String stepId, String name, int maxRetries) {
        return new WorkflowStepDef(
                stepId,
                name,
                agent1,
                StepType.TOOL_CALL,
                new StepConfig.ToolCallConfig("tool-" + stepId, Map.of()),
                30_000,
                new RetryPolicy(maxRetries, 100, 2.0),
                null,
                Map.of());
    }

    private WorkflowStepDef stepWithCompensation(String stepId, String name) {
        return new WorkflowStepDef(
                stepId,
                name,
                agent1,
                StepType.TOOL_CALL,
                new StepConfig.ToolCallConfig("tool-" + stepId, Map.of()),
                30_000,
                RetryPolicy.NO_RETRY,
                new CompensationAction(CompensationType.TOOL_CALL, Map.of(), Map.of()),
                Map.of());
    }

    @Nested
    @DisplayName("S1-HappyPath - lifecycle")
    class HappyPath {

        @Test
        @DisplayName(": ->start->complete ->instancecomplete")
        void fullLinearWorkflowLifecycle() {
            WorkflowStepDef step1 = step("s1", "Extract");
            WorkflowStepDef step2 = step("s2", "Analyze");
            WorkflowStepDef step3 = step("s3", "Report");
            List<WorkflowEdgeDef> edges =
                    List.of(
                            new WorkflowEdgeDef("s1", "s2", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("s2", "s3", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "linear-pipeline", agent1.id(), List.of(step1, step2, step3), edges);
            assertThat(result).isInstanceOf(WorkflowValidationResult.Valid.class);

            WorkflowDefinition definition = ((WorkflowValidationResult.Valid) result).definition();
            assertThat(definition.workflowId()).isNotEmpty();
            assertThat(definition.name()).isEqualTo("linear-pipeline");
            assertThat(definition.steps()).hasSize(3);

            WorkflowInstance instance =
                    orchestrator.startWorkflow(
                            definition.workflowId(), agent1.id(), Map.of("input", "data"), null);
            assertThat(instance.status()).isEqualTo(WorkflowInstanceStatus.RUNNING);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            assertThat(states).hasSize(3);
            assertThat(states.stream().filter(s -> s.status() == StepStatus.READY).count())
                    .isEqualTo(1);

            orchestrator.completeStep(
                    instance.instanceId(), "s1", Map.of("extracted", "result-1"), null);
            states = orchestrator.getStepStates(instance.instanceId());
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("s1"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.COMPLETED);
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("s2"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.READY);

            orchestrator.completeStep(
                    instance.instanceId(), "s2", Map.of("analyzed", "result-2"), null);
            orchestrator.completeStep(
                    instance.instanceId(), "s3", Map.of("reported", "result-3"), null);

            WorkflowInstance completed = orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(completed.status()).isEqualTo(WorkflowInstanceStatus.COMPLETED);
        }

        @Test
        @DisplayName("DAG : ->")
        void diamondDagWorkflow() {
            WorkflowStepDef start = step("start", "Start");
            WorkflowStepDef branchA = step("branchA", "Branch A");
            WorkflowStepDef branchB = step("branchB", "Branch B");
            WorkflowStepDef merge = step("merge", "Merge");
            List<WorkflowEdgeDef> edges =
                    List.of(
                            new WorkflowEdgeDef("start", "branchA", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("start", "branchB", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("branchA", "merge", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("branchB", "merge", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "diamond", agent1.id(), List.of(start, branchA, branchB, merge), edges);
            assertThat(result).isInstanceOf(WorkflowValidationResult.Valid.class);

            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.completeStep(instance.instanceId(), "start", Map.of(), null);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("branchA"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.READY);
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("branchB"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.READY);

            orchestrator.completeStep(instance.instanceId(), "branchA", Map.of("a", "1"), null);
            WorkflowInstance afterA = orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(afterA.status()).isEqualTo(WorkflowInstanceStatus.RUNNING);

            orchestrator.completeStep(instance.instanceId(), "branchB", Map.of("b", "2"), null);
            List<WorkflowStepState> afterBoth = orchestrator.getStepStates(instance.instanceId());
            assertThat(
                            afterBoth.stream()
                                    .filter(s -> s.stepId().equals("merge"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.READY);

            orchestrator.completeStep(instance.instanceId(), "merge", Map.of("merged", "ab"), null);
            WorkflowInstance completed = orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(completed.status()).isEqualTo(WorkflowInstanceStatus.COMPLETED);
        }

        @Test
        @DisplayName("conditional edge: ON_FAILUREbranch")
        void conditionalEdgeOnFailure() {
            WorkflowStepDef extract = step("extract", "Extract");
            WorkflowStepDef success = step("success", "Success Path");
            WorkflowStepDef failure = step("failure", "Failure Path");
            List<WorkflowEdgeDef> edges =
                    List.of(
                            new WorkflowEdgeDef("extract", "success", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("extract", "failure", EdgeCondition.ON_FAILURE));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "conditional", agent1.id(), List.of(extract, success, failure), edges);
            assertThat(result).isInstanceOf(WorkflowValidationResult.Valid.class);

            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.failStep(
                    instance.instanceId(),
                    "extract",
                    new StepError("EXTRACT_FAILED", "extraction failed", true, "execution"),
                    null);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("failure"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.READY);
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("success"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.SKIPPED);
        }

        @Test
        @DisplayName(": ->acquire->columntable->delete")
        void workflowDefinitionCrud() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "crud-test", agent1.id(), List.of(step1), List.of());
            assertThat(result).isInstanceOf(WorkflowValidationResult.Valid.class);

            String workflowId = ((WorkflowValidationResult.Valid) result).definition().workflowId();

            WorkflowDefinition fetched = orchestrator.getWorkflow(workflowId);
            assertThat(fetched).isNotNull();
            assertThat(fetched.name()).isEqualTo("crud-test");

            List<WorkflowDefinition> list = orchestrator.listWorkflows(agent1.id(), 0, 10);
            assertThat(list).hasSize(1);

            boolean deleted = orchestrator.deleteWorkflow(workflowId);
            assertThat(deleted).isTrue();
            assertThat(orchestrator.getWorkflow(workflowId)).isNull();
        }

        @Test
        @DisplayName("fail-> instance isFAILED")
        void stepFailureMarksInstanceFailed() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowStepDef step2 = step("s2", "Step 2");
            List<WorkflowEdgeDef> edges =
                    List.of(new WorkflowEdgeDef("s1", "s2", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "fail-test", agent1.id(), List.of(step1, step2), edges);
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.failStep(
                    instance.instanceId(),
                    "s1",
                    new StepError("STEP_FAILED", "step failed", false, "execution"),
                    null);

            WorkflowInstance failed = orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(failed.status()).isEqualTo(WorkflowInstanceStatus.FAILED);
        }

        @Test
        @DisplayName("rowinof")
        void cancelRunningWorkflow() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "cancel-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            boolean cancelled =
                    orchestrator.cancelWorkflow(instance.instanceId(), "user requested");
            assertThat(cancelled).isTrue();

            WorkflowInstance cancelledInstance =
                    orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(cancelledInstance.status()).isEqualTo(WorkflowInstanceStatus.CANCELLED);
        }

        @Test
        @DisplayName(": fail-> -> complete")
        void workflowCompensationLifecycle() {
            WorkflowStepDef step1 = stepWithCompensation("s1", "Step 1");
            WorkflowStepDef step2 = stepWithCompensation("s2", "Step 2");
            List<WorkflowEdgeDef> edges =
                    List.of(new WorkflowEdgeDef("s1", "s2", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "compensate-test", agent1.id(), List.of(step1, step2), edges);
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.completeStep(instance.instanceId(), "s1", Map.of(), null);
            orchestrator.failStep(
                    instance.instanceId(),
                    "s2",
                    new StepError("STEP_FAILED", "step 2 failed", false, "execution"),
                    null);

            boolean compensated =
                    orchestrator.compensateWorkflow(instance.instanceId(), "step 2 failed");
            assertThat(compensated).isTrue();

            WorkflowInstance compensating = orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(compensating.status()).isEqualTo(WorkflowInstanceStatus.COMPENSATING);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("s1"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.COMPENSATING);

            orchestrator.compensateStep(instance.instanceId(), "s1", Map.of(), null);

            WorkflowInstance compensatedInstance =
                    orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(compensatedInstance.status()).isEqualTo(WorkflowInstanceStatus.COMPENSATED);
        }

        @Test
        @DisplayName("retry: canretryerrortriggerRETRYINGstatus")
        void stepRetryOnRetryableError() {
            WorkflowStepDef step1 = stepWithRetry("s1", "Retry Step", 2);
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "retry-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.failStep(
                    instance.instanceId(),
                    "s1",
                    new StepError("TRANSIENT", "transient error", true, "network"),
                    null);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            WorkflowStepState stepState =
                    states.stream().filter(s -> s.stepId().equals("s1")).findFirst().get();
            assertThat(stepState.status()).isEqualTo(StepStatus.RETRYING);
            assertThat(stepState.retryCount()).isEqualTo(1);
            assertThat(stepState.nextRetryAt()).isGreaterThan(0);

            orchestrator.markStepReady(instance.instanceId(), "s1");
            orchestrator.completeStep(instance.instanceId(), "s1", Map.of("success", true), null);

            WorkflowInstance completed = orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(completed.status()).isEqualTo(WorkflowInstanceStatus.COMPLETED);
        }

        @Test
        @DisplayName("timeout : timeoutWorkflowtrigger (has)")
        void timeoutWorkflowWithCompensationTransitionsCorrectly() {
            WorkflowStepDef step1 = stepWithCompensation("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "timeout-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(
                            def.workflowId(), agent1.id(), Map.of(), Duration.ofMillis(100));

            boolean timedOut = orchestrator.timeoutWorkflow(instance.instanceId());
            assertThat(timedOut).isTrue();

            WorkflowInstance timedOutInstance =
                    orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(timedOutInstance.status()).isEqualTo(WorkflowInstanceStatus.COMPENSATING);
        }

        @Test
        @DisplayName("timeout : no timeoutWorkflowtriggerCANCELLED")
        void timeoutWorkflowWithoutCompensationTransitionsToCancelled() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "timeout-no-comp-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(
                            def.workflowId(), agent1.id(), Map.of(), Duration.ofMillis(100));

            boolean timedOut = orchestrator.timeoutWorkflow(instance.instanceId());
            assertThat(timedOut).isTrue();

            WorkflowInstance timedOutInstance =
                    orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(timedOutInstance.status()).isEqualTo(WorkflowInstanceStatus.CANCELLED);
        }

        @Test
        @DisplayName("recovery : resumeWorkflowfrom")
        void resumeWorkflowFromStep() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowStepDef step2 = step("s2", "Step 2");
            WorkflowStepDef step3 = step("s3", "Step 3");
            List<WorkflowEdgeDef> edges =
                    List.of(
                            new WorkflowEdgeDef("s1", "s2", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("s2", "s3", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "resume-test", agent1.id(), List.of(step1, step2, step3), edges);
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.completeStep(instance.instanceId(), "s1", Map.of(), null);
            orchestrator.failStep(
                    instance.instanceId(),
                    "s2",
                    new StepError("FAILED", "failed", false, "execution"),
                    null);

            WorkflowInstance resumed =
                    orchestrator.resumeWorkflow(
                            instance.instanceId(), "s2", Map.of("retryInput", "data"));
            assertThat(resumed.status()).isEqualTo(WorkflowInstanceStatus.RUNNING);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("s2"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.READY);

            orchestrator.completeStep(instance.instanceId(), "s2", Map.of(), null);
            orchestrator.completeStep(instance.instanceId(), "s3", Map.of(), null);

            WorkflowInstance completed = orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(completed.status()).isEqualTo(WorkflowInstanceStatus.COMPLETED);
        }
    }

    @Nested
    @DisplayName("S2-Concurrency - concurrent operations")
    class Concurrency {

        @Test
        @DisplayName("concurrentcomplete sync")
        void concurrentCompleteDifferentSteps() throws Exception {
            WorkflowStepDef start = step("start", "Start");
            WorkflowStepDef branchA = step("branchA", "Branch A");
            WorkflowStepDef branchB = step("branchB", "Branch B");
            WorkflowStepDef merge = step("merge", "Merge");
            List<WorkflowEdgeDef> edges =
                    List.of(
                            new WorkflowEdgeDef("start", "branchA", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("start", "branchB", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("branchA", "merge", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("branchB", "merge", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "concurrent-diamond",
                            agent1.id(),
                            List.of(start, branchA, branchB, merge),
                            edges);
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.completeStep(instance.instanceId(), "start", Map.of(), null);

            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);

            Runnable taskA =
                    () -> {
                        try {
                            startLatch.await();
                            orchestrator.completeStep(
                                    instance.instanceId(), "branchA", Map.of("a", "1"), null);
                            successCount.incrementAndGet();
                        } catch (Exception ignored) {
                        }
                    };

            Runnable taskB =
                    () -> {
                        try {
                            startLatch.await();
                            orchestrator.completeStep(
                                    instance.instanceId(), "branchB", Map.of("b", "2"), null);
                            successCount.incrementAndGet();
                        } catch (Exception ignored) {
                        }
                    };

            var executor = Executors.newFixedThreadPool(2);
            executor.submit(taskA);
            executor.submit(taskB);
            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            assertThat(successCount.get()).isGreaterThanOrEqualTo(1);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            WorkflowStepState mergeState =
                    states.stream().filter(s -> s.stepId().equals("merge")).findFirst().get();
            if (successCount.get() == 2) {
                assertThat(mergeState.status()).isEqualTo(StepStatus.READY);
            }
        }

        @Test
        @DisplayName("concurrentstartdifferent")
        void concurrentStartDifferentWorkflows() throws Exception {
            WorkflowStepDef step1 = step("s1", "Step 1");
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);

            String[] workflowIds = new String[3];
            for (int i = 0; i < 3; i++) {
                WorkflowValidationResult result =
                        orchestrator.defineWorkflow(
                                "workflow-" + i, agent1.id(), List.of(step1), List.of());
                workflowIds[i] =
                        ((WorkflowValidationResult.Valid) result).definition().workflowId();
            }

            for (int i = 0; i < 3; i++) {
                final int idx = i;
                new Thread(
                                () -> {
                                    try {
                                        startLatch.await();
                                        orchestrator.startWorkflow(
                                                workflowIds[idx], agent1.id(), Map.of(), null);
                                        successCount.incrementAndGet();
                                    } catch (Exception ignored) {
                                    }
                                })
                        .start();
            }

            startLatch.countDown();
            Thread.sleep(2000);
            assertThat(successCount.get()).isEqualTo(3);
        }

        @Test
        @DisplayName("requestTokenidempotent: sametokenduplicatecompleteStep duplicatetrigger")
        void requestTokenIdempotency() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowStepDef step2 = step("s2", "Step 2");
            List<WorkflowEdgeDef> edges =
                    List.of(new WorkflowEdgeDef("s1", "s2", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "idempotent-test", agent1.id(), List.of(step1, step2), edges);
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            String token = "token-123";
            orchestrator.completeStep(
                    instance.instanceId(), "s1", Map.of("result", "first"), token);

            WorkflowStepState first =
                    orchestrator.getStepStates(instance.instanceId()).stream()
                            .filter(s -> s.stepId().equals("s1"))
                            .findFirst()
                            .get();
            assertThat(first.status()).isEqualTo(StepStatus.COMPLETED);

            orchestrator.completeStep(
                    instance.instanceId(), "s1", Map.of("result", "duplicate"), token);

            WorkflowStepState second =
                    orchestrator.getStepStates(instance.instanceId()).stream()
                            .filter(s -> s.stepId().equals("s1"))
                            .findFirst()
                            .get();
            assertThat(second.status()).isEqualTo(StepStatus.COMPLETED);
        }
    }

    @Nested
    @DisplayName("S3-FaultRecovery - failure recovery")
    class FaultRecovery {

        @Test
        @DisplayName("non- status alreadyhas status")
        void invalidStepTransitionDoesNotCorruptState() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "invalid-transition-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.completeStep(instance.instanceId(), "s1", Map.of(), null);

            WorkflowStepState state =
                    orchestrator.completeStep(
                            instance.instanceId(), "s1", Map.of("duplicate", true), null);
            assertThat(state.status()).isEqualTo(StepStatus.COMPLETED);
        }

        @Test
        @DisplayName("fail already")
        void compensateStepOnNonCompensatingStepNoOp() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "compensate-noop-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            orchestrator.completeStep(instance.instanceId(), "s1", Map.of(), null);

            WorkflowStepState state =
                    orchestrator.compensateStep(instance.instanceId(), "s1", Map.of(), null);
            assertThat(state.status()).isEqualTo(StepStatus.COMPLETED);
        }

        @Test
        @DisplayName("deletehas rowinstanceof exception")
        void deleteWorkflowWithRunningInstancesThrows() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "delete-running-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            assertThatThrownBy(() -> orchestrator.deleteWorkflow(def.workflowId()))
                    .isInstanceOf(WorkflowDefinitionInUseException.class);
        }

        @Test
        @DisplayName("startnon-existent exception")
        void startNonExistentWorkflowThrows() {
            assertThatThrownBy(
                            () ->
                                    orchestrator.startWorkflow(
                                            "nonexistent", agent1.id(), Map.of(), null))
                    .isInstanceOf(WorkflowNotFoundException.class);
        }

        @Test
        @DisplayName("alreadycompleteof exception")
        void cancelCompletedWorkflowThrows() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "cancel-completed-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);
            orchestrator.completeStep(instance.instanceId(), "s1", Map.of(), null);

            assertThatThrownBy(() -> orchestrator.cancelWorkflow(instance.instanceId(), "too late"))
                    .isInstanceOf(WorkflowStepInvalidStateException.class);
        }

        @Test
        @DisplayName("not yetfailof exception")
        void compensateNonFailedWorkflowThrows() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "compensate-running-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            assertThatThrownBy(
                            () ->
                                    orchestrator.compensateWorkflow(
                                            instance.instanceId(), "not failed"))
                    .isInstanceOf(WorkflowStepInvalidStateException.class);
        }

        @Test
        @DisplayName("when not existscompleteStep exception")
        void completeNonExistentStepThrows() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "step-not-found-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            assertThatThrownBy(
                            () ->
                                    orchestrator.completeStep(
                                            instance.instanceId(), "nonexistent", Map.of(), null))
                    .isInstanceOf(WorkflowStepNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("S4-Boundary - boundary conditions")
    class Boundary {

        @Test
        @DisplayName("DAGverify: detect")
        void dagCycleDetection() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowStepDef step2 = step("s2", "Step 2");
            List<WorkflowEdgeDef> edges =
                    List.of(
                            new WorkflowEdgeDef("s1", "s2", EdgeCondition.ON_SUCCESS),
                            new WorkflowEdgeDef("s2", "s1", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "cycle-dag", agent1.id(), List.of(step1, step2), edges);
            assertThat(result).isInstanceOf(WorkflowValidationResult.Invalid.class);

            List<ValidationError> errors = ((WorkflowValidationResult.Invalid) result).errors();
            assertThat(errors).anyMatch(e -> e.type() == ValidationErrorType.CYCLE);
        }

        @Test
        @DisplayName("DAGverify: detect")
        void dagSelfLoopDetection() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            List<WorkflowEdgeDef> edges =
                    List.of(new WorkflowEdgeDef("s1", "s1", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow("self-loop", agent1.id(), List.of(step1), edges);
            assertThat(result).isInstanceOf(WorkflowValidationResult.Invalid.class);
        }

        @Test
        @DisplayName("DAGverify: duplicatestepIddetect")
        void dagDuplicateStepIdDetection() {
            WorkflowStepDef step1a = step("s1", "Step 1a");
            WorkflowStepDef step1b = step("s1", "Step 1b");

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "duplicate-id", agent1.id(), List.of(step1a, step1b), List.of());
            assertThat(result).isInstanceOf(WorkflowValidationResult.Invalid.class);
        }

        @Test
        @DisplayName("DAGverify: non-existentstepId")
        void dagInvalidEdgeRefDetection() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            List<WorkflowEdgeDef> edges =
                    List.of(new WorkflowEdgeDef("s1", "nonexistent", EdgeCondition.ON_SUCCESS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow("invalid-ref", agent1.id(), List.of(step1), edges);
            assertThat(result).isInstanceOf(WorkflowValidationResult.Invalid.class);
        }

        @Test
        @DisplayName("single")
        void singleStepWorkflow() {
            WorkflowStepDef step1 = step("s1", "Only Step");
            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "single-step", agent1.id(), List.of(step1), List.of());
            assertThat(result).isInstanceOf(WorkflowValidationResult.Valid.class);

            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();
            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            assertThat(states).hasSize(1);
            assertThat(states.get(0).status()).isEqualTo(StepStatus.READY);

            orchestrator.completeStep(instance.instanceId(), "s1", Map.of("done", true), null);
            WorkflowInstance completed = orchestrator.getWorkflowInstance(instance.instanceId());
            assertThat(completed.status()).isEqualTo(WorkflowInstanceStatus.COMPLETED);
        }

        @Test
        @DisplayName("limit: exceeds concurrentinstance")
        void quotaExceededMaxConcurrentInstances() {
            AxiomOrchestratorOptions limitedOptions =
                    new AxiomOrchestratorOptions(2, 2, 50, 10, 10, 30_000);
            DefaultWorkflowCoordinator coordinator = new DefaultWorkflowCoordinator();
            AgentOrchestrator limitedOrchestrator =
                    new DefaultAgentOrchestrator(
                            coordinator,
                            new DefaultStateEngine(metastore, new DefaultStateContract()),
                            limitedOptions);
            coordinator.setOrchestrator(limitedOrchestrator);

            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowValidationResult result =
                    limitedOrchestrator.defineWorkflow(
                            "quota-test", agent1.id(), List.of(step1), List.of());
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();

            limitedOrchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);
            limitedOrchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);

            assertThatThrownBy(
                            () ->
                                    limitedOrchestrator.startWorkflow(
                                            def.workflowId(), agent1.id(), Map.of(), null))
                    .isInstanceOf(WorkflowQuotaExceededException.class);
        }

        @Test
        @DisplayName("acquirenon-existent instance null")
        void getNonExistentInstanceReturnsNull() {
            WorkflowInstance instance = orchestrator.getWorkflowInstance("nonexistent");
            assertThat(instance).isNull();
        }

        @Test
        @DisplayName("deletenon-existent exception")
        void deleteNonExistentWorkflowThrows() {
            assertThatThrownBy(() -> orchestrator.deleteWorkflow("nonexistent"))
                    .isInstanceOf(WorkflowNotFoundException.class);
        }

        @Test
        @DisplayName(
                "ALWAYSconditional edge: regardless of upstream success or failurealways triggers")
        void alwaysConditionEdge() {
            WorkflowStepDef step1 = step("s1", "Step 1");
            WorkflowStepDef alwaysStep = step("always", "Always Run");
            List<WorkflowEdgeDef> edges =
                    List.of(new WorkflowEdgeDef("s1", "always", EdgeCondition.ALWAYS));

            WorkflowValidationResult result =
                    orchestrator.defineWorkflow(
                            "always-edge", agent1.id(), List.of(step1, alwaysStep), edges);
            WorkflowDefinition def = ((WorkflowValidationResult.Valid) result).definition();

            WorkflowInstance instance =
                    orchestrator.startWorkflow(def.workflowId(), agent1.id(), Map.of(), null);
            orchestrator.failStep(
                    instance.instanceId(),
                    "s1",
                    new StepError("FAILED", "failed", false, "execution"),
                    null);

            List<WorkflowStepState> states = orchestrator.getStepStates(instance.instanceId());
            assertThat(
                            states.stream()
                                    .filter(s -> s.stepId().equals("always"))
                                    .findFirst()
                                    .get()
                                    .status())
                    .isEqualTo(StepStatus.READY);
        }
    }
}
