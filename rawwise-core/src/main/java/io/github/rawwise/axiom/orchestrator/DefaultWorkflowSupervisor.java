package io.github.rawwise.axiom.orchestrator;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DefaultWorkflowSupervisor implements WorkflowSupervisor {

    private final AgentOrchestrator orchestrator;
    private final WorkflowCoordinator coordinator;
    private final AxiomOrchestratorOptions options;
    private volatile boolean running = false;
    private Thread supervisorThread;

    @Override
    public void start() {
        running = true;
        supervisorThread = new Thread(this::superviseLoop, "workflow-supervisor");
        supervisorThread.setDaemon(true);
        supervisorThread.start();

        performCrashRecovery();

        log.info("Workflow supervisor started");
    }

    @Override
    public void stop() {
        running = false;
        if (supervisorThread != null) {
            supervisorThread.interrupt();
            try {
                supervisorThread.join(options.timeoutCheckIntervalMs() * 2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("Workflow supervisor stopped");
    }

    private void superviseLoop() {
        while (running) {
            try {
                Thread.sleep(options.timeoutCheckIntervalMs());
                scanAndRecover();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error in supervisor scan cycle", e);
            }
        }
    }

    private void performCrashRecovery() {
        try {
            List<WorkflowInstance> instances = orchestrator.listInstances("", 0, Integer.MAX_VALUE);
            for (WorkflowInstance instance : instances) {
                if (instance.status() == WorkflowInstanceStatus.RUNNING) {
                    log.info("Crash recovery: re-evaluating instance={}", instance.instanceId());

                    List<WorkflowStepState> states =
                            orchestrator.getStepStates(instance.instanceId());
                    for (WorkflowStepState step : states) {
                        switch (step.status()) {
                            case RUNNING, READY, WAITING -> {
                                log.info(
                                        "Crash recovery: re-evaluating downstream for instance={}, step={}",
                                        instance.instanceId(),
                                        step.stepId());
                                coordinator.evaluateDownstream(
                                        instance.instanceId(), step.stepId());
                            }
                            case RETRYING -> {
                                if (step.nextRetryAt() != null
                                        && step.nextRetryAt() <= System.currentTimeMillis()) {
                                    orchestrator.triggerStepRetry(
                                            instance.instanceId(), step.stepId());
                                }
                            }
                            default -> {}
                        }
                    }

                    if (instance.timeoutMs() > 0
                            && instance.startedAt() + instance.timeoutMs()
                                    < System.currentTimeMillis()) {
                        orchestrator.timeoutWorkflow(instance.instanceId());
                    }
                }
            }
            log.info("Crash recovery scan completed");
        } catch (Exception e) {
            log.error("Error during crash recovery", e);
        }
    }

    private void scanAndRecover() {
        List<WorkflowInstance> instances = orchestrator.listInstances("", 0, Integer.MAX_VALUE);
        long now = System.currentTimeMillis();

        for (WorkflowInstance instance : instances) {
            if (instance.status() != WorkflowInstanceStatus.RUNNING) {
                continue;
            }

            checkInstanceTimeout(instance, now);
            checkStepTimeoutsAndRetries(instance, now);
        }
    }

    private void checkInstanceTimeout(WorkflowInstance instance, long now) {
        if (instance.timeoutMs() <= 0) {
            return;
        }
        if (instance.startedAt() + instance.timeoutMs() < now) {
            log.warn(
                    "Workflow instance timed out: instanceId={}, startedAt={}, timeoutMs={}",
                    instance.instanceId(),
                    instance.startedAt(),
                    instance.timeoutMs());
            try {
                orchestrator.timeoutWorkflow(instance.instanceId());
            } catch (Exception e) {
                log.error("Failed to timeout instance: instanceId={}", instance.instanceId(), e);
            }
        }
    }

    private void checkStepTimeoutsAndRetries(WorkflowInstance instance, long now) {
        WorkflowInstance currentInstance = orchestrator.getWorkflowInstance(instance.instanceId());
        if (currentInstance == null || currentInstance.status() != WorkflowInstanceStatus.RUNNING) {
            return;
        }

        WorkflowDefinition def = orchestrator.getWorkflow(instance.workflowId());
        List<WorkflowStepState> steps = orchestrator.getStepStates(instance.instanceId());

        for (WorkflowStepState step : steps) {
            if (step.status() == StepStatus.RUNNING && step.startedAt() > 0) {
                if (def != null) {
                    WorkflowStepDef stepDef =
                            def.steps().stream()
                                    .filter(s -> s.stepId().equals(step.stepId()))
                                    .findFirst()
                                    .orElse(null);
                    if (stepDef != null
                            && stepDef.timeoutMs() > 0
                            && step.startedAt() + stepDef.timeoutMs() < now) {
                        log.warn(
                                "Step timed out: instanceId={}, stepId={}, timeoutMs={}",
                                instance.instanceId(),
                                step.stepId(),
                                stepDef.timeoutMs());
                        try {
                            orchestrator.failStep(
                                    instance.instanceId(),
                                    step.stepId(),
                                    new StepError(
                                            "TIMEOUT",
                                            "Step timed out after " + stepDef.timeoutMs() + "ms",
                                            true,
                                            "TIMEOUT"),
                                    null);
                        } catch (Exception e) {
                            log.error(
                                    "Failed to fail timed-out step: instanceId={}, stepId={}",
                                    instance.instanceId(),
                                    step.stepId(),
                                    e);
                        }
                    }
                }
            }

            if (step.status() == StepStatus.RETRYING) {
                if (step.nextRetryAt() != null && step.nextRetryAt() <= now) {
                    log.info(
                            "Triggering step retry: instanceId={}, stepId={}",
                            instance.instanceId(),
                            step.stepId());
                    try {
                        orchestrator.triggerStepRetry(instance.instanceId(), step.stepId());
                    } catch (Exception e) {
                        log.error(
                                "Failed to trigger step retry: instanceId={}, stepId={}",
                                instance.instanceId(),
                                step.stepId(),
                                e);
                    }
                }
            }
        }
    }
}
