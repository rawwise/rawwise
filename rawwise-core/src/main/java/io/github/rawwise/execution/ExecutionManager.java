package io.github.rawwise.execution;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface ExecutionManager {

    ExecutionPlan createPlan(
            String agentId,
            String userId,
            String sessionId,
            String description,
            List<ExecutionStep> steps,
            RetryPolicy defaultRetryPolicy)
            throws IOException;

    Optional<ExecutionPlan> getPlan(String planId) throws IOException;

    List<ExecutionPlan> listPlansByAgent(String agentId) throws IOException;

    List<ExecutionPlan> listPlansBySession(String sessionId) throws IOException;

    boolean cancelPlan(String planId) throws IOException;

    ExecutionStep startStep(String planId, int stepIndex) throws IOException;

    ExecutionStep completeStep(String planId, int stepIndex, StepResult result) throws IOException;

    ExecutionStep failStep(String planId, int stepIndex, String error) throws IOException;

    ExecutionStep skipStep(String planId, int stepIndex, String reason) throws IOException;

    ExecutionStep retryStep(String planId, int stepIndex) throws IOException;

    ExecutionPlan compensatePlan(String planId, int fromStepIndex) throws IOException;

    ExecutionStep compensateStep(String planId, int stepIndex) throws IOException;

    ExecutionPlan resumePlan(String planId, int fromStepIndex) throws IOException;

    void appendStepLog(String planId, int stepIndex, String level, String message)
            throws IOException;

    List<StepLog> getStepLogs(String planId, int stepIndex) throws IOException;
}
