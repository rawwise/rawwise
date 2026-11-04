package io.github.rawwise.engine.clean;

import io.github.rawwise.engine.storage.manifest.SnapshotOperations;

public interface CleanSafetyPolicy {

    enum Verdict {
        ALLOW,
        BLOCK,
        REPLAN_REQUIRED
    }

    record SafetyCheck(String reason, Verdict verdict) {
        public static SafetyCheck allow() {
            return new SafetyCheck("OK", Verdict.ALLOW);
        }

        public static SafetyCheck block(String reason) {
            return new SafetyCheck(reason, Verdict.BLOCK);
        }

        public static SafetyCheck replan(String reason) {
            return new SafetyCheck(reason, Verdict.REPLAN_REQUIRED);
        }
    }

    SafetyCheck evaluate(CleanPlan plan, SnapshotOperations snapshotOps);
}
