package io.github.rawwise.execution;

public record StepLog(long timestamp, String level, String message) {

    public StepLog {
        if (level == null || level.isEmpty()) {
            level = "INFO";
        }
        if (message == null) {
            message = "";
        }
    }

    public static StepLog info(String message) {
        return new StepLog(System.currentTimeMillis(), "INFO", message);
    }

    public static StepLog warn(String message) {
        return new StepLog(System.currentTimeMillis(), "WARN", message);
    }

    public static StepLog error(String message) {
        return new StepLog(System.currentTimeMillis(), "ERROR", message);
    }
}
