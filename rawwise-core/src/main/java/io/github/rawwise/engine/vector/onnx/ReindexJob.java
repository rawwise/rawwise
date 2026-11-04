package io.github.rawwise.engine.vector.onnx;

public record ReindexJob(
        String jobId,
        String srcTableId,
        String destTableId,
        int srcDimension,
        int destDimension,
        Status status,
        long startedAtMs,
        long completedAtMs,
        long processedRows,
        long totalRows,
        String errorMessage) {

    public enum Status {
        PENDING,
        RUNNING,
        COMPLETED,
        FAILED
    }

    public ReindexJob(
            String jobId,
            String srcTableId,
            String destTableId,
            int srcDimension,
            int destDimension,
            Status status,
            long startedAtMs,
            long completedAtMs) {
        this(
                jobId,
                srcTableId,
                destTableId,
                srcDimension,
                destDimension,
                status,
                startedAtMs,
                completedAtMs,
                0L,
                0L,
                null);
    }

    public ReindexJob withStatus(Status newStatus, long completedAtMs) {
        return new ReindexJob(
                jobId,
                srcTableId,
                destTableId,
                srcDimension,
                destDimension,
                newStatus,
                startedAtMs,
                completedAtMs,
                processedRows,
                totalRows,
                errorMessage);
    }

    public ReindexJob withProgress(long processedRows, long totalRows) {
        return new ReindexJob(
                jobId,
                srcTableId,
                destTableId,
                srcDimension,
                destDimension,
                status,
                startedAtMs,
                completedAtMs,
                processedRows,
                totalRows,
                errorMessage);
    }

    public ReindexJob withError(String errorMessage) {
        return new ReindexJob(
                jobId,
                srcTableId,
                destTableId,
                srcDimension,
                destDimension,
                status,
                startedAtMs,
                completedAtMs,
                processedRows,
                totalRows,
                errorMessage);
    }

    public record Result(String jobId, boolean success, long processedRows, String errorMessage) {}
}
