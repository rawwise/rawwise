package io.github.rawwise.model.op;

import java.util.Optional;

public interface Write extends Op<Write.Result> {

    long writeSize();

    @Override
    Result exec();

    final class Result implements OpResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long count;
        private final long timestamp;

        public Result(boolean success, String message, Throwable error, long count) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.count = count;
            this.timestamp = System.currentTimeMillis();
        }

        @Override
        public boolean success() {
            return success;
        }

        @Override
        public Optional<String> message() {
            return Optional.ofNullable(message);
        }

        @Override
        public Optional<Throwable> error() {
            return Optional.ofNullable(error);
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        public long count() {
            return count;
        }

        public static Result ok(long count) {
            return new Result(true, "success", null, count);
        }

        public static Result ok(String message, long count) {
            return new Result(true, message, null, count);
        }

        public static Result error(String msg) {
            return new Result(false, msg, null, 0);
        }

        public static Result error(String msg, Throwable throwable) {
            return new Result(false, msg, throwable, 0);
        }
    }
}
