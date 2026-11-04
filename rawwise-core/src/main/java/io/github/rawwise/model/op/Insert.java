package io.github.rawwise.model.op;

public interface Insert {
    Result exec();

    record Result(boolean success, long affectedRows, String message) {}
}
