package io.github.rawwise.model.op;

public interface Op<R extends OpResult> {
    R exec();

    default R execute() {
        return exec();
    }

    OpType type();
}
