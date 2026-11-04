package io.github.rawwise.model.op;

import java.io.IOException;

@FunctionalInterface
public interface ReadExecutor {
    Read.Result execute(Read read) throws IOException;
}
