package io.github.rawwise.engine.storage.writer;

import java.io.IOException;

public interface RollingFileWriter<T> extends FileWriter<T> {
    RollingPolicy rollingPolicy();

    void roll() throws IOException;
}
