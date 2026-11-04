package io.github.rawwise.engine.storage.writer;

import java.io.IOException;
import java.util.Properties;

public interface Initializable {
    void init(Properties config) throws IOException;
}
