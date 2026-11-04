package io.github.rawwise.engine.storage.reader;

import io.github.rawwise.engine.storage.QueryCapability;
import java.util.Set;

public interface QueryReader extends Reader {
    Set<QueryCapability> capabilities();
}
