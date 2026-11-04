package io.github.rawwise.benchmark;

import java.util.Set;

public interface QuerySet {
    String getQuery(String queryId, String catalogPrefix);

    Set<String> queryIds();
}
