package io.github.rawwise.common.hash;

import java.util.Collection;
import java.util.Map;

public interface ConsistentHashRing<T> {

    void addNode(T node);

    void removeNode(T node);

    T getNode(Object key);

    Map<T, Collection<Object>> getNodes(Collection<?> keys);

    int getNodeCount();

    int getVirtualNodeCount();

    Collection<T> getAllNodes();

    void rebalance();

    void clear();
}
