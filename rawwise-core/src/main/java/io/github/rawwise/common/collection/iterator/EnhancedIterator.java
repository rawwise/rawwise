package io.github.rawwise.common.collection.iterator;

import java.util.Iterator;
import java.util.List;

public interface EnhancedIterator<T> extends Iterator<T> {
    List<T> toList();
}
