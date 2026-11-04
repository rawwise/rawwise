package io.github.rawwise.engine.storage.merge;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import java.io.IOException;
import java.util.NoSuchElementException;

public class PeekingIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> inner;
    private T peeked;
    private boolean hasPeeked;

    public PeekingIterator(CloseableIterator<T> inner) {
        this.inner = inner;
    }

    public T peek() {
        if (!hasPeeked) {
            if (!inner.hasNext()) {
                throw new NoSuchElementException("PeekingIterator is empty");
            }
            peeked = inner.next();
            hasPeeked = true;
        }
        return peeked;
    }

    @Override
    public boolean hasNext() {
        return hasPeeked || inner.hasNext();
    }

    @Override
    public T next() {
        if (hasPeeked) {
            T r = peeked;
            peeked = null;
            hasPeeked = false;
            return r;
        }
        return inner.next();
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }
}
