package io.github.rawwise.common.collection.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PeekableIterator<T> implements Iterator<T> {
    private final Iterator<T> iterator;
    private T nextElement;
    private boolean peeked;

    public PeekableIterator(Iterator<T> iterator) {
        this.iterator = iterator;
        advance();
    }

    @Override
    public boolean hasNext() {
        return peeked;
    }

    @Override
    public T next() {
        if (!peeked) {
            throw new NoSuchElementException();
        }
        T result = nextElement;
        advance();
        return result;
    }

    public T peek() {
        if (!peeked) {
            throw new NoSuchElementException();
        }
        return nextElement;
    }

    private void advance() {
        peeked = iterator.hasNext();
        if (peeked) {
            nextElement = iterator.next();
        } else {
            nextElement = null;
        }
    }
}
