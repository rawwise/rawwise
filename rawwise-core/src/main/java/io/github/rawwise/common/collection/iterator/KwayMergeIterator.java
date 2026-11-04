package io.github.rawwise.common.collection.iterator;

import java.util.*;

public class KwayMergeIterator<T> implements Iterator<T> {
    private final PriorityQueue<PeekableIterator<T>> heap;

    public KwayMergeIterator(List<Iterator<T>> iterators, Comparator<T> comparator) {
        this.heap =
                new PriorityQueue<>(
                        iterators.size(), Comparator.comparing(PeekableIterator::peek, comparator));
        for (Iterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                heap.add(new PeekableIterator<>(iterator));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        PeekableIterator<T> current = heap.poll();
        T value = current.next();

        if (current.hasNext()) {
            heap.add(current);
        }

        return value;
    }
}
