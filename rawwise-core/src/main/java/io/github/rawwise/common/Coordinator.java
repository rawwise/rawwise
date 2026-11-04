package io.github.rawwise.common;

import java.io.Closeable;

public interface Coordinator extends Closeable {

    void watch(String nodePath);

    void newNode(String nodePath, byte[] value);

    void deleteNode(String nodePath);

    Long uniqId(String uniqTable);

    boolean exist(String nodePath);

    void update(String nodePath, byte[] value);
}
