package io.github.rawwise.benchmark.ycsb;

import java.io.IOException;

public interface YcsbClient {
    void init(int recordCount) throws IOException;

    default void initRange(int keyStart, int keyEnd) throws IOException {
        int recordCount = keyEnd - keyStart;
        init(recordCount);
    }

    String read(int key) throws IOException;

    void write(int key) throws IOException;

    void insert(int key) throws IOException;

    void delete(int key) throws IOException;

    int scan(int startKey) throws IOException;

    void close() throws Exception;
}
