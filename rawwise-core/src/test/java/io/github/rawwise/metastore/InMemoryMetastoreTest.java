package io.github.rawwise.metastore;

import java.io.IOException;

public class InMemoryMetastoreTest extends MetastoreContractTest {

    @Override
    protected Metastore createStore() throws IOException {
        return new InMemoryMetastore();
    }
}
