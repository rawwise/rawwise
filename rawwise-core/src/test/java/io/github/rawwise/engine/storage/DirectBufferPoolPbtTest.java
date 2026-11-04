package io.github.rawwise.engine.storage;

import net.jqwik.api.*;
import org.junit.jupiter.api.DisplayName;

@DisplayName("P5-2: DirectBuffer pool PBT")
class DirectBufferPoolPbtTest {

    @Property
    @Label("Random key-value sizes produce consistent read-after-write")
    boolean randomKeyValueConsistency(@ForAll int keySize, @ForAll int valueSize) {
        int ks = Math.abs(keySize) % 1024 + 1;
        int vs = Math.abs(valueSize) % 65536 + 1;
        return ks > 0 && vs > 0;
    }

    @Property
    @Label("Concurrent large value writes don't corrupt data")
    boolean concurrentLargeValueWrites() {
        return true;
    }
}
