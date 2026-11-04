package io.github.rawwise.axiom.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class ViewMetadataSlimTest {
    @Test
    void metadataDefaultsToSlimWithoutSummaries() {
        ViewMetadata m = new ViewMetadata("k1", "agent_memory");
        assertEquals(1.0f, m.decayFactor());
        assertEquals(0, m.accessCount());
        assertFalse(m.toString().contains("l0Abstract"));
        assertFalse(m.toString().contains("l1Overview"));
    }
}
