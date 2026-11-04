package io.github.rawwise.axiom.memory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

class AxiomMemoryFacadeConsolidateTest {

    @Test
    void consolidateIsNoOpAndNeverReadsOrWritesStore() throws Exception {
        AxiomClient client = mock(AxiomClient.class);
        ViewMetadataStore vms = mock(ViewMetadataStore.class);
        AxiomMemoryFacade facade = new AxiomMemoryFacade(client, vms, DecayPolicy.DEFAULT);

        facade.consolidate("alice", MemoryTier.WORKING, MemoryTier.SEMANTIC);

        verify(vms, never()).scanByTable(anyString(), anyInt());
        verify(client, never()).getEntry(anyString(), anyString());
        verify(vms, never()).update(anyString(), anyString(), any(ViewMetadata.class));
    }

    @Test
    void consolidateWithBareFacadeIsNoOp() throws Exception {
        AxiomClient client = mock(AxiomClient.class);
        AxiomMemoryFacade facade = new AxiomMemoryFacade(client);

        facade.consolidate("alice", MemoryTier.WORKING, MemoryTier.SEMANTIC);

        verify(client, never()).getEntry(anyString(), anyString());
    }
}
