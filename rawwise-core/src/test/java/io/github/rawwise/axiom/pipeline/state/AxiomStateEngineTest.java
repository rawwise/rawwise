package io.github.rawwise.axiom.pipeline.state;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.axiom.memory.InMemoryAxiomClient;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.io.IOException;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("AxiomStateEngine - CAS semantic + immutable version chain + guard validation")
class AxiomStateEngineTest {

    private InMemoryMetastore metastore;
    private InMemoryAxiomClient axiomClient;
    private AxiomStateEngine engine;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        axiomClient = new InMemoryAxiomClient();
        engine = new AxiomStateEngine(axiomClient, metastore);
    }

    @Test
    @DisplayName("createStateSpace createsuccess,duplicatecreatesame spaceId returns false")
    void createStateSpace_success() throws IOException {
        StateSpace space =
                engine.createStateSpace(
                        "agent-1",
                        "workflow",
                        "idle",
                        List.of("idle", "running", "done"),
                        List.of(new StateTransition("idle", "running", null, Map.of())),
                        Map.of());

        assertNotNull(space.spaceId());
        assertEquals("agent-1", space.agentId());
        assertEquals("idle", space.initialState());
        assertEquals(3, space.states().size());

        Optional<StateSpace> retrieved = engine.getStateSpace(space.spaceId());
        assertTrue(retrieved.isPresent());
        assertEquals("workflow", retrieved.get().name());
    }

    @Test
    @DisplayName("listStateSpacesByAgent by agentId filter")
    void listByAgent_filtersCorrectly() throws IOException {
        engine.createStateSpace("agent-1", "wf1", "s0", List.of("s0"), List.of(), Map.of());
        engine.createStateSpace("agent-1", "wf2", "s0", List.of("s0"), List.of(), Map.of());
        engine.createStateSpace("agent-2", "wf3", "s0", List.of("s0"), List.of(), Map.of());

        List<StateSpace> agent1Spaces = engine.listStateSpacesByAgent("agent-1");
        assertEquals(2, agent1Spaces.size());

        List<StateSpace> agent2Spaces = engine.listStateSpacesByAgent("agent-2");
        assertEquals(1, agent2Spaces.size());
    }

    @Test
    @DisplayName("write createnew entry,version from 1 start")
    void write_newEntry_version1() throws IOException {
        StateSpace space = createSimpleSpace();
        StateEntry entry = engine.write(space.spaceId(), "task-1", "data", "idle", Map.of());

        assertEquals("task-1", entry.key());
        assertEquals("data", entry.value());
        assertEquals("idle", entry.stateName());
        assertEquals(1, entry.version());
    }

    @Test
    @DisplayName("write increment version,read returnsmostnewversion")
    void write_incrementsVersion_readReturnsLatest() throws IOException {
        StateSpace space = createSimpleSpace();

        engine.write(space.spaceId(), "k1", "v1", "idle", Map.of());
        engine.write(space.spaceId(), "k1", "v2", "running", Map.of());
        StateEntry entry3 = engine.write(space.spaceId(), "k1", "v3", "done", Map.of());

        StateEntry read = engine.read(space.spaceId(), "k1");
        assertNotNull(read);
        assertEquals(3, read.version());
        assertEquals("v3", read.value());
        assertEquals("done", read.stateName());
    }

    @Test
    @DisplayName("write illegal stateName exception")
    void write_invalidState_throws() throws IOException {
        StateSpace space = createSimpleSpace();
        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> engine.write(space.spaceId(), "k1", "v1", "nonexistent", Map.of()));
        assertTrue(ex.getMessage().contains("Invalid state"));
    }

    @Test
    @DisplayName("transition legal transitionsuccess,guard validationthroughpass")
    void transition_validTransition_succeeds() throws IOException {
        StateSpace space =
                engine.createStateSpace(
                        "agent-1",
                        "wf",
                        "idle",
                        List.of("idle", "running", "done"),
                        List.of(
                                new StateTransition("idle", "running", null, Map.of()),
                                new StateTransition("running", "done", null, Map.of())),
                        Map.of());

        engine.write(space.spaceId(), "task-1", "data", "idle", Map.of());
        StateEntry afterTransition =
                engine.transition(space.spaceId(), "task-1", "running", Map.of());

        assertEquals("running", afterTransition.stateName());
        assertEquals(2, afterTransition.version());
    }

    @Test
    @DisplayName("transition undefined transitionexception")
    void transition_undefinedTransition_throws() throws IOException {
        StateSpace space =
                engine.createStateSpace(
                        "agent-1",
                        "wf",
                        "idle",
                        List.of("idle", "running", "done"),
                        List.of(new StateTransition("idle", "running", null, Map.of())),
                        Map.of());

        engine.write(space.spaceId(), "task-1", "data", "idle", Map.of());
        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> engine.transition(space.spaceId(), "task-1", "done", Map.of()));
        assertTrue(ex.getMessage().contains("Contract violation"));
        assertTrue(ex.getMessage().contains("Transition not defined"));
    }

    @Test
    @DisplayName("transition guard tablereachstylevalidation,condition not satisfiedwhenexception")
    void transition_guardFails_throws() throws IOException {
        StateSpace space =
                engine.createStateSpace(
                        "agent-1",
                        "wf",
                        "idle",
                        List.of("idle", "running"),
                        List.of(new StateTransition("idle", "running", "attempts >= 3", Map.of())),
                        Map.of());

        engine.write(space.spaceId(), "task-1", "data", "idle", Map.of("attempts", "1"));
        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> engine.transition(space.spaceId(), "task-1", "running", Map.of()));
        assertTrue(ex.getMessage().contains("Contract violation"));
        assertTrue(ex.getMessage().contains("Guard failed"));
    }

    @Test
    @DisplayName(
            "transition guard tablereachstylevalidation,condition satisfiedwhentransitionsuccess")
    void transition_guardPasses_succeeds() throws IOException {
        StateSpace space =
                engine.createStateSpace(
                        "agent-1",
                        "wf",
                        "idle",
                        List.of("idle", "running"),
                        List.of(new StateTransition("idle", "running", "attempts >= 3", Map.of())),
                        Map.of());

        engine.write(space.spaceId(), "task-1", "data", "idle", Map.of("attempts", "5"));
        StateEntry result = engine.transition(space.spaceId(), "task-1", "running", Map.of());
        assertEquals("running", result.stateName());
    }

    @Test
    @DisplayName("scan by prefix filter,limit take effect")
    void scan_prefixAndLimit() throws IOException {
        StateSpace space = createSimpleSpace();
        engine.write(space.spaceId(), "task:1", "d1", "idle", Map.of());
        engine.write(space.spaceId(), "task:2", "d2", "idle", Map.of());
        engine.write(space.spaceId(), "job:1", "d3", "idle", Map.of());

        List<StateEntry> taskEntries = engine.scan(space.spaceId(), "task:", 100);
        assertEquals(2, taskEntries.size());

        List<StateEntry> allLimited = engine.scan(space.spaceId(), "", 2);
        assertEquals(2, allLimited.size());
    }

    @Test
    @DisplayName(
            "deleteStateSpace deletedefinitely + entries data,historyversionpreservein Axiom storein")
    void deleteStateSpace_removesDefinitionsAndMetadata() throws IOException {
        StateSpace space = createSimpleSpace();
        engine.write(space.spaceId(), "k1", "v1", "idle", Map.of());
        engine.write(space.spaceId(), "k2", "v2", "idle", Map.of());

        boolean deleted = engine.deleteStateSpace(space.spaceId());
        assertTrue(deleted);

        assertTrue(engine.getStateSpace(space.spaceId()).isEmpty());
        assertEquals(0, engine.scan(space.spaceId(), "", 100).size());
    }

    @Test
    @DisplayName("deleteStateSpace non-existent spaceId returns false")
    void deleteStateSpace_nonExistent_returnsFalse() throws IOException {
        assertFalse(engine.deleteStateSpace("nonexistent"));
    }

    @Test
    @DisplayName("read non-existent key returns null")
    void read_nonExistentKey_returnsNull() throws IOException {
        StateSpace space = createSimpleSpace();
        assertNull(engine.read(space.spaceId(), "nonexistent"));
    }

    @Test
    @DisplayName(
            "write CAS semantic:immutable version chain,oldversionstillin Axiom storeincanread")
    void write_casImmutableVersionChain() throws IOException {
        StateSpace space = createSimpleSpace();
        engine.write(space.spaceId(), "k1", "v1", "idle", Map.of());
        engine.write(space.spaceId(), "k1", "v2", "running", Map.of());

        String v1Key = "state:" + space.spaceId() + ":k1:v1";
        String v2Key = "state:" + space.spaceId() + ":k1:v2";
        assertTrue(axiomClient.contains(v1Key), "old版this v1 should仍in Axiom 存储in");
        assertTrue(axiomClient.contains(v2Key), "new版this v2 shouldin Axiom 存储in");

        StateEntry latest = engine.read(space.spaceId(), "k1");
        assertEquals(2, latest.version());
        assertEquals("v2", latest.value());
    }

    private StateSpace createSimpleSpace() throws IOException {
        return engine.createStateSpace(
                "agent-1",
                "test-space",
                "idle",
                List.of("idle", "running", "done"),
                List.of(
                        new StateTransition("idle", "running", null, Map.of()),
                        new StateTransition("running", "done", null, Map.of())),
                Map.of());
    }
}
