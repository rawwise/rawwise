package io.github.rawwise.axiom.orchestrator;

import java.util.List;

public interface CoordResourceManager {
    List<String> allocateCoordResources(String instanceId, WorkflowDefinition definition);

    void releaseCoordResources(String instanceId);
}
