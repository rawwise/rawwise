package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.frontend.config.FrontendOptions;
import io.javalin.Javalin;

public record AxiomRouteGroup(
        MemoryRoutes memoryRoutes,
        StateRoutes stateRoutes,
        TrustRoutes trustRoutes,
        GraphRoutes graphRoutes,
        CoordRoutes coordRoutes,
        WorkflowRoutes workflowRoutes,
        AxiomKnowledgeRoutes knowledgeRoutes) {
    public void register(Javalin app, FrontendOptions options) {
        if (!options.isAxiomEnabled()) {
            return;
        }
        if (memoryRoutes != null) memoryRoutes.register(app);
        if (stateRoutes != null) stateRoutes.register(app);
        if (trustRoutes != null) trustRoutes.register(app);
        if (options.isAxiomGraphEnabled() && graphRoutes != null) graphRoutes.register(app);
        if (options.isAxiomCoordEnabled() && coordRoutes != null) coordRoutes.register(app);
        if (options.isAxiomOrchestratorEnabled() && workflowRoutes != null)
            workflowRoutes.register(app);
        if (knowledgeRoutes != null) knowledgeRoutes.register(app);
    }
}
