package io.github.rawwise.backend.shared;

import io.github.rawwise.axiom.memory.SummarizationService;
import io.github.rawwise.axiom.pipeline.graph.GraphQuery;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.Metastore;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Cross (Spark Executor / Backend Server) same-JVM shared registry.
 *
 * <p>Design motivation: In dogfooding/local-cluster deployment, Backend Server and Spark Executor
 * run in the same JVM but communicate via gRPC; direct object sharing requires an intermediary.
 * This class provides a minimal static holder.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>Spark Executor (RawWiseExecutorRoutingPlugin) creates GraphQuery then calls {@link
 *       #setGraphQuery}
 *   <li>Backend Server (Main.setupAxiomStubs) calls {@link #getGraphQuery} to get the reference and
 *       inject into AxiomServiceImpl
 *   <li>Backend Server (Main.setupAxiomStubs) creates SummarizationService then calls {@link
 *       #setSummarizationService}
 *   <li>Backend Node (DynamicMmapStore) calls {@link #getSummarizationService} to get the reference
 *       and inject into AutoDerivationHook
 * </ul>
 *
 * <p>In multi-JVM deployment (cross-process executor), this holder is independent per JVM, no
 * GraphQuery available, AxiomServiceImpl degrades to vector+keyword dual-path RRF (backward
 * compatible).
 *
 * <p>Note: This is a temporary bridge for the dogfooding phase; production deployment requires
 * replacing with an RPC-based GraphQuery service.
 */
@Slf4j
public final class BackendSharedRegistry {

    @Getter private static volatile GraphQuery graphQuery;
    @Getter private static volatile SummarizationService summarizationService;
    @Getter private static volatile KnowledgeStore knowledgeStore;
    @Getter private static volatile Metastore metastore;

    private BackendSharedRegistry() {}

    public static void setGraphQuery(GraphQuery gq) {
        graphQuery = gq;
        log.info(
                "BackendSharedRegistry: GraphQuery registered (impl={})",
                gq == null ? "null" : gq.getClass().getSimpleName());
    }

    public static void setSummarizationService(SummarizationService svc) {
        summarizationService = svc;
        log.info(
                "BackendSharedRegistry: SummarizationService registered (impl={})",
                svc == null ? "null" : svc.getClass().getSimpleName());
    }

    public static void setKnowledgeStore(KnowledgeStore ks) {
        knowledgeStore = ks;
        log.info(
                "BackendSharedRegistry: KnowledgeStore registered (impl={})",
                ks == null ? "null" : ks.getClass().getSimpleName());
    }

    public static void setMetastore(Metastore ms) {
        metastore = ms;
        log.info(
                "BackendSharedRegistry: Metastore registered (impl={})",
                ms == null ? "null" : ms.getClass().getSimpleName());
    }

    public static void reset() {
        graphQuery = null;
        summarizationService = null;
        knowledgeStore = null;
        metastore = null;
    }
}
