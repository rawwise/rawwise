package io.github.rawwise.backend.server.service;

import com.google.protobuf.ByteString;
import io.github.rawwise.axiom.pipeline.feedback.retrieval.FeedbackRecord;
import io.github.rawwise.axiom.pipeline.feedback.retrieval.FeedbackType;
import io.github.rawwise.axiom.pipeline.feedback.retrieval.RerankerAbSwitcher;
import io.github.rawwise.axiom.pipeline.feedback.retrieval.RerankerModelMetadata;
import io.github.rawwise.axiom.pipeline.feedback.retrieval.RetrievalFeedbackCollector;
import io.github.rawwise.axiom.pipeline.graph.distributed.HybridSearchRequest;
import io.github.rawwise.axiom.pipeline.graph.distributed.HybridSearchResponse;
import io.github.rawwise.engine.vector.DistributedSearchCoordinator;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.IvfRouter;
import io.github.rawwise.engine.vector.RemoteShardIndex;
import io.github.rawwise.engine.vector.SearchCandidate;
import io.github.rawwise.engine.vector.ShardCentroids;
import io.github.rawwise.engine.vector.ShardClient;
import io.github.rawwise.engine.vector.ShardInternalIndex;
import io.github.rawwise.engine.vector.VectorSearchResult;
import io.github.rawwise.engine.vector.onnx.DefaultRerankerModel;
import io.github.rawwise.model.record.DefaultFeedbackAnnotation;
import io.github.rawwise.model.record.FeedbackAnnotation;
import io.github.rawwise.proto.axiom.*;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.FeedbackRequest;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AxiomServiceImpl extends AxiomServiceGrpc.AxiomServiceImplBase {

    private static final long DEFAULT_SEARCH_TIMEOUT_MS = 5000;

    private volatile List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub>
            executorStubs = List.of();
    private volatile EmbeddingProvider embeddingProvider;
    private volatile DistributedSearchCoordinator searchCoordinator;
    private volatile Map<String, ShardClient> shardClientsMap = Map.of();
    private volatile IvfRouter ivfRouter = new IvfRouter();
    private volatile io.github.rawwise.metastore.Metastore metastoreRef;
    private final Map<String, Double> rankBiases = new ConcurrentHashMap<>();
    private static final java.nio.file.Path RANK_BIAS_PATH =
            java.nio.file.Path.of(
                    System.getProperty("user.home"), ".rawwise", "rank-bias.properties");
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private volatile ScheduledExecutorService centroidRefreshScheduler;
    private volatile RetrievalFeedbackCollector feedbackCollector;
    private volatile RerankerAbSwitcher rerankerSwitcher;
    private volatile DefaultRerankerModel rerankerModel;
    private volatile io.github.rawwise.axiom.pipeline.graph.distributed.HybridGraphSearchService
            hybridSearchService;

    /**
     * graph recall engine, optionally injected. if injectedthen search() willexecute graph recall
     * third path, with vector + keyword combined with RRF fusion. if not injectedthen maintain
     * original behavior(vector + keyword dual-path).
     *
     * <p>config options(via -Drawwise.axiom.graph.* system properties):
     * rawwise.axiom.graph.rescue.enabled (default: false) — graph rescue master switch
     * rawwise.axiom.graph.boost (default: 0.1) — rescue boost strength
     *
     * <p>stub LLM under graph traverse returnlarge amount ofirrelevant dia, rescue isnegative gain,
     * disabled by default. switching to real LLM can be set to rescue.enabled=true enable.
     */
    private volatile io.github.rawwise.axiom.pipeline.graph.GraphQuery graphQueryEngine;

    private volatile io.github.rawwise.axiom.pipeline.graph.distributed.QueryReformulationStage
            queryReformulationStage;
    private static final boolean GRAPH_RESCUE_ENABLED =
            Boolean.parseBoolean(System.getProperty("rawwise.axiom.graph.rescue.enabled", "false"));
    private static final double GRAPH_BOOST =
            Double.parseDouble(System.getProperty("rawwise.axiom.graph.boost", "0.1"));

    /**
     * Multi-Query Reformulation. fororiginal query generate N variant(remove question words +
     * keyword extraction), eachvariantparalleldo vec+keyword search, resultusing RRF fusion.
     * multiplevariantallrecall doc scoreaccumulate → rank boost.
     *
     * <p>config options: rawwise.axiom.multiquery.enabled (default: true) — multi-query master
     * switch rawwise.axiom.multiquery.expand.n (default: 3) — variant count(including original
     * query)
     */
    private static final boolean MULTIQUERY_ENABLED =
            Boolean.parseBoolean(System.getProperty("rawwise.axiom.multiquery.enabled", "true"));

    private static final int MULTIQUERY_EXPAND_N =
            Integer.parseInt(System.getProperty("rawwise.axiom.multiquery.expand.n", "3"));

    /**
     * RRF (Reciprocal Rank Fusion) parameters. rawwise.axiom.rrf.k (default: 60) — RRF k constant,
     * smaller top rank moreprominent rawwise.axiom.search.topk-multiplier (default: 2) —
     * vec/keyword recall count = topK * multiplier
     */
    private static final int RRF_K =
            Integer.parseInt(System.getProperty("rawwise.axiom.rrf.k", "60"));

    private static final int SEARCH_TOPK_MULTIPLIER =
            Integer.parseInt(System.getProperty("rawwise.axiom.search.topk-multiplier", "2"));

    /** 批量点查预取上限：filter 前置过滤需覆盖更多候选，但避免超大候选集时点查失控。 */
    private static final int PROPS_PREFETCH_MAX =
            Integer.parseInt(System.getProperty("rawwise.axiom.search.props-prefetch-max", "200"));

    /** 批量点查整体超时（ms）：尽力而为，超时返回已预取子集。 */
    private static final int PROPS_FETCH_BATCH_TIMEOUT_MS =
            Integer.parseInt(
                    System.getProperty(
                            "rawwise.axiom.search.props-fetch-batch-timeout-ms", "3000"));

    private static final boolean FEATURE_RERANK_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.feature-rerank.enabled", "true"));
    private static final double FEATURE_VEC_WEIGHT =
            Double.parseDouble(
                    System.getProperty("rawwise.axiom.feature-rerank.vec-weight", "0.30"));
    private static final double FEATURE_KW_WEIGHT =
            Double.parseDouble(
                    System.getProperty("rawwise.axiom.feature-rerank.kw-weight", "0.30"));
    private static final double FEATURE_GRAPH_WEIGHT =
            Double.parseDouble(
                    System.getProperty("rawwise.axiom.feature-rerank.graph-weight", "0.15"));
    private static final double FEATURE_MULTIQUERY_WEIGHT =
            Double.parseDouble(
                    System.getProperty("rawwise.axiom.feature-rerank.multiquery-weight", "0.10"));

    /**
     * Entity-Aware Retrieval (Phase 1, boost mode). query subject identification → subject
     * entityparticipate dia get additive boost.
     *
     * <p>config options: rawwise.axiom.entity-aware.enabled (default: false) — subject
     * awarenessswitch rawwise.axiom.feature-rerank.subject-boost (default: 0.0) — subject boost
     * value
     *
     * <p>sig arraydimension: [vec, kw, graph, mq, subject], sig[4] does not participate min-max
     * normalization. weight allocation(sum=1.0): vec=0.35 kw=0.35 graph=0.15 mq=0.15 final score =
     * baseScore(4channelweighted) + subjectBoost * sig[4]
     */
    private static final int SIG_CHANNEL_COUNT = 5;

    private static final boolean ENTITY_AWARE_ENABLED =
            Boolean.parseBoolean(System.getProperty("rawwise.axiom.entity-aware.enabled", "false"));
    private static final double FEATURE_SUBJECT_BOOST =
            Double.parseDouble(
                    System.getProperty("rawwise.axiom.feature-rerank.subject-boost", "0.0"));

    private static final boolean LLM_REFORMULATION_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.queryreformulation.enabled", "false"));

    private static final boolean QUERY_REWRITE_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.query-rewrite.enabled", "false"));
    private static final String QUERY_REWRITE_ENDPOINT =
            System.getProperty("rawwise.axiom.query-rewrite.endpoint", "");
    private static final String QUERY_REWRITE_API_KEY =
            System.getProperty("rawwise.axiom.query-rewrite.api-key", "");
    private static final String QUERY_REWRITE_MODEL =
            System.getProperty("rawwise.axiom.query-rewrite.model", "deepseek-chat");
    private static final long QUERY_REWRITE_TIMEOUT_MS =
            Long.parseLong(System.getProperty("rawwise.axiom.query-rewrite.timeout-ms", "5000"));

    private static final boolean REMOTE_SHARD_INDEX_ENABLED =
            Boolean.parseBoolean(
                    System.getProperty("rawwise.axiom.remote-shard-index.enabled", "false"));

    private static final boolean RERANKER_ENABLED =
            Boolean.parseBoolean(System.getProperty("rawwise.axiom.reranker.enabled", "false"));
    private static final String RERANKER_ENDPOINT =
            System.getProperty("rawwise.axiom.reranker.endpoint", "");
    private static final String RERANKER_API_KEY =
            System.getProperty("rawwise.axiom.reranker.api-key", "");
    private static final String RERANKER_MODEL =
            System.getProperty("rawwise.axiom.reranker.model", "deepseek-chat");
    private static final int RERANKER_TOP_N = 30;
    private static final double CROSSENCODER_ALPHA =
            Double.parseDouble(System.getProperty("rawwise.axiom.crossencoder.alpha", "0.6"));
    private static final java.net.http.HttpClient RERANKER_HTTP_CLIENT =
            java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(5))
                    .build();
    private static final java.util.concurrent.ExecutorService RERANKER_EXECUTOR =
            java.util.concurrent.Executors.newFixedThreadPool(
                    10,
                    r -> {
                        Thread t = new Thread(r, "llm-reranker");
                        t.setDaemon(true);
                        return t;
                    });

    private static final long RERANKER_TIMEOUT_MS =
            Long.parseLong(System.getProperty("rawwise.axiom.reranker.timeout-ms", "5000"));
    private final ExecutorService rerankExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public AxiomServiceImpl() {
        loadPersistentRankBiases();
    }

    public void setExecutorStubs(
            List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs) {
        this.executorStubs = List.copyOf(stubs);
        buildSearchCoordinator();
    }

    public void setEmbeddingProvider(EmbeddingProvider provider) {
        this.embeddingProvider = provider;
    }

    public void setIvfRouter(IvfRouter router) {
        this.ivfRouter = router != null ? router : new IvfRouter();
    }

    /**
     * Inject GraphQuery engine to enable the third search path (graph recall). When this setter is
     * not called, graphQueryEngine is null, search only does vector + keyword dual-path RRF.
     */
    public void setGraphQueryEngine(io.github.rawwise.axiom.pipeline.graph.GraphQuery engine) {
        this.graphQueryEngine = engine;
    }

    public void setQueryReformulationStage(
            io.github.rawwise.axiom.pipeline.graph.distributed.QueryReformulationStage stage) {
        this.queryReformulationStage = stage;
    }

    public void setFeedbackCollector(RetrievalFeedbackCollector collector) {
        this.feedbackCollector = collector;
    }

    public void setRerankerSwitcher(RerankerAbSwitcher switcher) {
        this.rerankerSwitcher = switcher;
    }

    public void setRerankerModel(DefaultRerankerModel model) {
        this.rerankerModel = model;
    }

    public io.github.rawwise.axiom.pipeline.graph.GraphQuery getGraphQueryEngine() {
        return graphQueryEngine;
    }

    public DistributedSearchCoordinator getSearchCoordinator() {
        return searchCoordinator;
    }

    public IvfRouter getIvfRouter() {
        return ivfRouter;
    }

    public EmbeddingProvider getEmbeddingProvider() {
        return embeddingProvider;
    }

    public void setHybridSearchService(
            io.github.rawwise.axiom.pipeline.graph.distributed.HybridGraphSearchService service) {
        this.hybridSearchService = service;
    }

    public io.github.rawwise.axiom.pipeline.graph.distributed.HybridGraphSearchService
            getHybridSearchService() {
        return hybridSearchService;
    }

    public void loadIvfRouterFromMetastore(io.github.rawwise.metastore.Metastore metastore) {
        this.metastoreRef = metastore;
        try {
            IvfRouter router = new IvfRouter();
            var entries = metastore.scanAll(ShardCentroids.METASTORE_PREFIX);
            for (var entry : entries) {
                try {
                    ShardCentroids sc = ShardCentroids.fromBytes(entry.value());
                    router.registerShard(sc);
                } catch (Exception e) {
                    log.warn("Failed to deserialize shard centroids: {}", e.getMessage(), e);
                }
            }
            this.ivfRouter = router;
            log.info("Loaded {} shard centroids from metastore", router.shardCount());
        } catch (Exception e) {
            log.warn("Failed to load IvfRouter centroids from metastore: {}", e.getMessage(), e);
        }
    }

    public void startCentroidRefresh(io.github.rawwise.metastore.Metastore metastore) {
        loadIvfRouterFromMetastore(metastore);
        centroidRefreshScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "centroid-refresh");
                            t.setDaemon(true);
                            return t;
                        });
        centroidRefreshScheduler.scheduleAtFixedRate(
                () -> loadIvfRouterFromMetastore(metastore), 30, 30, TimeUnit.SECONDS);
    }

    public void close() {
        if (rerankerModel != null) {
            try {
                rerankerModel.close();
            } catch (Exception e) {
                log.debug("Failed to close DefaultRerankerModel", e);
            }
        }
        if (centroidRefreshScheduler != null) {
            centroidRefreshScheduler.shutdown();
            try {
                if (!centroidRefreshScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    centroidRefreshScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                centroidRefreshScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (hybridSearchService != null) {
            hybridSearchService.close();
        }
        rerankExecutor.shutdown();
        RERANKER_EXECUTOR.shutdownNow();
    }

    private void executeHybridSearch(
            AxiomSearchRequest request, StreamObserver<AxiomSearchResponse> responseObserver) {
        long totalStart = System.nanoTime();
        try {
            int topK = request.getTopK() > 0 ? request.getTopK() : 10;
            HybridSearchRequest req =
                    HybridSearchRequest.builder()
                            .tableId(request.getTableId())
                            .query(request.getQuery())
                            .topK(topK)
                            .multiQuery(
                                    Boolean.parseBoolean(
                                            System.getProperty(
                                                    "rawwise.axiom.hybrid.multi-query", "true")))
                            .graphMaxHops(2)
                            .rrfK(60)
                            .agentId("")
                            .memoryTier("")
                            .snapshotId(request.getSnapshotId())
                            .build();

            long vectorStart = System.nanoTime();
            HybridSearchResponse resp = hybridSearchService.hybridSearch(req);
            long vectorMs = (System.nanoTime() - vectorStart) / 1_000_000;

            Map<String, Double> fusedScores = new HashMap<>();
            List<VectorSearchResult> hybridVecResults = new ArrayList<>();
            for (HybridSearchResponse.Hit hit : resp.hits()) {
                fusedScores.put(hit.viewId(), hit.score());
                String summary = hit.summary() != null ? hit.summary() : "";
                hybridVecResults.add(
                        new VectorSearchResult(hit.viewId(), summary, (float) hit.score()));
            }

            long[] rerankHolder = new long[1];
            applyRerankAndRespond(
                    fusedScores,
                    hybridVecResults,
                    List.of(),
                    request.getQuery(),
                    request.getTableId(),
                    topK,
                    responseObserver,
                    rerankHolder,
                    Map.of(),
                    request.getFilterMap());

            long totalMs = (System.nanoTime() - totalStart) / 1_000_000;
            log.info(
                    "Search latency breakdown: total={}ms embedding={}ms vector={}ms graph={}ms rerank={}ms (query=\"{}\")",
                    totalMs,
                    0L,
                    vectorMs,
                    0L,
                    rerankHolder[0],
                    request.getQuery());
        } catch (Exception e) {
            log.error("Hybrid axiom search failed", e);
            responseObserver.onError(e);
        }
    }

    private void applyRerankAndRespond(
            Map<String, Double> fusedScores,
            List<VectorSearchResult> vecResults,
            List<io.github.rawwise.server.KeywordSearchResponse> kwResponses,
            String query,
            String tableId,
            int topK,
            StreamObserver<AxiomSearchResponse> responseObserver,
            Map<String, String> filter) {
        applyRerankAndRespond(
                fusedScores,
                vecResults,
                kwResponses,
                query,
                tableId,
                topK,
                responseObserver,
                null,
                Map.of(),
                filter);
    }

    private void applyRerankAndRespond(
            Map<String, Double> fusedScores,
            List<VectorSearchResult> vecResults,
            List<io.github.rawwise.server.KeywordSearchResponse> kwResponses,
            String query,
            String tableId,
            int topK,
            StreamObserver<AxiomSearchResponse> responseObserver,
            long[] rerankTimeHolder,
            Map<String, String> graphContentMap,
            Map<String, String> filter) {
        List<Map.Entry<String, Double>> sorted = new ArrayList<>(fusedScores.entrySet());
        if (!rankBiases.isEmpty()) {
            java.util.Set<String> currentEntries = new java.util.HashSet<>(fusedScores.keySet());
            int biasedCount = 0;
            int injectedCount = 0;
            for (int i = 0; i < sorted.size(); i++) {
                Map.Entry<String, Double> e = sorted.get(i);
                Double bias = rankBiases.get(e.getKey());
                if (bias != null) {
                    double newScore = e.getValue() + bias * 0.1;
                    sorted.set(i, Map.entry(e.getKey(), newScore));
                    biasedCount++;
                }
            }
            for (Map.Entry<String, Double> biasEntry : rankBiases.entrySet()) {
                if (biasEntry.getValue() > 0 && !currentEntries.contains(biasEntry.getKey())) {
                    sorted.add(Map.entry(biasEntry.getKey(), biasEntry.getValue() * 0.05));
                    injectedCount++;
                }
            }
            if (biasedCount + injectedCount > 0) {
                log.info(
                        "rankBias applied: biased={} injected={} (rankBiases total={})",
                        biasedCount,
                        injectedCount,
                        rankBiases.size());
            }
        }
        sorted.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        sorted = applySemanticBoost(sorted, vecResults, kwResponses, tableId);

        String dedupMode = System.getProperty("rawwise.axiom.fact.dedup-mode", "D");
        if (!"off".equalsIgnoreCase(dedupMode)) {
            sorted = applyFactDedup(sorted, dedupMode, topK);
        }

        long rerankStart = System.nanoTime();
        if (rerankerModel != null) {
            final List<Map.Entry<String, Double>> sortedForRerank = sorted;
            var rerankFuture =
                    CompletableFuture.supplyAsync(
                            () ->
                                    applyCrossEncoderRerank(
                                            sortedForRerank,
                                            query,
                                            vecResults,
                                            kwResponses,
                                            rerankerModel),
                            rerankExecutor);
            try {
                sorted = rerankFuture.get(RERANKER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                rerankFuture.cancel(true);
                log.warn(
                        "Reranker timeout ({}ms), skipping cross-encoder rerank",
                        RERANKER_TIMEOUT_MS);
            } catch (Exception e) {
                log.warn("Reranker failed, skipping: {}", e.getMessage(), e);
            }
        }

        if (rerankerSwitcher != null) {
            RerankerModelMetadata trainedModel = rerankerSwitcher.allocateTraffic(query);
            if (trainedModel != null && !trainedModel.weights().isEmpty()) {
                sorted = applyTrainedReranker(sorted, trainedModel.weights());
            }
        }

        log.info(
                "Reranker check: enabled={} endpointEmpty={} sortedEmpty={}",
                RERANKER_ENABLED,
                RERANKER_ENDPOINT.isEmpty(),
                sorted.isEmpty());
        if (RERANKER_ENABLED && !RERANKER_ENDPOINT.isEmpty() && !sorted.isEmpty()) {
            try {
                sorted = llmRerank(sorted, query, vecResults, kwResponses, topK);
                log.info(
                        "LLM reranker applied: {} candidates rescored",
                        Math.min(sorted.size(), RERANKER_TOP_N));
            } catch (Exception e) {
                log.warn("LLM reranker failed, using RRF scores: {}", e.getMessage(), e);
            }
        } else if (RERANKER_ENABLED) {
            log.debug("LLM reranker enabled but no endpoint configured or no candidates");
        }
        long rerankMs = (System.nanoTime() - rerankStart) / 1_000_000;
        if (rerankTimeHolder != null && rerankTimeHolder.length > 0) {
            rerankTimeHolder[0] = rerankMs;
        }

        AxiomSearchResponse.Builder responseBuilder = AxiomSearchResponse.newBuilder();

        List<SearchResultItem> items = new ArrayList<>();
        // filter 为空时只需前 topK 个候选的 props（循环在 count>=topK 即停），避免无谓预取；
        // filter 非空时预取窗口放宽到 PROPS_PREFETCH_MAX，超出窗口的候选在循环内按需回退点查，
        // 保证 filter 匹配不受预取上限截断（权衡：filter 场景下超出窗口的命中走串行点查）。
        int prefetchLimit = filter.isEmpty() ? Math.min(topK, sorted.size()) : PROPS_PREFETCH_MAX;
        List<String> candidateKeys = new ArrayList<>(Math.min(prefetchLimit, sorted.size()));
        for (var fusedEntry : sorted) {
            candidateKeys.add(fusedEntry.getKey());
            if (candidateKeys.size() >= prefetchLimit) {
                break;
            }
        }
        Map<String, RecordPoint> propsCache = fetchRecordPropsBatch(tableId, candidateKeys);
        int count = 0;
        for (var fusedEntry : sorted) {
            if (count >= topK) break;
            String entryKey = fusedEntry.getKey();
            double biasedScore = applyRankBias(entryKey, (float) (double) fusedEntry.getValue());

            RecordPoint recPoint = propsCache.get(entryKey);
            if (!filter.isEmpty()) {
                if (recPoint == null) {
                    recPoint = fetchRecordProps(tableId, entryKey);
                }
                Map<String, String> props = recPoint == null ? Map.of() : recPoint.properties();
                boolean matched = true;
                for (var fe : filter.entrySet()) {
                    if (!fe.getValue().equals(props.get(fe.getKey()))) {
                        matched = false;
                        break;
                    }
                }
                if (!matched) {
                    continue;
                }
            }

            String summary = "";
            for (VectorSearchResult r : vecResults) {
                if (r.entryKey().equals(entryKey) && !r.l0Abstract().isEmpty()) {
                    summary = r.l0Abstract();
                    break;
                }
            }
            if (summary.isEmpty()) {
                for (io.github.rawwise.server.KeywordSearchResponse resp : kwResponses) {
                    for (io.github.rawwise.server.KeywordSearchHit hit : resp.getHitsList()) {
                        if (hit.getDocId().equals(entryKey)) {
                            summary = hit.getSummary();
                            break;
                        }
                    }
                }
            }
            if (summary.isEmpty()) {
                summary = graphContentMap.getOrDefault(entryKey, "");
            }

            SearchResultItem.Builder itemBuilder =
                    SearchResultItem.newBuilder()
                            .setViewId(entryKey)
                            .setScore((float) biasedScore)
                            .setSummary(summary);
            if (recPoint != null) {
                if (!recPoint.properties().isEmpty()) {
                    itemBuilder.putAllProperties(recPoint.properties());
                }
                if (!recPoint.body().isEmpty()) {
                    itemBuilder.setRawText(recPoint.body());
                }
            }

            String rawKey = extractRawKey(entryKey);
            if (rawKey != null && !rawKey.isEmpty()) {
                itemBuilder.setProvenance(ProvenanceChain.newBuilder().setRawKey(rawKey).build());
            }

            items.add(itemBuilder.build());
            count++;
        }

        items.forEach(responseBuilder::addResults);
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    private void buildSearchCoordinator() {
        List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                this.executorStubs;
        if (stubs.isEmpty()) {
            this.shardClientsMap = Map.of();
            this.searchCoordinator =
                    new DistributedSearchCoordinator(Map.of(), DEFAULT_SEARCH_TIMEOUT_MS);
            return;
        }

        Map<String, ShardClient> shardClients = new HashMap<>();
        for (int i = 0; i < stubs.size(); i++) {
            final int shardIdx = i;
            final String shardId = "shard-" + i;
            shardClients.put(
                    shardId,
                    new ShardClient() {
                        @Override
                        public List<SearchCandidate> search(
                                String tableId, String sid, float[] queryEmb, int k) {
                            ByteString queryVecBytes = floatArrayToBytes(queryEmb);
                            io.github.rawwise.server.VectorSearchRequest req =
                                    io.github.rawwise.server.VectorSearchRequest.newBuilder()
                                            .setTableId(tableId)
                                            .setQueryVector(queryVecBytes)
                                            .setTopK(k)
                                            .build();
                            try {
                                io.github.rawwise.server.VectorSearchResponse resp =
                                        stubs.get(shardIdx)
                                                .withDeadlineAfter(10, TimeUnit.SECONDS)
                                                .vectorSearch(req);
                                List<SearchCandidate> candidates = new ArrayList<>();
                                for (var r : resp.getResultsList()) {
                                    candidates.add(
                                            new SearchCandidate(
                                                    r.getKey().toStringUtf8(),
                                                    (float) (1.0f - r.getSimilarity())));
                                }
                                return candidates;
                            } catch (Exception e) {
                                throw new RuntimeException("Shard search failed: " + sid, e);
                            }
                        }
                    });
        }
        this.shardClientsMap = Map.copyOf(shardClients);
        this.searchCoordinator =
                new DistributedSearchCoordinator(shardClients, DEFAULT_SEARCH_TIMEOUT_MS);
    }

    private List<VectorSearchResult> distributedVectorSearch(
            String tableId,
            float[] queryEmbedding,
            int limit,
            int nprobe,
            IvfRouter router,
            DistributedSearchCoordinator coordinator) {
        if (REMOTE_SHARD_INDEX_ENABLED && !shardClientsMap.isEmpty()) {
            Map<String, ShardInternalIndex> remoteIndices = new HashMap<>();
            for (var entry : shardClientsMap.entrySet()) {
                remoteIndices.put(
                        entry.getKey(),
                        new RemoteShardIndex(tableId, entry.getKey(), entry.getValue()));
            }
            return coordinator.distributedSearch(
                    remoteIndices, queryEmbedding, limit, router, nprobe);
        }
        return coordinator.distributedSearch(
                tableId, queryEmbedding, limit, DEFAULT_SEARCH_TIMEOUT_MS, nprobe, router);
    }

    @Override
    public void ingest(
            AxiomIngestRequest request, StreamObserver<AxiomIngestResponse> responseObserver) {
        List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                this.executorStubs;
        if (stubs.isEmpty()) {
            responseObserver.onError(
                    new UnsupportedOperationException(
                            "Ingest unavailable: no Spark executor nodes (running in no-spark mode). Start backend without --no-spark for full dogfooding."));
            return;
        }
        try {
            io.github.rawwise.server.AxiomIngestRequest.Builder execReqBuilder =
                    io.github.rawwise.server.AxiomIngestRequest.newBuilder()
                            .setTableId(request.getTableId())
                            .setKey(ByteString.copyFromUtf8(request.getKey()))
                            .setBody(request.getBody())
                            .setImmutable(request.getImmutable())
                            .putAllProperties(request.getPropertiesMap());
            if (request.hasEmbedding()) {
                execReqBuilder.setEmbedding(request.getEmbedding());
            }
            io.github.rawwise.server.AxiomIngestRequest execReq = execReqBuilder.build();

            int targetIdx = routeIngestTarget(request, stubs);
            io.github.rawwise.server.AxiomIngestResponse execResp =
                    stubs.get(targetIdx)
                            .withDeadlineAfter(30, TimeUnit.SECONDS)
                            .axiomIngest(execReq);
            AxiomIngestResponse response =
                    AxiomIngestResponse.newBuilder()
                            .setRecordKey(execResp.getRecordKey())
                            .setChunkCount(execResp.getChunkCount())
                            .setStatus(execResp.getStatus())
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Axiom ingest failed", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void rederive(
            AxiomRederiveRequest request, StreamObserver<AxiomRederiveResponse> responseObserver) {
        List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                this.executorStubs;
        if (stubs.isEmpty()) {
            responseObserver.onError(
                    new UnsupportedOperationException(
                            "Rederive unavailable: no Spark executor nodes."));
            return;
        }
        try {
            io.github.rawwise.server.ExecutorRederiveRequest.Builder execReqBuilder =
                    io.github.rawwise.server.ExecutorRederiveRequest.newBuilder()
                            .setTableId(request.getTableId())
                            .setScope(request.getScope().getNumber())
                            .setForceReindex(request.getForceReindex());
            if (!request.getStartKey().isEmpty()) {
                execReqBuilder.setStartKey(ByteString.copyFromUtf8(request.getStartKey()));
            }
            if (!request.getEndKey().isEmpty()) {
                execReqBuilder.setEndKey(ByteString.copyFromUtf8(request.getEndKey()));
            }
            if (!request.getSingleKey().isEmpty()) {
                execReqBuilder.setSingleKey(ByteString.copyFromUtf8(request.getSingleKey()));
            }
            io.github.rawwise.server.ExecutorRederiveRequest execReq = execReqBuilder.build();

            int totalKeys = 0;
            int rederivedCount = 0;
            int failedCount = 0;
            String status = "OK";

            for (ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub : stubs) {
                try {
                    io.github.rawwise.server.ExecutorRederiveResponse execResp =
                            stub.withDeadlineAfter(120, TimeUnit.SECONDS).axiomRederive(execReq);
                    totalKeys += execResp.getTotalKeys();
                    rederivedCount += execResp.getRederivedCount();
                    failedCount += execResp.getFailedCount();
                    if (!"OK".equals(execResp.getStatus())) {
                        status = "PARTIAL";
                    }
                } catch (Exception e) {
                    log.warn("Rederive failed on executor stub: {}", e.getMessage(), e);
                    status = "PARTIAL";
                }
            }

            AxiomRederiveResponse response =
                    AxiomRederiveResponse.newBuilder()
                            .setTotalKeys(totalKeys)
                            .setRederivedCount(rederivedCount)
                            .setFailedCount(failedCount)
                            .setStatus(status)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Axiom rederive failed", e);
            responseObserver.onError(e);
        }
    }

    private int routeIngestTarget(
            AxiomIngestRequest request,
            List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs) {
        if (stubs.size() <= 1) return 0;

        // L1: request carries embedding -> IVF route
        if (request.hasEmbedding()) {
            float[] emb = bytesToFloatArray(request.getEmbedding().toByteArray());
            List<String> shards = ivfRouter.route(emb, 1);
            if (!shards.isEmpty()) {
                return shardToStubIndex(shards.get(0));
            }
        }

        // L2: request has text_content property, centroids available -> embed + IVF route
        String textContent = request.getPropertiesMap().get("text_content");
        if (textContent != null
                && !textContent.isEmpty()
                && ivfRouter.shardCount() > 0
                && embeddingProvider != null) {
            try {
                float[] emb = embeddingProvider.embed(textContent);
                List<String> shards = ivfRouter.route(emb, 1);
                if (!shards.isEmpty()) {
                    return shardToStubIndex(shards.get(0));
                }
            } catch (Exception e) {
                log.warn(
                        "Embedding for ingest routing failed, falling back to round-robin: {}",
                        e.getMessage(),
                        e);
            }
        }

        // L3: cold start / fallback -> Round-Robin
        return Math.abs(roundRobinIndex.getAndIncrement() % stubs.size());
    }

    private int shardToStubIndex(String shardId) {
        return Integer.parseInt(shardId.substring("shard-".length()));
    }

    private static float[] bytesToFloatArray(byte[] bytes) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
        float[] arr = new float[bytes.length / 4];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = buf.getFloat();
        }
        return arr;
    }

    @Override
    public void search(
            AxiomSearchRequest request, StreamObserver<AxiomSearchResponse> responseObserver) {
        if (hybridSearchService != null
                && "hybrid".equals(System.getProperty("rawwise.axiom.search.path", "hybrid"))) {
            try {
                executeHybridSearch(request, responseObserver);
                return;
            } catch (Exception e) {
                log.warn("Hybrid search failed, falling back to legacy: {}", e.getMessage(), e);
            }
        }
        List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                this.executorStubs;
        EmbeddingProvider ep = this.embeddingProvider;
        DistributedSearchCoordinator coordinator = this.searchCoordinator;
        if (stubs.isEmpty()) {
            log.warn("No executor stubs available for search");
            responseObserver.onNext(AxiomSearchResponse.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }
        if (ep == null) {
            responseObserver.onError(new IllegalStateException("EmbeddingProvider not available"));
            return;
        }
        try {
            long totalStart = System.nanoTime();
            String originalQuery = request.getQuery();

            String rewrittenQuery = originalQuery;
            if (QUERY_REWRITE_ENABLED
                    && !QUERY_REWRITE_ENDPOINT.isEmpty()
                    && !QUERY_REWRITE_API_KEY.isEmpty()) {
                try {
                    rewrittenQuery = rewriteQueryWithLlm(originalQuery);
                    if (!rewrittenQuery.isBlank()
                            && !rewrittenQuery.equalsIgnoreCase(originalQuery)) {
                        log.info("Query rewritten: [{}] -> [{}]", originalQuery, rewrittenQuery);
                    } else {
                        rewrittenQuery = originalQuery;
                    }
                } catch (Exception e) {
                    log.warn("LLM query rewrite failed, using original: {}", e.getMessage(), e);
                    rewrittenQuery = originalQuery;
                }
            }

            long embedStart = System.nanoTime();
            float[] queryEmbedding = ep.embed(rewrittenQuery);
            int topK = request.getTopK() > 0 ? request.getTopK() : 10;

            String queryText = rewrittenQuery;
            try {
                if (io.github.rawwise.axiom.pipeline.derivation.ChineseSegmenter.hasChinese(
                        queryText)) {
                    queryText =
                            io.github.rawwise.axiom.pipeline.derivation.ChineseSegmenter
                                    .getInstance()
                                    .segment(queryText);
                }
            } catch (Exception e) {
                log.warn("Chinese segmentation failed, using raw query: {}", e.getMessage(), e);
            }
            long embedMs = (System.nanoTime() - embedStart) / 1_000_000;

            long vectorStart = System.nanoTime();
            List<VectorSearchResult> vecResults = List.of();
            if (coordinator != null) {
                try {
                    int nprobe =
                            ivfRouter.shardCount() > 0
                                    ? Math.min(4, coordinator.shardCount())
                                    : coordinator.shardCount();
                    float[] viewEmbedding = queryEmbedding;
                    vecResults =
                            coordinator.distributedSearch(
                                    request.getTableId(),
                                    viewEmbedding,
                                    topK * SEARCH_TOPK_MULTIPLIER,
                                    DEFAULT_SEARCH_TIMEOUT_MS,
                                    nprobe,
                                    ivfRouter);
                } catch (Exception e) {
                    log.warn("Distributed vector search failed: {}", e.getMessage(), e);
                }
            }

            io.github.rawwise.server.KeywordSearchRequest kwReq =
                    io.github.rawwise.server.KeywordSearchRequest.newBuilder()
                            .setTableId(request.getTableId())
                            .setQuery(queryText)
                            .setTopK(topK * SEARCH_TOPK_MULTIPLIER)
                            .build();

            java.util.List<
                            java.util.concurrent.CompletableFuture<
                                    io.github.rawwise.server.KeywordSearchResponse>>
                    kwFutures = new ArrayList<>();
            for (ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub : stubs) {
                kwFutures.add(
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                () ->
                                        stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                                                .keywordSearch(kwReq)));
            }

            java.util.List<io.github.rawwise.server.KeywordSearchResponse> kwResponses =
                    kwFutures.stream()
                            .map(
                                    f -> {
                                        try {
                                            return f.get(15, TimeUnit.SECONDS);
                                        } catch (Exception e) {
                                            log.debug(
                                                    "Keyword search future failed: {}",
                                                    e.getMessage());
                                            return null;
                                        }
                                    })
                            .filter(r -> r != null)
                            .toList();

            Map<String, double[]> sigRaw = new HashMap<>();
            int vecRank = 1;
            for (VectorSearchResult r : vecResults) {
                double[] sig =
                        sigRaw.computeIfAbsent(r.entryKey(), k -> new double[SIG_CHANNEL_COUNT]);
                sig[0] += 1.0 / (RRF_K + vecRank);
                vecRank++;
            }
            int kwRank = 1;
            for (io.github.rawwise.server.KeywordSearchResponse resp : kwResponses) {
                for (io.github.rawwise.server.KeywordSearchHit hit : resp.getHitsList()) {
                    double[] sig =
                            sigRaw.computeIfAbsent(
                                    hit.getDocId(), k -> new double[SIG_CHANNEL_COUNT]);
                    sig[1] += 1.0 / (RRF_K + kwRank);
                    kwRank++;
                }
            }

            // Graph recallthird path(if graphQueryEngine injectand rescue.enabled=true then
            // enabled).
            // from query textextractcandidate entity name → findEntity → 1-hop traverse →
            // lookupDiaIds → RRF accumulate.
            // graph hitin entry even ifnotin vector+keyword recallsetalsoinjectcandidate
            // pool(graph-only signal),
            // graph-only entry content using entity name as fallback.
            io.github.rawwise.axiom.pipeline.graph.GraphQuery gq = this.graphQueryEngine;
            Map<String, String> graphContentMap = new HashMap<>();
            if (gq != null && GRAPH_RESCUE_ENABLED) {
                int graphHits = 0;
                double graphBoost = GRAPH_BOOST;
                try {
                    java.util.List<String> candidates = extractEntityCandidates(request.getQuery());
                    java.util.Set<String> graphEntryKeys = new java.util.LinkedHashSet<>();
                    for (String name : candidates) {
                        java.util.Optional<io.github.rawwise.axiom.pipeline.entity.Entity> ent =
                                gq.findEntity("", name);
                        if (ent.isEmpty()) continue;
                        String entityLabel = ent.get().name();
                        java.util.Map<String, Double> traversed =
                                gq.traverse(
                                        java.util.Set.of(ent.get().id()),
                                        1,
                                        io.github.rawwise.axiom.pipeline.graph.EdgeFilter
                                                .allowAll());
                        for (String entityId : traversed.keySet()) {
                            for (io.github.rawwise.axiom.pipeline.graph.DiaRef ref :
                                    gq.lookupDiaIds(entityId)) {
                                // ref.diaId is base64url(record.key),
                                // needs decoding to original record key to match vec/keyword's
                                // "rec:<rawKey>" format.
                                String decodedKey;
                                try {
                                    decodedKey =
                                            new String(
                                                    java.util.Base64.getUrlDecoder()
                                                            .decode(ref.diaId()),
                                                    java.nio.charset.StandardCharsets.UTF_8);
                                } catch (IllegalArgumentException ex) {
                                    // Compatible with non-base64url legacy/test data
                                    decodedKey = ref.diaId();
                                }
                                String entryKey = "rec:" + decodedKey;
                                graphEntryKeys.add(entryKey);
                                graphContentMap.putIfAbsent(entryKey, entityLabel);
                            }
                        }
                    }
                    double gBoost = graphBoost / 61.0;
                    for (String key : graphEntryKeys) {
                        double[] sig =
                                sigRaw.computeIfAbsent(key, k -> new double[SIG_CHANNEL_COUNT]);
                        sig[2] += gBoost;
                        graphHits++;
                    }
                    log.info(
                            "Graph recall path (rescue mode): candidates={} graph_keys={} graph_boosted={} boost={}",
                            candidates.size(),
                            graphEntryKeys.size(),
                            graphHits,
                            graphBoost);
                } catch (RuntimeException e) {
                    log.warn(
                            "Graph recall path failed (degraded to vector+keyword only): {}",
                            e.getMessage());
                }
            }

            // Multi-Query Reformulation.
            // generate variants query, eachvariantparalleldo vec+keyword search, resultusing RRF
            // fuse into fusedScores.
            // multiplevariantallrecall doc scoreaccumulate → rank boost, increase recall coverage.
            if (MULTIQUERY_ENABLED && MULTIQUERY_EXPAND_N > 1 && coordinator != null) {
                List<String> variants;
                io.github.rawwise.axiom.pipeline.graph.distributed.QueryReformulationStage rs =
                        this.queryReformulationStage;
                if (LLM_REFORMULATION_ENABLED && rs != null) {
                    try {
                        variants = rs.expand(request.getQuery(), MULTIQUERY_EXPAND_N);
                        if (variants == null || variants.isEmpty()) {
                            variants = expandQuery(request.getQuery(), MULTIQUERY_EXPAND_N);
                            log.info(
                                    "LLM reformulation returned empty, fell back to rule-based expandQuery");
                        } else {
                            log.info(
                                    "LLM reformulation: {} variants for query='{}'",
                                    variants.size(),
                                    request.getQuery());
                        }
                    } catch (Exception e) {
                        log.warn(
                                "LLM reformulation failed, falling back to rule-based: {}",
                                e.getMessage(),
                                e);
                        variants = expandQuery(request.getQuery(), MULTIQUERY_EXPAND_N);
                    }
                } else {
                    variants = expandQuery(request.getQuery(), MULTIQUERY_EXPAND_N);
                    log.debug("Rule-based expandQuery: {} variants", variants.size());
                }
                int nprobe =
                        ivfRouter.shardCount() > 0
                                ? Math.min(4, coordinator.shardCount())
                                : coordinator.shardCount();

                List<java.util.concurrent.CompletableFuture<Map<String, double[]>>> variantFutures =
                        new ArrayList<>();
                for (int vi = 1; vi < variants.size(); vi++) {
                    final String variant = variants.get(vi);
                    variantFutures.add(
                            java.util.concurrent.CompletableFuture.supplyAsync(
                                    () -> {
                                        Map<String, double[]> vMap = new HashMap<>();
                                        try {
                                            float[] variantEmb = ep.embed(variant);
                                            List<VectorSearchResult> variantVec =
                                                    distributedVectorSearch(
                                                            request.getTableId(),
                                                            variantEmb,
                                                            topK * SEARCH_TOPK_MULTIPLIER,
                                                            nprobe,
                                                            ivfRouter,
                                                            coordinator);
                                            int rank = 1;
                                            for (VectorSearchResult r : variantVec) {
                                                double[] vs =
                                                        vMap.computeIfAbsent(
                                                                r.entryKey(), k -> new double[2]);
                                                vs[0] += 1.0 / (RRF_K + rank);
                                                rank++;
                                            }
                                        } catch (Exception e) {
                                            log.debug(
                                                    "Variant vec search failed for '{}': {}",
                                                    variant,
                                                    e.getMessage());
                                        }
                                        try {
                                            String variantText = variant;
                                            if (io.github.rawwise.axiom.pipeline.derivation
                                                    .ChineseSegmenter.hasChinese(variantText)) {
                                                variantText =
                                                        io.github.rawwise.axiom.pipeline.derivation
                                                                .ChineseSegmenter.getInstance()
                                                                .segment(variantText);
                                            }
                                            io.github.rawwise.server.KeywordSearchRequest
                                                    variantKwReq =
                                                            io.github.rawwise.server
                                                                    .KeywordSearchRequest
                                                                    .newBuilder()
                                                                    .setTableId(
                                                                            request.getTableId())
                                                                    .setQuery(variantText)
                                                                    .setTopK(
                                                                            topK
                                                                                    * SEARCH_TOPK_MULTIPLIER)
                                                                    .build();
                                            for (ExecutorPointQueryServiceGrpc
                                                            .ExecutorPointQueryServiceBlockingStub
                                                    stub : stubs) {
                                                try {
                                                    io.github.rawwise.server.KeywordSearchResponse
                                                            resp =
                                                                    stub.withDeadlineAfter(
                                                                                    10,
                                                                                    TimeUnit
                                                                                            .SECONDS)
                                                                            .keywordSearch(
                                                                                    variantKwReq);
                                                    int rank = 1;
                                                    for (io.github.rawwise.server.KeywordSearchHit
                                                            hit : resp.getHitsList()) {
                                                        double[] vs =
                                                                vMap.computeIfAbsent(
                                                                        hit.getDocId(),
                                                                        k -> new double[2]);
                                                        vs[1] += 1.0 / (RRF_K + rank);
                                                        rank++;
                                                    }
                                                } catch (Exception e) {
                                                    log.debug(
                                                            "Variant keyword search failed: {}",
                                                            e.getMessage());
                                                }
                                            }
                                        } catch (Exception e) {
                                            log.debug(
                                                    "Variant keyword search failed for '{}': {}",
                                                    variant,
                                                    e.getMessage());
                                        }
                                        return vMap;
                                    }));
                }

                int variantHits = 0;
                for (java.util.concurrent.CompletableFuture<Map<String, double[]>> f :
                        variantFutures) {
                    try {
                        Map<String, double[]> vs =
                                f.get(DEFAULT_SEARCH_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS);
                        for (Map.Entry<String, double[]> e : vs.entrySet()) {
                            double[] sig =
                                    sigRaw.computeIfAbsent(
                                            e.getKey(), k -> new double[SIG_CHANNEL_COUNT]);
                            sig[0] += e.getValue()[0];
                            sig[1] += e.getValue()[1];
                            sig[3] += 1;
                            variantHits++;
                        }
                    } catch (Exception e) {
                        log.debug("Variant future failed: {}", e.getMessage());
                    }
                }
                log.info(
                        "Multi-Query: {} variants, variant_hits={}, sig_keys={}",
                        variants.size() - 1,
                        variantHits,
                        sigRaw.size());
            }

            // Phase 1: collectSubjectSignal — subject consistencysignalcollect
            // inallrecallchannel(vec/kw/graph/multi-query)populate sigRaw after, feature
            // weightfusionbefore,
            // foreachcandidate entry compute sig[4]: subject entityparticipate dia → 1.0, otherwise
            // 0.0.
            // no subjectorinverted indexmissingwhen sig[4]=0(no bonus no penalty, by
            // FEATURE_SUBJECT_BOOST control).
            if (ENTITY_AWARE_ENABLED && gq != null) {
                try {
                    java.util.List<String> subjectCandidates =
                            extractEntityCandidates(request.getQuery());
                    io.github.rawwise.axiom.pipeline.graph.QuerySubjectProfile subjectProfile =
                            io.github.rawwise.axiom.pipeline.graph.QuerySubjectProfile.resolve(
                                    request.getQuery(), subjectCandidates, gq, "");
                    if (subjectProfile.hasSubject()) {
                        int subjectMatched = 0;
                        for (Map.Entry<String, double[]> entry : sigRaw.entrySet()) {
                            String diaId = extractDiaIdFromEntryKey(entry.getKey());
                            if (diaId == null) continue;
                            java.util.Set<String> diaEntities = gq.lookupEntitiesByDia("", diaId);
                            if (subjectProfile.isSubjectInDia(diaEntities)) {
                                entry.getValue()[4] = 1.0;
                                subjectMatched++;
                            }
                        }
                        log.info(
                                "Subject signal: subject_matched={}/{} candidates (subject_ids={})",
                                subjectMatched,
                                sigRaw.size(),
                                subjectProfile.subjectEntityIds());
                    }
                } catch (RuntimeException e) {
                    log.warn(
                            "collectSubjectSignal failed (degraded, sig[4]=0 for all): {}",
                            e.getMessage());
                }
            }

            Map<String, Double> fusedScores = new HashMap<>();
            if (FEATURE_RERANK_ENABLED) {
                boolean useMinMaxNorm = (rerankerModel == null);
                int normChannelCount =
                        4; // sig[4] (subject) does not participate in min-max normalization, uses
                // additive boost
                double[] channelMin = new double[normChannelCount];
                double[] channelMax = new double[normChannelCount];
                if (useMinMaxNorm && !sigRaw.isEmpty()) {
                    java.util.Arrays.fill(channelMin, Double.MAX_VALUE);
                    java.util.Arrays.fill(channelMax, -Double.MAX_VALUE);
                    for (double[] sig : sigRaw.values()) {
                        for (int c = 0; c < normChannelCount; c++) {
                            if (sig[c] < channelMin[c]) channelMin[c] = sig[c];
                            if (sig[c] > channelMax[c]) channelMax[c] = sig[c];
                        }
                    }
                }
                for (Map.Entry<String, double[]> e : sigRaw.entrySet()) {
                    double[] sig = e.getValue();
                    double s0 = sig[0];
                    double s1 = sig[1];
                    double s2 = sig[2];
                    double s3 = sig[3];
                    if (useMinMaxNorm) {
                        s0 = normalizeMinMax(channelMin[0], channelMax[0], sig[0]);
                        s1 = normalizeMinMax(channelMin[1], channelMax[1], sig[1]);
                        s2 = normalizeMinMax(channelMin[2], channelMax[2], sig[2]);
                        s3 = normalizeMinMax(channelMin[3], channelMax[3], sig[3]);
                    }
                    double baseScore =
                            FEATURE_VEC_WEIGHT * s0
                                    + FEATURE_KW_WEIGHT * s1
                                    + FEATURE_GRAPH_WEIGHT * s2
                                    + FEATURE_MULTIQUERY_WEIGHT * s3;
                    double score = baseScore + FEATURE_SUBJECT_BOOST * sig[4];
                    fusedScores.put(e.getKey(), score);
                }
                log.info(
                        "Feature-based reranker applied: {} candidates (w[vec={} kw={} graph={} mq={} subjectBoost={}], minMaxNorm={})",
                        fusedScores.size(),
                        FEATURE_VEC_WEIGHT,
                        FEATURE_KW_WEIGHT,
                        FEATURE_GRAPH_WEIGHT,
                        FEATURE_MULTIQUERY_WEIGHT,
                        FEATURE_SUBJECT_BOOST,
                        useMinMaxNorm);
            } else {
                // Pure RRF fusion (baseline behavior): each channel RRF score is directly
                // accumulated, no min-max norm/weighting
                // sig[3] is multiquery hit count (feature-rerank specific), not used in RRF mode
                // multiquery contribution is already reflected via RRF accumulation in
                // sig[0]/sig[1]
                for (Map.Entry<String, double[]> e : sigRaw.entrySet()) {
                    double[] sig = e.getValue();
                    double score = sig[0] + sig[1] + sig[2] + FEATURE_SUBJECT_BOOST * sig[4];
                    fusedScores.put(e.getKey(), score);
                }
                log.info(
                        "RRF fusion applied: {} candidates (k={}, subjectBoost={})",
                        fusedScores.size(),
                        RRF_K,
                        FEATURE_SUBJECT_BOOST);
            }

            long[] rerankHolder = new long[1];
            applyRerankAndRespond(
                    fusedScores,
                    vecResults,
                    kwResponses,
                    request.getQuery(),
                    request.getTableId(),
                    topK,
                    responseObserver,
                    rerankHolder,
                    graphContentMap,
                    request.getFilterMap());
            long totalMs = (System.nanoTime() - totalStart) / 1_000_000;
            log.info(
                    "Search latency: total={}ms rerank={}ms (query=\"{}\")",
                    totalMs,
                    rerankHolder[0],
                    request.getQuery());
        } catch (Exception e) {
            log.error("Axiom search failed", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void annotate(
            AxiomAnnotateRequest request, StreamObserver<AxiomAnnotateResponse> responseObserver) {
        try {
            // Fix wiring: viewId may be entryKey (rec:...) or actual viewId (view:...)
            // Server-side rankBiases uses inputId (preserves search-time score bias),
            // actual viewId resolution is deferred to Executor side (local store accessible)
            String inputId = request.getViewId();

            FeedbackAnnotation.AnnotationType annType;
            try {
                annType = FeedbackAnnotation.AnnotationType.valueOf(request.getType());
            } catch (IllegalArgumentException e) {
                annType =
                        switch (request.getType().toUpperCase()) {
                            case "NEGATIVE", "POSITIVE" ->
                                    FeedbackAnnotation.AnnotationType.QUALITY_SCORE;
                            case "PREFERENCE" -> FeedbackAnnotation.AnnotationType.PREFERENCE;
                            default -> FeedbackAnnotation.AnnotationType.CORRECTION;
                        };
            }
            FeedbackAnnotation annotation =
                    new DefaultFeedbackAnnotation(
                            "ann-" + UUID.randomUUID().toString().substring(0, 8),
                            inputId,
                            request.getAnnotatorId(),
                            annType,
                            request.getValue(),
                            request.getConfidence(),
                            System.currentTimeMillis());

            double biasDelta =
                    resolveAnnotationBias(
                            request.getType(), request.getValue(), annotation.decayWeight());
            double adjustedScore = 1.0;
            if (biasDelta != 0.0) {
                rankBiases.merge(inputId, biasDelta, Double::sum);
                adjustedScore = 1.0 + rankBiases.getOrDefault(inputId, 0.0);
                savePersistentRankBiases();
            }

            List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                    this.executorStubs;
            if (!stubs.isEmpty()) {
                try {
                    ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub =
                            selectStub(inputId);
                    FeedbackRequest feedbackRequest =
                            FeedbackRequest.newBuilder()
                                    .setViewId(inputId)
                                    .setAnnotationType(request.getType())
                                    .setAnnotationValue(request.getValue())
                                    .setSource("HUMAN")
                                    .setDepth(0)
                                    .build();
                    stub.withDeadlineAfter(10, TimeUnit.SECONDS).processFeedback(feedbackRequest);
                } catch (Exception e) {
                    log.warn(
                            "gRPC processFeedback failed for viewId={}: {}",
                            inputId,
                            e.getMessage(),
                            e);
                }
            }

            RetrievalFeedbackCollector fc = this.feedbackCollector;
            if (fc != null) {
                try {
                    FeedbackType fbType =
                            mapAnnotationToFeedbackType(request.getType(), request.getValue());
                    if (fbType != null) {
                        FeedbackRecord record =
                                FeedbackRecord.builder()
                                        .query("")
                                        .retrievedHitIds(List.of(inputId))
                                        .userExpectedHitId(
                                                fbType == FeedbackType.POSITIVE ? inputId : null)
                                        .feedbackType(fbType)
                                        .correctionText(
                                                fbType == FeedbackType.CORRECTION
                                                        ? request.getValue()
                                                        : null)
                                        .timestamp(java.time.Instant.now())
                                        .metadata(
                                                Map.of(
                                                        "annotatorId",
                                                        request.getAnnotatorId(),
                                                        "confidence",
                                                        String.valueOf(request.getConfidence()),
                                                        "annotationType",
                                                        request.getType()))
                                        .build();
                        fc.recordFeedback(record);
                    }
                } catch (Exception e) {
                    log.warn(
                            "RetrievalFeedbackCollector.recordFeedback failed for viewId={}: {}",
                            inputId,
                            e.getMessage(),
                            e);
                }
            }

            AxiomAnnotateResponse response =
                    AxiomAnnotateResponse.newBuilder()
                            .setSuccess(true)
                            .setAdjustedScore(adjustedScore)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Axiom annotate failed", e);
            responseObserver.onError(e);
        }
    }

    private static FeedbackType mapAnnotationToFeedbackType(String annotationType, String value) {
        if ("CORRECTION".equals(annotationType)) return FeedbackType.CORRECTION;
        if ("DECISION_ADOPTED".equals(annotationType)) return FeedbackType.POSITIVE;
        try {
            double val = Double.parseDouble(value);
            if (val > 0) return FeedbackType.POSITIVE;
            if (val < 0) return FeedbackType.NEGATIVE;
        } catch (NumberFormatException e) {
            log.debug("Failed to parse number", e);
        }
        return null;
    }

    @Override
    public void recordImplicitFeedback(
            ImplicitFeedbackRequest request,
            StreamObserver<ImplicitFeedbackResponse> responseObserver) {
        try {
            RetrievalFeedbackCollector fc = this.feedbackCollector;
            if (fc != null) {
                FeedbackType fbType = mapImplicitSignalToFeedbackType(request.getSignalType());
                FeedbackRecord record =
                        FeedbackRecord.builder()
                                .query("")
                                .retrievedHitIds(List.of(request.getViewId()))
                                .userExpectedHitId(
                                        fbType == FeedbackType.POSITIVE
                                                ? request.getViewId()
                                                : null)
                                .feedbackType(fbType)
                                .correctionText(null)
                                .timestamp(java.time.Instant.now())
                                .metadata(
                                        Map.of(
                                                "signalType", request.getSignalType(),
                                                "dwellMs", String.valueOf(request.getDwellMs()),
                                                "source", "IMPLICIT"))
                                .build();
                fc.recordFeedback(record);
            }
            responseObserver.onNext(
                    ImplicitFeedbackResponse.newBuilder().setAccepted(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.warn(
                    "recordImplicitFeedback failed for viewId={}: {}",
                    request.getViewId(),
                    e.getMessage(),
                    e);
            responseObserver.onNext(
                    ImplicitFeedbackResponse.newBuilder().setAccepted(false).build());
            responseObserver.onCompleted();
        }
    }

    private static FeedbackType mapImplicitSignalToFeedbackType(String signalType) {
        if (signalType == null || signalType.isBlank()) return FeedbackType.POSITIVE;
        String lower = signalType.toLowerCase();
        if (lower.contains("skip")
                || lower.contains("bounce")
                || lower.contains("ignore")
                || lower.contains("dislike")
                || lower.contains("negative")) {
            return FeedbackType.NEGATIVE;
        }
        return FeedbackType.POSITIVE;
    }

    /**
     * Strategy D/E/F: deduplicate and merge fact entries with original text by rawKey.
     *
     * <p>mode=D (baseline): take the first occurrence (highest score) as representative, other
     * entries with same rawKey accumulate at 0.5x
     *
     * <p>mode=E (preferOriginal): prefer retaining original text entry (without #fact), fact only
     * supplements score
     *
     * <p>mode=F (bucketize): fact and original text are bucketed separately, each takes topK*2,
     * merged and deduplicated, fact-only hits discounted at 0.7x
     */
    private List<Map.Entry<String, Double>> applyFactDedup(
            List<Map.Entry<String, Double>> sorted, String mode, int topK) {
        if ("F".equalsIgnoreCase(mode)) {
            return applyFactDedupBucketize(sorted, topK);
        }
        boolean preferOriginal = "E".equalsIgnoreCase(mode);
        Map<String, Double> rawKeyScore = new java.util.LinkedHashMap<>();
        Map<String, String> rawKeyRep = new HashMap<>();
        Map<String, Boolean> rawKeyHasOriginal = new HashMap<>();
        for (Map.Entry<String, Double> e : sorted) {
            String rawKey = extractRawKey(e.getKey());
            if (rawKey == null) rawKey = e.getKey();
            boolean isOriginal = !e.getKey().contains("#fact:");
            Double existing = rawKeyScore.get(rawKey);
            if (existing == null) {
                rawKeyScore.put(rawKey, e.getValue());
                rawKeyRep.put(rawKey, e.getKey());
                rawKeyHasOriginal.put(rawKey, isOriginal);
            } else {
                rawKeyScore.put(rawKey, existing + e.getValue() * 0.5);
                if (preferOriginal) {
                    boolean repIsOriginal = rawKeyHasOriginal.getOrDefault(rawKey, false);
                    if (isOriginal && !repIsOriginal) {
                        rawKeyRep.put(rawKey, e.getKey());
                        rawKeyHasOriginal.put(rawKey, true);
                    }
                }
            }
        }
        List<Map.Entry<String, Double>> result = new ArrayList<>(rawKeyScore.size());
        for (Map.Entry<String, Double> rk : rawKeyScore.entrySet()) {
            result.add(Map.entry(rawKeyRep.get(rk.getKey()), rk.getValue()));
        }
        result.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
        return result;
    }

    /**
     * Strategy F: bucketize fact and original text separately, each takes topK*2 then merge and
     * deduplicate. Fact-only hits are discounted at 0.7x, original+fact simultaneous hits are
     * weighted at 0.3x.
     */
    private List<Map.Entry<String, Double>> applyFactDedupBucketize(
            List<Map.Entry<String, Double>> sorted, int topK) {
        int bucketTopK = Math.max(topK * 2, 30);
        List<Map.Entry<String, Double>> originals = new ArrayList<>();
        List<Map.Entry<String, Double>> facts = new ArrayList<>();
        for (Map.Entry<String, Double> e : sorted) {
            if (e.getKey().contains("#fact:")) {
                if (facts.size() < bucketTopK) facts.add(e);
            } else {
                if (originals.size() < bucketTopK) originals.add(e);
            }
        }
        Map<String, Double> rawKeyScore = new java.util.LinkedHashMap<>();
        Map<String, String> rawKeyRep = new HashMap<>();
        for (Map.Entry<String, Double> e : originals) {
            String rawKey = extractRawKey(e.getKey());
            if (rawKey == null) rawKey = e.getKey();
            rawKeyScore.put(rawKey, e.getValue());
            rawKeyRep.put(rawKey, e.getKey());
        }
        for (Map.Entry<String, Double> e : facts) {
            String rawKey = extractRawKey(e.getKey());
            if (rawKey == null) rawKey = e.getKey();
            Double existing = rawKeyScore.get(rawKey);
            if (existing == null) {
                rawKeyScore.put(rawKey, e.getValue() * 0.7);
                rawKeyRep.put(rawKey, e.getKey());
            } else {
                rawKeyScore.put(rawKey, existing + e.getValue() * 0.3);
            }
        }
        List<Map.Entry<String, Double>> result = new ArrayList<>(rawKeyScore.size());
        for (Map.Entry<String, Double> rk : rawKeyScore.entrySet()) {
            result.add(Map.entry(rawKeyRep.get(rk.getKey()), rk.getValue()));
        }
        result.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
        return result;
    }

    /**
     * Extract the raw record key from an entry key. Entry key format: - rec:&lt;recordKey&gt; (from
     * AutoDerivationHook, original text) - rec:&lt;recordKey&gt;#fact:&lt;idx&gt; (Strategy D:
     * fact-level entries, multiple share the same rawKey) Strips the "rec:" prefix and "#fact:N"
     * suffix to return the original rawKey.
     */
    private static String extractRawKey(String entryKey) {
        if (entryKey == null || entryKey.isEmpty()) return null;
        String key = entryKey;
        if (key.startsWith("rec:")) {
            key = key.substring(4);
        }
        int factIdx = key.indexOf("#fact:");
        if (factIdx >= 0) {
            key = key.substring(0, factIdx);
        }
        return key;
    }

    /** 点查结果属性/正文载体：properties = 持久化属性（agent_id/memory_tier 等），body = 原文。 */
    private record RecordPoint(Map<String, String> properties, String body) {}

    /**
     * 批量并行预取候选 entryKey 的 properties/body，消除循环内串行 N+1 点查。 单个 key 点查失败或超时则跳过（缓存中缺失），search 端降级为
     * summary-only。
     */
    private Map<String, RecordPoint> fetchRecordPropsBatch(String tableId, List<String> entryKeys) {
        Map<String, RecordPoint> cache = new ConcurrentHashMap<>();
        if (entryKeys.isEmpty() || this.executorStubs.isEmpty()) {
            return cache;
        }
        List<java.util.concurrent.CompletableFuture<Void>> futures =
                new ArrayList<>(entryKeys.size());
        for (String entryKey : entryKeys) {
            futures.add(
                    java.util.concurrent.CompletableFuture.runAsync(
                            () -> {
                                try {
                                    RecordPoint rp = fetchRecordProps(tableId, entryKey);
                                    if (rp != null) {
                                        cache.put(entryKey, rp);
                                    }
                                } catch (RuntimeException e) {
                                    log.debug(
                                            "fetchRecordPropsBatch key {} failed: {}",
                                            entryKey,
                                            e.getMessage());
                                }
                            },
                            rerankExecutor));
        }
        try {
            java.util.concurrent.CompletableFuture.allOf(
                            futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                    .get(PROPS_FETCH_BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn(
                    "fetchRecordPropsBatch partial: prefetched {}/{} within {}ms",
                    cache.size(),
                    entryKeys.size(),
                    PROPS_FETCH_BATCH_TIMEOUT_MS);
        }
        return cache;
    }

    /**
     * 尽力而为地从 executor 读取 entryKey 对应记录的 properties 与 body 原文。 使用 raw_key 点查 （key = 存储原始 key
     * bytes）。executor 不可用或数据缺失时返回 null，search 端降级为 summary-only。
     */
    private RecordPoint fetchRecordProps(String tableId, String entryKey) {
        List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                this.executorStubs;
        if (stubs.isEmpty()) return null;
        String rawKey = extractRawKey(entryKey);
        if (rawKey == null || rawKey.isEmpty()) return null;
        io.github.rawwise.server.PointQueryRequest req =
                io.github.rawwise.server.PointQueryRequest.newBuilder()
                        .setTableId(tableId)
                        .setKey(ByteString.copyFromUtf8(rawKey))
                        .setRawKey(true)
                        .build();
        for (ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub : stubs) {
            try {
                io.github.rawwise.server.PointQueryResponse resp =
                        stub.withDeadlineAfter(1000, TimeUnit.MILLISECONDS).pointQuery(req);
                if (resp.getFound() && !resp.getData().isEmpty()) {
                    return parsePointData(resp.getData().toStringUtf8());
                }
            } catch (Exception e) {
                log.debug(
                        "fetchRecordProps pointQuery failed for key={}: {}",
                        rawKey,
                        e.getMessage());
            }
        }
        return null;
    }

    private static RecordPoint parsePointData(String json) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(json);
            String propsJson = node.path("properties").asText("");
            Map<String, String> props = Map.of();
            if (!propsJson.isEmpty()) {
                props =
                        mapper.readValue(
                                propsJson,
                                new com.fasterxml.jackson.core.type.TypeReference<
                                        Map<String, String>>() {});
            }
            String bodyB64 = node.path("body").asText("");
            String body = "";
            if (!bodyB64.isEmpty()) {
                body =
                        new String(
                                java.util.Base64.getDecoder().decode(bodyB64),
                                java.nio.charset.StandardCharsets.UTF_8);
            }
            return new RecordPoint(props, body);
        } catch (Exception e) {
            return new RecordPoint(Map.of(), "");
        }
    }

    /**
     * Extract candidate entity names from query text (used as seeds for graph recall). Rule: tokens
     * starting with uppercase (>=2 chars, letter-start, excluding stop words). This is a simplified
     * extraction for LoCoMo/English scenarios; can be replaced with NER later.
     */
    private static java.util.List<String> extractEntityCandidates(String query) {
        if (query == null || query.isBlank()) return java.util.List.of();
        java.util.List<String> result = new java.util.ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        java.util.Set<String> stop =
                java.util.Set.of(
                        "When", "What", "Where", "Who", "Why", "How", "Which", "Did", "Does", "Do",
                        "Is", "Are", "Was", "Were", "Has", "Have", "The");
        for (String token : query.split("[\\s\\p{Punct}]+")) {
            if (token.length() < 2) continue;
            char c0 = token.charAt(0);
            if (!Character.isLetter(c0) || !Character.isUpperCase(c0)) continue;
            if (stop.contains(token)) continue;
            if (seen.add(token)) result.add(token);
            if (result.size() >= 8) break;
        }
        return result;
    }

    /**
     * convert entryKey("rec:&lt;rawKey&gt;")convert to graph inverted indexin diaId
     * format(base64url(rawKey)). with graph rescue path opposite encoding direction: rescue decode
     * diaId→rawKey→entryKey, hereencode entryKey→rawKey→diaId withquery lookupEntitiesByDia.
     */
    private static String extractDiaIdFromEntryKey(String entryKey) {
        if (entryKey == null) return null;
        String rawKey = entryKey.startsWith("rec:") ? entryKey.substring(4) : entryKey;
        return java.util.Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(rawKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * generate query variant(does not depend on LLM). variant strategy(sorted by priority, take
     * first n ): 0. original query(alwaysfirst) 1. removequestion wordprefix("When did X go..." →
     * "X go...") 2. keyword extraction(removestop words) 3. entity focus(keep onlycapital-starting
     * entity name) 4. event keyword combination(removeentity, keep onlyverb+noun) 5. entity+event
     * combination 6. remove question words+prepositions
     */
    private static java.util.List<String> expandQuery(String query, int n) {
        if (query == null || query.isBlank() || n <= 1) return java.util.List.of(query);
        java.util.List<String> variants = new java.util.ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        variants.add(query);
        seen.add(query.toLowerCase());

        String stripped =
                query.replaceAll(
                                "^(?i)(when|what|where|who|how|why|which|did|does|do|is|are|was|were|can|could|would|will|should)\\s+",
                                "")
                        .trim();
        if (!stripped.isEmpty() && seen.add(stripped.toLowerCase())) {
            variants.add(stripped);
        }

        String keywords = extractKeywords(query);
        if (!keywords.isEmpty() && seen.add(keywords.toLowerCase())) {
            variants.add(keywords);
        }

        String entityFocused = extractEntityFocused(query);
        if (!entityFocused.isEmpty() && seen.add(entityFocused.toLowerCase())) {
            variants.add(entityFocused);
        }

        String eventKeywords = extractEventKeywords(query);
        if (!eventKeywords.isEmpty() && seen.add(eventKeywords.toLowerCase())) {
            variants.add(eventKeywords);
        }

        if (!entityFocused.isEmpty() && !eventKeywords.isEmpty()) {
            String combined = entityFocused + " " + eventKeywords;
            if (seen.add(combined.toLowerCase())) {
                variants.add(combined);
            }
        }

        String noStopwords = removeStopwords(query);
        if (!noStopwords.isEmpty() && seen.add(noStopwords.toLowerCase())) {
            variants.add(noStopwords);
        }

        return variants.size() > n ? variants.subList(0, n) : variants;
    }

    private static String extractKeywords(String query) {
        java.util.Set<String> stop =
                java.util.Set.of(
                        "when", "what", "where", "who", "how", "why", "which", "did", "does", "do",
                        "is", "are", "was", "were", "can", "could", "would", "will", "should",
                        "the", "a", "an", "to", "of", "in", "on", "at", "and", "or", "but", "for",
                        "with", "by", "that", "this");
        java.util.List<String> keywords = new java.util.ArrayList<>();
        for (String word : query.toLowerCase().split("\\s+")) {
            word = word.replaceAll("[^a-z0-9'-]", "");
            if (!word.isEmpty() && !stop.contains(word) && word.length() > 1) {
                keywords.add(word);
            }
        }
        return String.join(" ", keywords);
    }

    /**
     * Entity-focused variant: extract uppercase entity names (person names, place names, etc.),
     * used to focus recall on conversations involving relevant entities.
     */
    private static String extractEntityFocused(String query) {
        java.util.Set<String> stop =
                java.util.Set.of(
                        "When", "What", "Where", "Who", "How", "Why", "Which", "Did", "Does", "Do",
                        "Is", "Are", "Was", "Were", "The", "A", "An");
        java.util.List<String> entities = new java.util.ArrayList<>();
        for (String token : query.split("[\\s\\p{Punct}]+")) {
            if (token.length() < 2) continue;
            char c0 = token.charAt(0);
            if (!Character.isLetter(c0) || !Character.isUpperCase(c0)) continue;
            if (stop.contains(token)) continue;
            entities.add(token);
        }
        return String.join(" ", entities);
    }

    /**
     * Event keyword variant: remove entity names, keep only verbs+nouns, used for recall by event
     * semantics.
     */
    private static String extractEventKeywords(String query) {
        java.util.Set<String> stop =
                java.util.Set.of(
                        "when", "what", "where", "who", "how", "why", "which", "did", "does", "do",
                        "is", "are", "was", "were", "can", "could", "would", "will", "should",
                        "the", "a", "an", "to", "of", "in", "on", "at", "and", "or", "but", "for",
                        "with", "by", "that", "this");
        java.util.List<String> eventWords = new java.util.ArrayList<>();
        for (String token : query.split("[\\s\\p{Punct}]+")) {
            if (token.length() < 2) continue;
            char c0 = token.charAt(0);
            if (Character.isLetter(c0) && Character.isUpperCase(c0)) continue;
            String lower = token.toLowerCase();
            if (!stop.contains(lower) && lower.length() > 1) {
                eventWords.add(lower);
            }
        }
        return String.join(" ", eventWords);
    }

    /**
     * Remove question words and common prepositions/articles, retaining all content words
     * (including entity names).
     */
    private static String removeStopwords(String query) {
        java.util.Set<String> stop =
                java.util.Set.of(
                        "when", "what", "where", "who", "how", "why", "which", "did", "does", "do",
                        "is", "are", "was", "were", "can", "could", "would", "will", "should",
                        "the", "a", "an", "to", "of", "in", "on", "at", "and", "or", "but", "for",
                        "with", "by", "that", "this");
        java.util.List<String> kept = new java.util.ArrayList<>();
        for (String token : query.split("\\s+")) {
            String clean = token.replaceAll("[^a-zA-Z0-9'-]", "");
            if (clean.isEmpty()) continue;
            if (stop.contains(clean.toLowerCase())) continue;
            kept.add(clean);
        }
        return String.join(" ", kept);
    }

    private ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub selectStub(
            String entityId) {
        List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs =
                this.executorStubs;
        if (stubs.isEmpty()) {
            throw new IllegalStateException("No executor stubs available");
        }
        int idx = Math.abs(entityId.hashCode()) % stubs.size();
        return stubs.get(idx);
    }

    @Override
    public void getProvenance(
            AxiomProvenanceRequest request,
            StreamObserver<AxiomProvenanceResponse> responseObserver) {
        try {
            String viewId = request.getViewId();
            AxiomProvenanceResponse.Builder responseBuilder =
                    AxiomProvenanceResponse.newBuilder().setViewId(viewId);

            if (viewId.startsWith("rec:")) {
                String rawKey = viewId.substring(4);
                responseBuilder
                        .setRawKey(rawKey)
                        .addFullChain(
                                DerivationLink.newBuilder()
                                        .setFrom(rawKey)
                                        .setFromLayer("RAW")
                                        .setTo(viewId)
                                        .setToLayer("SEARCH_ENTRY")
                                        .build());
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Axiom getProvenance failed", e);
            responseObserver.onError(e);
        }
    }

    private float applyRankBias(String entryKey, float baseScore) {
        Double bias = rankBiases.get(entryKey);
        if (bias == null) return baseScore;
        return (float) (baseScore * (1.0 + bias));
    }

    private static final double TRAINED_RERANKER_SCALE =
            Double.parseDouble(System.getProperty("rawwise.reranker.trained.scale", "0.01"));

    /**
     * Min-max normalize a raw score to {@code [0, 1]}.
     *
     * <p>When all values in a channel are equal ({@code max == min}), returns {@code 1.0} to avoid
     * division by zero and to avoid penalizing candidates in a uniform-score channel.
     *
     * @param min the minimum value in the channel
     * @param max the maximum value in the channel
     * @param raw the raw score to normalize
     * @return normalized score in {@code [0, 1]}
     */
    private static double normalizeMinMax(double min, double max, double raw) {
        if (max == min) return 1.0;
        return (raw - min) / (max - min);
    }

    /**
     * Trained Reranker: applies hitId weights learned from feedback to search results. Positive
     * weight values (POSITIVE feedback) boost score, negative values (NEGATIVE feedback) reduce
     * score. Uses TRAINED_RERANKER_SCALE to avoid weight values (+/-0.15~0.25) overwhelming RRF
     * scores (~0.001-0.03).
     */
    private static List<Map.Entry<String, Double>> applyTrainedReranker(
            List<Map.Entry<String, Double>> sorted, Map<String, Double> weights) {
        List<Map.Entry<String, Double>> result = new ArrayList<>(sorted.size());
        int applied = 0;
        for (var entry : sorted) {
            String key = entry.getKey();
            double baseScore = entry.getValue();
            Double weight = weights.get(key);
            if (weight != null) {
                result.add(Map.entry(key, baseScore + weight * TRAINED_RERANKER_SCALE));
                applied++;
            } else {
                result.add(entry);
            }
        }
        result.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
        if (applied > 0) {
            log.info(
                    "Trained reranker applied: {} entries adjusted out of {} (scale={})",
                    applied,
                    sorted.size(),
                    TRAINED_RERANKER_SCALE);
        }
        return result;
    }

    /**
     * Cross-encoder rerank: uses the ONNX {@link DefaultRerankerModel} to re-score the top
     * candidates by directly evaluating (query, document) relevance.
     *
     * <p>Up to {@value #RERANKER_TOP_N} candidates are re-scored (equivalent to topK*3 for the
     * default topK=10). Candidates beyond this limit retain their original fused score and are
     * appended after the re-scored block.
     *
     * @param sorted candidates sorted by fused score (descending)
     * @param query the search query
     * @param vecResults vector search results (for document text extraction)
     * @param kwResponses keyword search responses (for document text extraction)
     * @param model the loaded cross-encoder model
     * @return re-sorted list with cross-encoder scores for the top block
     */
    private List<Map.Entry<String, Double>> applyCrossEncoderRerank(
            List<Map.Entry<String, Double>> sorted,
            String query,
            List<VectorSearchResult> vecResults,
            List<io.github.rawwise.server.KeywordSearchResponse> kwResponses,
            DefaultRerankerModel model) {
        int maxN = Math.min(sorted.size(), RERANKER_TOP_N);
        List<Map.Entry<String, Double>> toRerank = new ArrayList<>(sorted.subList(0, maxN));
        List<Map.Entry<String, Double>> remaining =
                new ArrayList<>(sorted.subList(maxN, sorted.size()));

        List<Map.Entry<String, Double>> rescored = new ArrayList<>(maxN);
        int scored = 0;
        for (Map.Entry<String, Double> entry : toRerank) {
            String docText = findSummaryForEntry(entry.getKey(), vecResults, kwResponses);
            float crossScore;
            try {
                crossScore = model.score(query, docText);
                scored++;
            } catch (Exception e) {
                log.debug(
                        "Cross-encoder scoring failed for entry={}, retaining original score: {}",
                        entry.getKey(),
                        e.getMessage());
                rescored.add(entry);
                continue;
            }
            rescored.add(Map.entry(entry.getKey(), (double) crossScore));
        }
        rescored.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        log.info(
                "Cross-encoder reranker applied: {} candidates re-scored (fallback={}, model={})",
                scored,
                model.isFallback(),
                model.modelIdentifier());

        List<Map.Entry<String, Double>> result = new ArrayList<>(rescored);
        result.addAll(remaining);
        return result;
    }

    private void loadPersistentRankBiases() {
        if (!java.nio.file.Files.exists(RANK_BIAS_PATH)) return;
        try (java.io.InputStream in = java.nio.file.Files.newInputStream(RANK_BIAS_PATH)) {
            java.util.Properties props = new java.util.Properties();
            props.load(in);
            for (String key : props.stringPropertyNames()) {
                try {
                    rankBiases.put(key, Double.parseDouble(props.getProperty(key)));
                } catch (NumberFormatException e) {
                    log.debug("Failed to parse number", e);
                }
            }
            log.info(
                    "Loaded {} persistent rankBias entries from {}",
                    rankBiases.size(),
                    RANK_BIAS_PATH);
        } catch (Exception e) {
            log.warn(
                    "Failed to load persistent rankBiases from {}: {}",
                    RANK_BIAS_PATH,
                    e.getMessage(),
                    e);
        }
    }

    private void savePersistentRankBiases() {
        try {
            java.nio.file.Files.createDirectories(RANK_BIAS_PATH.getParent());
            java.util.Properties props = new java.util.Properties();
            for (Map.Entry<String, Double> entry : rankBiases.entrySet()) {
                props.setProperty(entry.getKey(), Double.toString(entry.getValue()));
            }
            try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(RANK_BIAS_PATH)) {
                props.store(out, "RawWise persistent rank biases");
            }
        } catch (Exception e) {
            log.warn(
                    "Failed to save persistent rankBiases to {}: {}",
                    RANK_BIAS_PATH,
                    e.getMessage(),
                    e);
        }
    }

    /**
     * Session-Level Semantic Boost: scans SEMANTIC entries in the candidate pool (entryKey prefix
     * semantic:session:), parses their [source_entries:...] markers to get EPISODIC source
     * entryKeys of the same session, passes the SEMANTIC entry score as a boost signal to source
     * entries, then removes SEMANTIC entries from the candidate pool (they are boost signals, do
     * not occupy topK slots).
     *
     * <p>Boost uses a fixed absolute value (SEMANTIC_BOOST_VALUE), independent of reranker switch,
     * ensuring consistent boost effect in both RRF mode and reranker mode.
     */
    private static final double SEMANTIC_BOOST_VALUE =
            Double.parseDouble(System.getProperty("rawwise.axiom.semantic.boost-value", "0.01"));

    private static final String SEMANTIC_ENTRY_PREFIX = "semantic:session:";
    private static final java.util.regex.Pattern SOURCE_ENTRIES_PATTERN =
            java.util.regex.Pattern.compile("\\[source_entries:([^\\]]+)\\]");

    private List<Map.Entry<String, Double>> applySemanticBoost(
            List<Map.Entry<String, Double>> sorted,
            List<VectorSearchResult> vecResults,
            List<io.github.rawwise.server.KeywordSearchResponse> kwResponses,
            String tableId) {
        if (sorted.isEmpty()) return sorted;

        // Only boost source entries of SEMANTIC entries that appear in the candidate pool
        // This way, only when the query matches a session SEMANTIC summary are that session
        // EPISODIC entries boosted
        java.util.Set<String> matchedSemanticKeys = new java.util.HashSet<>();
        for (var e : sorted) {
            if (e.getKey().startsWith(SEMANTIC_ENTRY_PREFIX)) {
                matchedSemanticKeys.add(e.getKey());
            }
        }

        if (matchedSemanticKeys.isEmpty()) {
            // No SEMANTIC entry entered the candidate pool, no boost needed
            return sorted;
        }

        // Read source_entries mappings of matched SEMANTIC entries from metastore
        Map<String, Double> boostMap = new HashMap<>();
        try {
            io.github.rawwise.metastore.Metastore ms = metastoreRef;
            if (ms != null) {
                int scanCount = 0;
                for (String semKey : matchedSemanticKeys) {
                    byte[] metaKey =
                            ("semantic_sources:" + semKey)
                                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    byte[] val = ms.get(metaKey).orElse(null);
                    if (val == null) continue;
                    scanCount++;
                    try {
                        com.fasterxml.jackson.databind.ObjectMapper mapper =
                                new com.fasterxml.jackson.databind.ObjectMapper();
                        List<String> sources =
                                mapper.readValue(
                                        val,
                                        mapper.getTypeFactory()
                                                .constructCollectionType(List.class, String.class));
                        for (String src : sources) {
                            boostMap.merge(src.trim(), SEMANTIC_BOOST_VALUE, Double::sum);
                        }
                    } catch (Exception e) {
                        log.debug(
                                "Semantic boost: failed to parse sources for {}: {}",
                                semKey,
                                e.getMessage());
                    }
                }
                log.info(
                        "Semantic boost: {} matched SEMANTIC entries, {} source mappings",
                        matchedSemanticKeys.size(),
                        scanCount);
            }
        } catch (Exception e) {
            log.warn("Semantic boost: metastore lookup failed: {}", e.getMessage(), e);
        }

        if (boostMap.isEmpty()) {
            // No source mappings, only remove SEMANTIC entries
            List<Map.Entry<String, Double>> result = new ArrayList<>(sorted.size());
            for (var e : sorted) {
                if (!e.getKey().startsWith(SEMANTIC_ENTRY_PREFIX)) {
                    result.add(e);
                }
            }
            return result;
        }

        // Apply boost to EPISODIC entries in sorted, while removing SEMANTIC entries
        int boosted = 0;
        List<Map.Entry<String, Double>> result = new ArrayList<>(sorted.size());
        for (var e : sorted) {
            if (e.getKey().startsWith(SEMANTIC_ENTRY_PREFIX)) {
                continue; // Remove SEMANTIC entries, do not occupy topK slots
            }
            Double boost = boostMap.get(e.getKey());
            if (boost != null) {
                result.add(Map.entry(e.getKey(), e.getValue() + boost));
                boosted++;
            } else {
                result.add(e);
            }
        }
        if (boosted > 0) {
            log.info(
                    "Semantic boost applied: {} entries boosted out of {}", boosted, result.size());
        }
        return result;
    }

    private static final boolean FEEDBACK_LLM_ENABLED =
            Boolean.parseBoolean(System.getProperty("rawwise.axiom.feedback.llm.enabled", "false"));
    private static final String FEEDBACK_LLM_ENDPOINT =
            System.getProperty("rawwise.axiom.feedback.llm.endpoint", "");
    private static final String FEEDBACK_LLM_API_KEY =
            System.getProperty("rawwise.axiom.feedback.llm.api-key", "");
    private static final String FEEDBACK_LLM_MODEL =
            System.getProperty("rawwise.axiom.feedback.llm.model", "gpt-4o-mini");
    private static final java.net.http.HttpClient FEEDBACK_HTTP_CLIENT =
            java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(5))
                    .build();

    private double resolveAnnotationBias(String type, String value, double decayWeight) {
        double val;
        try {
            val = Double.parseDouble(value);
        } catch (NumberFormatException e) {
            if (FEEDBACK_LLM_ENABLED && !FEEDBACK_LLM_ENDPOINT.isEmpty()) {
                double llmBias = resolveAnnotationBiasWithLlm(type, value, decayWeight);
                if (llmBias != 0.0) return llmBias;
            }
            return 0.0;
        }
        double rawBias;
        FeedbackAnnotation.AnnotationType resolvedType;
        try {
            resolvedType = FeedbackAnnotation.AnnotationType.valueOf(type);
        } catch (IllegalArgumentException e) {
            resolvedType =
                    switch (type.toUpperCase()) {
                        case "NEGATIVE", "POSITIVE" ->
                                FeedbackAnnotation.AnnotationType.QUALITY_SCORE;
                        case "PREFERENCE" -> FeedbackAnnotation.AnnotationType.PREFERENCE;
                        default -> FeedbackAnnotation.AnnotationType.CORRECTION;
                    };
        }
        rawBias =
                switch (resolvedType) {
                    case PREFERENCE -> Math.tanh(val / 10.0) * 0.5;
                    case QUALITY_SCORE -> Math.tanh(val / 10.0) * 0.3;
                    case CORRECTION -> Math.tanh(val / 20.0) * 0.3;
                    case DECISION_ADOPTED -> 0.0;
                };
        return rawBias * decayWeight;
    }

    private double resolveAnnotationBiasWithLlm(String type, String freeText, double decayWeight) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.node.ObjectNode requestBody = mapper.createObjectNode();
            requestBody.put("model", FEEDBACK_LLM_MODEL);
            com.fasterxml.jackson.databind.node.ArrayNode messages =
                    requestBody.putArray("messages");
            String prompt =
                    "A user provided feedback of type '"
                            + type
                            + "' with free text: \""
                            + freeText
                            + "\". "
                            + "Rate the severity of this feedback on a scale of -10 (very negative) to +10 (very positive). "
                            + "Output ONLY a number.";
            messages.add(mapper.createObjectNode().put("role", "user").put("content", prompt));
            requestBody.put("max_tokens", 10);
            requestBody.put("temperature", 0);

            java.net.http.HttpRequest httpRequest =
                    java.net.http.HttpRequest.newBuilder()
                            .uri(java.net.URI.create(FEEDBACK_LLM_ENDPOINT))
                            .header("Content-Type", "application/json")
                            .header("Authorization", "Bearer " + FEEDBACK_LLM_API_KEY)
                            .POST(
                                    java.net.http.HttpRequest.BodyPublishers.ofString(
                                            mapper.writeValueAsString(requestBody)))
                            .timeout(java.time.Duration.ofSeconds(10))
                            .build();

            java.net.http.HttpResponse<String> response =
                    FEEDBACK_HTTP_CLIENT.send(
                            httpRequest, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                log.warn(
                        "Feedback LLM API returned status={}: {}",
                        response.statusCode(),
                        response.body());
                return 0.0;
            }
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(response.body());
            String content =
                    root.path("choices").path(0).path("message").path("content").asText("").trim();
            double llmVal = Double.parseDouble(content);
            double bias = Math.tanh(llmVal / 10.0) * 0.5 * decayWeight;
            log.info(
                    "Feedback LLM: free text '{}' -> severity={} bias={} type={}",
                    freeText,
                    llmVal,
                    bias,
                    type);
            return bias;
        } catch (Exception e) {
            log.warn("Feedback LLM failed for free text '{}': {}", freeText, e.getMessage(), e);
            return 0.0;
        }
    }

    private static ByteString floatArrayToBytes(float[] arr) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(arr.length * 4);
        for (float v : arr) {
            buf.putFloat(v);
        }
        return ByteString.copyFrom(buf.array());
    }

    private List<Map.Entry<String, Double>> llmRerank(
            List<Map.Entry<String, Double>> sorted,
            String query,
            List<VectorSearchResult> vecResults,
            java.util.List<io.github.rawwise.server.KeywordSearchResponse> kwResponses,
            int topK) {
        int rerankN = Math.min(sorted.size(), RERANKER_TOP_N);
        List<Map.Entry<String, Double>> toRerank = new ArrayList<>(sorted.subList(0, rerankN));
        List<Map.Entry<String, Double>> remaining =
                new ArrayList<>(sorted.subList(rerankN, sorted.size()));

        double maxRrf = toRerank.stream().mapToDouble(Map.Entry::getValue).max().orElse(1.0);
        double minRrf = toRerank.stream().mapToDouble(Map.Entry::getValue).min().orElse(0.0);
        double rrfRange = Math.max(maxRrf - minRrf, 0.001);

        List<java.util.concurrent.CompletableFuture<Map.Entry<String, Double>>> futures =
                new ArrayList<>();
        for (Map.Entry<String, Double> entry : toRerank) {
            String summary = findSummaryForEntry(entry.getKey(), vecResults, kwResponses);
            double rrfScore = entry.getValue();
            double rrfNormalized = (rrfScore - minRrf) / rrfRange;
            futures.add(
                    java.util.concurrent.CompletableFuture.supplyAsync(
                            () -> {
                                double crossScore;
                                try {
                                    crossScore = callLlmForScoring(query, summary);
                                } catch (Exception e) {
                                    log.debug(
                                            "Cross-encoder scoring failed for entry={}, using RRF score: {}",
                                            entry.getKey(),
                                            e.getMessage());
                                    crossScore = rrfNormalized;
                                }
                                double fused =
                                        CROSSENCODER_ALPHA
                                                        * Math.max(0.0, Math.min(1.0, crossScore))
                                                + (1.0 - CROSSENCODER_ALPHA) * rrfNormalized;
                                return Map.entry(entry.getKey(), fused);
                            },
                            RERANKER_EXECUTOR));
        }

        List<Map.Entry<String, Double>> rescored = new ArrayList<>();
        int futureIdx = 0;
        for (java.util.concurrent.CompletableFuture<Map.Entry<String, Double>> f : futures) {
            try {
                rescored.add(f.get(15, TimeUnit.SECONDS));
            } catch (Exception e) {
                String key =
                        futureIdx < toRerank.size() ? toRerank.get(futureIdx).getKey() : "unknown";
                log.warn("Rerank future failed for entry={}: {}", key, e.getMessage(), e);
            }
            futureIdx++;
        }
        rescored.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        List<Map.Entry<String, Double>> result = new ArrayList<>(rescored);
        result.addAll(remaining);
        return result;
    }

    private static String findSummaryForEntry(
            String entryKey,
            List<VectorSearchResult> vecResults,
            java.util.List<io.github.rawwise.server.KeywordSearchResponse> kwResponses) {
        for (VectorSearchResult r : vecResults) {
            if (r.entryKey().equals(entryKey) && !r.l0Abstract().isEmpty()) {
                return r.l0Abstract();
            }
        }
        for (io.github.rawwise.server.KeywordSearchResponse resp : kwResponses) {
            for (io.github.rawwise.server.KeywordSearchHit hit : resp.getHitsList()) {
                if (hit.getDocId().equals(entryKey)) {
                    return hit.getSummary();
                }
            }
        }
        return "";
    }

    private static String rewriteQueryWithLlm(String query)
            throws IOException, InterruptedException {
        com.fasterxml.jackson.databind.ObjectMapper mapper =
                new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.node.ObjectNode requestBody = mapper.createObjectNode();
        requestBody.put("model", QUERY_REWRITE_MODEL);
        com.fasterxml.jackson.databind.node.ArrayNode messages = requestBody.putArray("messages");
        String prompt =
                """
                You are a query rewriter for a conversational document retrieval system.
                The user asks a question about a long conversation. The question may use abstract
                meta-language (e.g. "research", "identity", "relationship status", "destress") that
                doesn't appear in the actual conversation.

                Your job: rewrite the question into the concrete, everyday language that the speakers
                would actually use in the conversation. Keep it as a natural phrase someone might say
                in dialogue.

                Rules:
                - Replace abstract words with concrete alternatives (e.g. "research" -> "looked into", "identity" -> "transgender", "relationship status" -> "single or dating", "destress" -> "relax or unwind")
                - Keep entity names (person names, places, organizations) intact
                - Keep it concise: 3-8 words
                - Output ONLY the rewritten query, nothing else. No quotes, no explanation.

                Q: %s ->"""
                        .formatted(query);
        messages.add(mapper.createObjectNode().put("role", "user").put("content", prompt));
        requestBody.put("max_tokens", 50);
        requestBody.put("temperature", 0);

        java.net.http.HttpRequest httpRequest =
                java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create(QUERY_REWRITE_ENDPOINT))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + QUERY_REWRITE_API_KEY)
                        .POST(
                                java.net.http.HttpRequest.BodyPublishers.ofString(
                                        mapper.writeValueAsString(requestBody)))
                        .timeout(java.time.Duration.ofMillis(QUERY_REWRITE_TIMEOUT_MS))
                        .build();

        java.net.http.HttpResponse<String> response =
                RERANKER_HTTP_CLIENT.send(
                        httpRequest, java.net.http.HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Query rewrite API returned status " + response.statusCode());
        }
        com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(response.body());
        String content =
                root.path("choices").path(0).path("message").path("content").asText("").trim();
        return content.isEmpty() ? query : content;
    }

    private static double callLlmForScoring(String query, String passage)
            throws IOException, InterruptedException {
        if (passage == null || passage.isBlank()) {
            return 0.0;
        }
        String truncatedPassage =
                passage.length() > 800 ? passage.substring(0, 800) + "..." : passage;
        String truncatedQuery = query.length() > 200 ? query.substring(0, 200) + "..." : query;

        com.fasterxml.jackson.databind.ObjectMapper mapper =
                new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.node.ObjectNode requestBody = mapper.createObjectNode();
        requestBody.put("model", RERANKER_MODEL);
        com.fasterxml.jackson.databind.node.ArrayNode messages = requestBody.putArray("messages");
        String prompt =
                """
            Score the relevance of this passage to the query on a 0-10 scale.

            Scoring rubric:
            - 10: The passage contains the exact answer to the query, including all key details (names, events, facts).
            - 7-9: The passage covers most key details relevant to the query.
            - 4-6: The passage is partially relevant, touches on the topic but lacks specific details.
            - 1-3: The passage is barely related, only shares keywords or vague theme.
            - 0: The passage is completely unrelated to the query.

            Output ONLY the number (0-10), nothing else. Do not explain.

            Query: %s

            Passage: %s

            Relevance score (0-10):"""
                        .formatted(truncatedQuery, truncatedPassage);
        messages.add(mapper.createObjectNode().put("role", "user").put("content", prompt));
        requestBody.put("max_tokens", 5);
        requestBody.put("temperature", 0);

        java.net.http.HttpRequest httpRequest =
                java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create(RERANKER_ENDPOINT))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + RERANKER_API_KEY)
                        .POST(
                                java.net.http.HttpRequest.BodyPublishers.ofString(
                                        mapper.writeValueAsString(requestBody)))
                        .timeout(java.time.Duration.ofSeconds(10))
                        .build();

        java.net.http.HttpResponse<String> response =
                RERANKER_HTTP_CLIENT.send(
                        httpRequest, java.net.http.HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("LLM reranker API returned status " + response.statusCode());
        }
        com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(response.body());
        String content =
                root.path("choices").path(0).path("message").path("content").asText("").trim();
        try {
            double score = Double.parseDouble(content);
            return Math.max(0.0, Math.min(1.0, score / 10.0));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
