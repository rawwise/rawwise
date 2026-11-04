package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.axiom.memory.NoOpSummarizationService;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.InMemoryVectorIndex;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.governance.context.DefaultContextManager;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * GraphEngine multi-hop traversal microbenchmark for OpenClaw benchmark plan P0.8.
 *
 * <p>Constructs an in-memory graph with N nodes (each having ~5 outgoing edges to random other
 * nodes) and measures:
 *
 * <ul>
 *   <li>Build phase throughput (addNode + addEdge)
 *   <li>Single-hop traversal latency p50/p95/p99 from 100 random starts
 *   <li>2-hop traversal latency p50/p95/p99 from 100 random starts
 * </ul>
 *
 * <p>Backend: pure in-memory (InMemoryMetastore + InMemoryKnowledgeStore + InMemoryVectorIndex). No
 * RawWise process required.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * # Run all three N (1k/10k/100k) and emit JSON to stdout
 * java ... io.github.rawwise.axiom.memory.semantic.GraphTraversalMicrobench
 *
 * # Run a specific N only
 * java ... io.github.rawwise.axiom.memory.semantic.GraphTraversalMicrobench 1000
 *
 * # Write to a file (also still prints to stdout)
 * java ... io.github.rawwise.axiom.memory.semantic.GraphTraversalMicrobench all benchmark/openclaw/baseline/graph-traversal.json
 * }</pre>
 */
@Slf4j
public class GraphTraversalMicrobench {

    static final long RANDOM_SEED = 42L;
    static final int EDGES_PER_NODE = 5;
    static final int QUERY_SAMPLES = 100;
    static final String EDGE_LABEL = "related_to";
    static final String NODE_TYPE = "entity";
    static final int NEIGHBOR_LIMIT_1HOP = EDGES_PER_NODE * 2;
    static final int NEIGHBOR_LIMIT_2HOP = EDGES_PER_NODE * EDGES_PER_NODE * 2;

    public static void main(String[] args) throws Exception {
        List<Integer> sizes;
        Path outputFile = null;

        if (args.length == 0 || "all".equalsIgnoreCase(args[0])) {
            sizes = List.of(1_000, 10_000, 100_000);
        } else {
            sizes = new ArrayList<>();
            for (String token : args[0].split(",")) {
                sizes.add(Integer.parseInt(token.trim()));
            }
        }
        if (args.length >= 2) {
            outputFile = Path.of(args[1]);
        }

        List<Result> results = new ArrayList<>();
        for (int n : sizes) {
            results.add(runOne(n));
        }

        String json = toJson(results);
        if (outputFile != null) {
            if (outputFile.getParent() != null) {
                Files.createDirectories(outputFile.getParent());
            }
            Files.writeString(outputFile, json);
            System.err.println("Wrote " + outputFile.toAbsolutePath());
        }
        System.out.println(json);
        System.out.flush();
    }

    static Result runOne(int n) throws Exception {
        log.info("=== Running N={} ===", n);
        Metastore metastore = new InMemoryMetastore();
        KnowledgeStore knowledgeStore = new InMemoryKnowledgeStore();
        ContextManager contextManager = new DefaultContextManager(metastore, 1, 1);
        EmbeddingProvider embeddingProvider = text -> new float[1];
        VectorIndex vectorIndex = new InMemoryVectorIndex(embeddingProvider);
        GraphEngineConfig config =
                new GraphEngineConfig(
                        metastore,
                        knowledgeStore,
                        contextManager,
                        new NoOpSummarizationService(),
                        vectorIndex,
                        embeddingProvider,
                        new NoOpKnowledgeExtractor(),
                        null);
        GraphEngine engine = new DefaultGraphEngine(config);

        GraphSchema schema =
                engine.createGraph(
                        "agent-bench",
                        "bench-" + n,
                        "P0.8 microbench",
                        List.of(NODE_TYPE),
                        List.of(EDGE_LABEL),
                        Map.of("scale", Integer.toString(n)));
        String graphId = schema.graphId();
        log.info(
                "Created graph={} (maxNodes={}, maxEdges={})",
                graphId,
                schema.maxNodes(),
                schema.maxEdges());

        // ---------- Build phase: nodes ----------
        Random rng = new Random(RANDOM_SEED);
        String[] nodeIds = new String[n];

        long buildStart = System.nanoTime();
        for (int i = 0; i < n; i++) {
            GraphNode node =
                    engine.addNode(
                            graphId,
                            NODE_TYPE,
                            Map.of("logical_id", "n-" + i, "idx", Integer.toString(i)));
            nodeIds[i] = node.nodeId();
            if ((i + 1) % 10_000 == 0) {
                log.info("  built {} nodes", i + 1);
            }
        }
        long nodeBuildEnd = System.nanoTime();

        // ---------- Build phase: edges ----------
        long totalEdges = 0;
        for (int i = 0; i < n; i++) {
            Set<Integer> picked = new HashSet<>(EDGES_PER_NODE * 2);
            picked.add(i);
            for (int e = 0; e < EDGES_PER_NODE; e++) {
                int target;
                int spin = 0;
                do {
                    target = rng.nextInt(n);
                    spin++;
                    if (spin > 32) break;
                } while (!picked.add(target));
                if (target == i) continue;
                engine.addEdge(graphId, nodeIds[i], EDGE_LABEL, nodeIds[target], Map.of(), 1.0f);
                totalEdges++;
            }
            if ((i + 1) % 10_000 == 0) {
                log.info("  built edges for {} nodes", i + 1);
            }
        }
        long edgeBuildEnd = System.nanoTime();

        long buildTotalNs = edgeBuildEnd - buildStart;
        long nodeBuildNs = nodeBuildEnd - buildStart;
        long edgeBuildNs = edgeBuildEnd - nodeBuildEnd;

        // ---------- Single-hop phase ----------
        Random qRng = new Random(RANDOM_SEED + 1);
        long[] hop1Latencies = new long[QUERY_SAMPLES];
        long hop1NeighborSum = 0;
        for (int q = 0; q < QUERY_SAMPLES; q++) {
            String start = nodeIds[qRng.nextInt(n)];
            long t0 = System.nanoTime();
            List<GraphNode> hits =
                    engine.neighbors(
                            graphId,
                            start,
                            EDGE_LABEL,
                            1,
                            NEIGHBOR_LIMIT_1HOP,
                            TraversalDirection.OUT);
            hop1Latencies[q] = System.nanoTime() - t0;
            hop1NeighborSum += hits.size();
        }

        // ---------- 2-hop phase ----------
        long[] hop2Latencies = new long[QUERY_SAMPLES];
        long hop2NeighborSum = 0;
        for (int q = 0; q < QUERY_SAMPLES; q++) {
            String start = nodeIds[qRng.nextInt(n)];
            long t0 = System.nanoTime();
            List<GraphNode> hits =
                    engine.neighbors(
                            graphId,
                            start,
                            EDGE_LABEL,
                            2,
                            NEIGHBOR_LIMIT_2HOP,
                            TraversalDirection.OUT);
            hop2Latencies[q] = System.nanoTime() - t0;
            hop2NeighborSum += hits.size();
        }

        Result r = new Result();
        r.n = n;
        r.totalEdges = totalEdges;
        r.buildTotalMs = buildTotalNs / 1_000_000.0;
        r.nodeBuildMs = nodeBuildNs / 1_000_000.0;
        r.edgeBuildMs = edgeBuildNs / 1_000_000.0;
        r.buildNodesPerSec = n / (nodeBuildNs / 1_000_000_000.0);
        r.buildEdgesPerSec = totalEdges / (edgeBuildNs / 1_000_000_000.0);
        r.hop1 = summarize(hop1Latencies);
        r.hop1.avgNeighbors = hop1NeighborSum / (double) QUERY_SAMPLES;
        r.hop2 = summarize(hop2Latencies);
        r.hop2.avgNeighbors = hop2NeighborSum / (double) QUERY_SAMPLES;

        log.info(
                "N={} build={}ms (nodes={}/s, edges={}/s)",
                n,
                fmt(r.buildTotalMs),
                fmt(r.buildNodesPerSec),
                fmt(r.buildEdgesPerSec));
        log.info(
                "  hop1 p50={}ms p95={}ms p99={}ms avgN={}",
                fmt(r.hop1.p50Ms),
                fmt(r.hop1.p95Ms),
                fmt(r.hop1.p99Ms),
                fmt(r.hop1.avgNeighbors));
        log.info(
                "  hop2 p50={}ms p95={}ms p99={}ms avgN={}",
                fmt(r.hop2.p50Ms),
                fmt(r.hop2.p95Ms),
                fmt(r.hop2.p99Ms),
                fmt(r.hop2.avgNeighbors));

        if (contextManager instanceof DefaultContextManager dcm) {
            dcm.close();
        }
        knowledgeStore.close();
        metastore.close();
        return r;
    }

    static LatencyStats summarize(long[] latenciesNs) {
        long[] copy = latenciesNs.clone();
        Arrays.sort(copy);
        LatencyStats s = new LatencyStats();
        s.count = copy.length;
        s.minMs = copy[0] / 1_000_000.0;
        s.maxMs = copy[copy.length - 1] / 1_000_000.0;
        long sum = 0;
        for (long v : copy) sum += v;
        s.avgMs = (sum / (double) copy.length) / 1_000_000.0;
        s.p50Ms = pickPercentile(copy, 0.50) / 1_000_000.0;
        s.p95Ms = pickPercentile(copy, 0.95) / 1_000_000.0;
        s.p99Ms = pickPercentile(copy, 0.99) / 1_000_000.0;
        return s;
    }

    static long pickPercentile(long[] sorted, double p) {
        if (sorted.length == 0) return 0;
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        if (idx < 0) idx = 0;
        if (idx >= sorted.length) idx = sorted.length - 1;
        return sorted[idx];
    }

    static String fmt(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return String.valueOf(v);
        if (Math.abs(v) >= 1000) return String.format("%.0f", v);
        if (Math.abs(v) >= 10) return String.format("%.2f", v);
        return String.format("%.3f", v);
    }

    static String toJson(List<Result> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"benchmark\": \"graph-traversal-microbench\",\n");
        sb.append("  \"phase\": \"P0.8\",\n");
        sb.append(
                "  \"backend\": \"in-memory (InMemoryMetastore + InMemoryKnowledgeStore + InMemoryVectorIndex)\",\n");
        sb.append("  \"random_seed\": ").append(RANDOM_SEED).append(",\n");
        sb.append("  \"edges_per_node\": ").append(EDGES_PER_NODE).append(",\n");
        sb.append("  \"query_samples\": ").append(QUERY_SAMPLES).append(",\n");
        sb.append("  \"edge_label\": \"").append(EDGE_LABEL).append("\",\n");
        sb.append("  \"results\": [\n");
        for (int i = 0; i < results.size(); i++) {
            sb.append("    ").append(results.get(i).toJson());
            if (i < results.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");
        return sb.toString();
    }

    static class Result {
        int n;
        long totalEdges;
        double buildTotalMs;
        double nodeBuildMs;
        double edgeBuildMs;
        double buildNodesPerSec;
        double buildEdgesPerSec;
        LatencyStats hop1;
        LatencyStats hop2;

        String toJson() {
            return "{\n"
                    + "      \"n\": "
                    + n
                    + ",\n"
                    + "      \"total_edges\": "
                    + totalEdges
                    + ",\n"
                    + "      \"build_total_ms\": "
                    + fmt(buildTotalMs)
                    + ",\n"
                    + "      \"node_build_ms\": "
                    + fmt(nodeBuildMs)
                    + ",\n"
                    + "      \"edge_build_ms\": "
                    + fmt(edgeBuildMs)
                    + ",\n"
                    + "      \"build_nodes_per_sec\": "
                    + fmt(buildNodesPerSec)
                    + ",\n"
                    + "      \"build_edges_per_sec\": "
                    + fmt(buildEdgesPerSec)
                    + ",\n"
                    + "      \"hop1\": "
                    + hop1.toJson()
                    + ",\n"
                    + "      \"hop2\": "
                    + hop2.toJson()
                    + "\n"
                    + "    }";
        }
    }

    static class LatencyStats {
        int count;
        double minMs;
        double maxMs;
        double avgMs;
        double p50Ms;
        double p95Ms;
        double p99Ms;
        double avgNeighbors;

        String toJson() {
            return "{"
                    + "\"count\":"
                    + count
                    + ",\"min_ms\":"
                    + fmt(minMs)
                    + ",\"max_ms\":"
                    + fmt(maxMs)
                    + ",\"avg_ms\":"
                    + fmt(avgMs)
                    + ",\"p50_ms\":"
                    + fmt(p50Ms)
                    + ",\"p95_ms\":"
                    + fmt(p95Ms)
                    + ",\"p99_ms\":"
                    + fmt(p99Ms)
                    + ",\"avg_neighbors\":"
                    + fmt(avgNeighbors)
                    + "}";
        }
    }

    private GraphTraversalMicrobench() {}
}
