package io.github.rawwise.axiom.pipeline.cognition;

import java.util.List;
import java.util.Map;

/** 认知状态 — 探索进度快照，持久化到 KnowledgeStore 以支持崩溃恢复。 */
public record CognitiveState(
        String sceneHash,
        int exploreCount,
        int exploitCount,
        int consecutiveNoImprovement,
        double bestScore,
        String bestStrategyId,
        List<String> decisionHistory,
        Map<String, Double> latestMetrics,
        long lastUpdated) {
    public static CognitiveState initial(String sceneHash, Map<String, Double> initialMetrics) {
        return new CognitiveState(
                sceneHash,
                0,
                0,
                0,
                -Double.MAX_VALUE,
                null,
                List.of(),
                initialMetrics != null ? Map.copyOf(initialMetrics) : Map.of(),
                System.currentTimeMillis());
    }

    public CognitiveState recordExplore(String decisionId, double score, String strategyId) {
        var newHistory = new java.util.ArrayList<>(decisionHistory);
        newHistory.add(decisionId);
        var newBest = score > bestScore;
        return new CognitiveState(
                sceneHash,
                exploreCount + 1,
                exploitCount,
                newBest ? 0 : consecutiveNoImprovement + 1,
                newBest ? score : bestScore,
                newBest ? strategyId : bestStrategyId,
                List.copyOf(newHistory),
                latestMetrics,
                System.currentTimeMillis());
    }

    public CognitiveState recordExploit(String decisionId) {
        var newHistory = new java.util.ArrayList<>(decisionHistory);
        newHistory.add(decisionId);
        return new CognitiveState(
                sceneHash,
                exploreCount,
                exploitCount + 1,
                consecutiveNoImprovement,
                bestScore,
                bestStrategyId,
                List.copyOf(newHistory),
                latestMetrics,
                System.currentTimeMillis());
    }

    public CognitiveState updateMetrics(Map<String, Double> metrics) {
        return new CognitiveState(
                sceneHash,
                exploreCount,
                exploitCount,
                consecutiveNoImprovement,
                bestScore,
                bestStrategyId,
                decisionHistory,
                Map.copyOf(metrics),
                System.currentTimeMillis());
    }
}
