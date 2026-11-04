package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * @deprecated For testing only. Production code should use {@link Keyword Search Coordinator
 *     Adapter}.
 */
@Deprecated
public class SimpleStubKeywordSearchCoordinator implements KeywordSearchCoordinator {

    private final Map<String, String> documents;

    public SimpleStubKeywordSearchCoordinator() {
        this(Map.of());
    }

    public SimpleStubKeywordSearchCoordinator(Map<String, String> documents) {
        this.documents = documents == null ? Map.of() : Map.copyOf(documents);
    }

    @Override
    public List<RrfFusion.ScoredHit> search(String tableId, List<String> queries, int topK) {
        if (queries == null || queries.isEmpty() || documents.isEmpty()) return List.of();
        Set<String> queryTokens = tokenize(String.join(" ", queries));
        if (queryTokens.isEmpty()) return List.of();

        List<RrfFusion.ScoredHit> hits = new ArrayList<>();
        for (Map.Entry<String, String> entry : documents.entrySet()) {
            String docId = entry.getKey();
            Set<String> docTokens = tokenize(entry.getValue());
            int matchCount = 0;
            for (String qt : queryTokens) {
                if (docTokens.contains(qt)) matchCount++;
            }
            if (matchCount == 0) continue;
            double score = (double) matchCount / queryTokens.size();
            Map<String, Object> meta = new HashMap<>();
            meta.put("retrievalPath", "KEYWORD");
            meta.put("matchCount", matchCount);
            hits.add(new RrfFusion.ScoredHit(docId, score, meta));
        }
        hits.sort(Comparator.comparingDouble(RrfFusion.ScoredHit::score).reversed());
        int limit = Math.min(hits.size(), Math.max(topK, 1));
        return new ArrayList<>(hits.subList(0, limit));
    }

    private static Set<String> tokenize(String text) {
        if (text == null || text.isBlank()) return Set.of();
        Set<String> tokens = new HashSet<>();
        for (String token : text.toLowerCase(Locale.ROOT).split("[\\s\\p{Punct}]+")) {
            if (!token.isBlank()) tokens.add(token);
        }
        return tokens;
    }
}
