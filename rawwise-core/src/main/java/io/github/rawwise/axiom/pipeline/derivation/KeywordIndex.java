package io.github.rawwise.axiom.pipeline.derivation;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple BM25 keyword index for hybrid search. Stores term→document frequency and supports weighted
 * scoring. Chinese text should be pre-segmented before passing to {@link #index}.
 */
public class KeywordIndex {

    private static final double K1 = 1.2;
    private static final double B = 0.75;

    // term → doc Id → term frequency in that doc
    private final Map<String, Map<String, Integer>> termDocFreq = new ConcurrentHashMap<>();
    // doc Id → document length (word count)
    private final Map<String, Integer> docLengths = new ConcurrentHashMap<>();
    // doc Id → stored summary text for retrieval
    private final Map<String, String> docSummaries = new ConcurrentHashMap<>();
    private int totalDocs;

    /** Average document length across all indexed docs. */
    public double avgDocLength() {
        if (docLengths.isEmpty()) return 1.0;
        return docLengths.values().stream().mapToInt(Integer::intValue).average().orElse(1.0);
    }

    /** Index a document. Text should be pre-tokenized (space-separated words). */
    public void index(String namespace, String docId, String tokenizedText, String summary) {
        String[] words = tokenizedText.split("\\s+");
        if (words.length == 0) return;

        String key = namespace + ":" + docId;
        docLengths.put(key, words.length);
        docSummaries.put(key, summary);
        totalDocs++;

        Map<String, Integer> localFreq = new HashMap<>();
        for (String w : words) {
            w = w.toLowerCase();
            if (w.isEmpty()) continue;
            localFreq.merge(w, 1, Integer::sum);
        }

        for (var entry : localFreq.entrySet()) {
            termDocFreq
                    .computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
                    .put(key, entry.getValue());
        }
    }

    /** Search with BM25 scoring. Returns (doc Id, score) pairs sorted by score descending. */
    public List<KeywordHit> search(String namespace, String query, int topK) {
        String[] queryWords = query.toLowerCase().split("\\s+");
        if (queryWords.length == 0) return List.of();

        String prefix = namespace + ":";
        double avgdl = avgDocLength();

        Map<String, Double> scores = new HashMap<>();
        for (String qw : queryWords) {
            Map<String, Integer> postings = termDocFreq.get(qw);
            if (postings == null) continue;

            int df = postings.size();
            double idf = Math.log((totalDocs - df + 0.5) / (df + 0.5) + 1.0);

            for (var entry : postings.entrySet()) {
                String docId = entry.getKey();
                if (!docId.startsWith(prefix)) continue;

                int tf = entry.getValue();
                int docLen = docLengths.getOrDefault(docId, 1);
                double score = idf * (tf * (K1 + 1)) / (tf + K1 * (1 - B + B * docLen / avgdl));
                scores.merge(docId, score, Double::sum);
            }
        }

        return scores.entrySet().stream()
                .map(
                        e ->
                                new KeywordHit(
                                        stripPrefix(e.getKey(), prefix),
                                        e.getValue(),
                                        docSummaries.getOrDefault(e.getKey(), "")))
                .sorted(Comparator.comparingDouble(KeywordHit::score).reversed())
                .limit(topK)
                .toList();
    }

    public String getSummary(String namespace, String docId) {
        return docSummaries.getOrDefault(namespace + ":" + docId, "");
    }

    private static String stripPrefix(String s, String prefix) {
        return s.startsWith(prefix) ? s.substring(prefix.length()) : s;
    }

    public record KeywordHit(String docId, double score, String summary) {}
}
