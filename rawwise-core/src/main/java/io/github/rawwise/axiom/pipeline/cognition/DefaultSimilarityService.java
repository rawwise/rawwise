package io.github.rawwise.axiom.pipeline.cognition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultSimilarityService implements SimilarityService {

    private static final int NGRAM_SIZE = 3;

    @Override
    public double computeSimilarity(String text1, String text2) {
        if (text1 == null || text2 == null || text1.isEmpty() || text2.isEmpty()) {
            return 0.0;
        }
        if (text1.equals(text2)) {
            return 1.0;
        }

        List<String> tokens1 = tokenize(text1);
        List<String> tokens2 = tokenize(text2);
        if (tokens1.isEmpty() || tokens2.isEmpty()) {
            return 0.0;
        }

        Set<String> vocabulary = new HashSet<>();
        vocabulary.addAll(tokens1);
        vocabulary.addAll(tokens2);

        Map<String, Double> tfidf1 = computeTfIdf(tokens1, vocabulary, List.of(tokens1, tokens2));
        Map<String, Double> tfidf2 = computeTfIdf(tokens2, vocabulary, List.of(tokens1, tokens2));

        return cosineSimilarity(tfidf1, tfidf2, vocabulary);
    }

    List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        String lowered = text.toLowerCase();
        for (String word : lowered.split("[^a-zA-Z0-9\\p{L}]+")) {
            if (word.length() >= 2) {
                tokens.add(word);
            }
        }
        for (int i = 0; i <= lowered.length() - NGRAM_SIZE; i++) {
            tokens.add(lowered.substring(i, i + NGRAM_SIZE));
        }
        return tokens;
    }

    Map<String, Double> computeTfIdf(
            List<String> tokens, Set<String> vocabulary, Collection<List<String>> allDocuments) {
        Map<String, Double> tfidf = new HashMap<>();
        int docCount = allDocuments.size();
        for (String term : vocabulary) {
            double tf = countOccurrences(tokens, term) / (double) Math.max(1, tokens.size());
            double df = countDocumentsContaining(allDocuments, term);
            double idf = Math.log((docCount + 1.0) / (df + 1.0)) + 1.0;
            tfidf.put(term, tf * idf);
        }
        return tfidf;
    }

    double cosineSimilarity(
            Map<String, Double> vec1, Map<String, Double> vec2, Set<String> vocabulary) {
        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;
        for (String term : vocabulary) {
            double v1 = vec1.getOrDefault(term, 0.0);
            double v2 = vec2.getOrDefault(term, 0.0);
            dotProduct += v1 * v2;
            norm1 += v1 * v1;
            norm2 += v2 * v2;
        }
        if (norm1 == 0.0 || norm2 == 0.0) {
            return 0.0;
        }
        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    private int countOccurrences(List<String> tokens, String term) {
        int count = 0;
        for (String t : tokens) {
            if (t.equals(term)) count++;
        }
        return count;
    }

    private int countDocumentsContaining(Collection<List<String>> docs, String term) {
        int count = 0;
        for (List<String> doc : docs) {
            if (doc.contains(term)) count++;
        }
        return count;
    }
}
