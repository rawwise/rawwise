package io.github.rawwise.engine.vector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShardInternalIndexVectorAdapter implements VectorIndex {

    private final ShardInternalIndex delegate;
    private final String namespace;

    public ShardInternalIndexVectorAdapter(ShardInternalIndex delegate, String namespace) {
        this.delegate = delegate;
        this.namespace = namespace;
    }

    @Override
    public void index(String namespace, String entryKey, String l0Abstract) {
        ensureInitialized();
        log.debug(
                "ShardInternalIndexVectorAdapter.index: l0Abstract indexing not supported by ShardInternalIndex, skipping for namespace={}",
                namespace);
    }

    @Override
    public void indexWithEmbedding(String namespace, String entryKey, float[] embedding) {
        ensureInitialized();
        delegate.insert(entryKey, embedding);
    }

    @Override
    public List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK) {
        if (!delegate.isInitialized()) {
            return List.of();
        }
        List<SearchCandidate> candidates = delegate.search(queryEmbedding, topK);
        List<VectorSearchResult> results = new ArrayList<>(candidates.size());
        for (SearchCandidate c : candidates) {
            results.add(new VectorSearchResult(c.entryKey(), null, c.approximateDistance()));
        }
        return results;
    }

    @Override
    public void delete(String namespace, String entryKey) {
        delegate.markTombstone(entryKey);
    }

    @Override
    public void flushToCold(String namespace, Collection<String> entryKeys) {}

    @Override
    public void setColdStore(VectorColdStore coldStore) {}

    @Override
    public void close() {
        delegate.close();
    }

    private void ensureInitialized() {
        if (!delegate.isInitialized()) {
            delegate.initialize(List.of(), List.of());
        }
    }
}
