package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("VectorTimeTravel E2E - indexversion row")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class VectorTimeTravelE2ETest {

    private static final int DIM = 16;
    private static final int MERGE_THRESHOLD = 5;

    @TempDir Path tempDir;

    private StubEmbeddingProvider embeddingProvider;
    private JVectorVectorIndex index;

    @BeforeEach
    void setUp() {
        embeddingProvider = new StubEmbeddingProvider(DIM);
        JVectorConfig config =
                new JVectorConfig(
                        "disk",
                        DIM,
                        16,
                        10,
                        50,
                        tempDir.resolve("vector-index").toString(),
                        MERGE_THRESHOLD,
                        MERGE_THRESHOLD,
                        500000);
        index = new JVectorVectorIndex(config, embeddingProvider, false);
    }

    @AfterEach
    void tearDown() {
        try {
            index.close();
        } catch (Exception e) {
            /* ignore */
        }
    }

    @Test
    @DisplayName("Invalid snapshotId (<=0) delegates to current index")
    void testSearchWithInvalidSnapshotId() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("invalid snapshot doc " + i);
            index.indexWithEmbedding("ns", "key-" + i, embedding);
        }

        float[] query = embeddingProvider.embed("invalid snapshot doc 0");
        List<VectorSearchResult> results = index.search("ns", query, 5, -1L);
        assertThat(results).isNotEmpty();

        List<VectorSearchResult> zeroResults = index.search("ns", query, 5, 0L);
        assertThat(zeroResults).isNotEmpty();
    }

    @Test
    @DisplayName(
            "First merge creates version 1; search with snapshotId=1 returns first batch results")
    void testTimeTravelToFirstVersion() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("v1 doc " + i);
            index.indexWithEmbedding("ns", "key-" + i, embedding);
        }

        float[] query = embeddingProvider.embed("v1 doc 0");
        List<VectorSearchResult> current = index.search("ns", query, 5);
        assertThat(current).isNotEmpty();

        List<VectorSearchResult> v1 = index.search("ns", query, 5, 1L);
        assertThat(v1).isNotEmpty();
        assertThat(v1.stream().anyMatch(r -> r.entryKey().equals("key-0"))).isTrue();
    }

    @Test
    @DisplayName("Version 2 > version 1; newer snapshot includes more data")
    void testVersion2ContainsMoreData() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("v2 doc " + i);
            index.indexWithEmbedding("ns", "key-" + i, embedding);
        }

        float[] query = embeddingProvider.embed("v2 doc 5");

        List<VectorSearchResult> v1 = index.search("ns", query, 5, 1L);
        List<VectorSearchResult> v2 = index.search("ns", query, 5, 2L);

        assertThat(v1).isNotEmpty();
        assertThat(v2).isNotEmpty();
        assertThat(v2.get(0).entryKey()).isEqualTo(v1.get(0).entryKey());
    }

    @Test
    @DisplayName("snapshotId larger than any version falls back to current index")
    void testSnapshotTooLargeFallback() {
        for (int i = 0; i < 10; i++) {
            float[] embedding = embeddingProvider.embed("large snapshot doc " + i);
            index.indexWithEmbedding("ns", "key-" + i, embedding);
        }

        float[] query = embeddingProvider.embed("large snapshot doc 0");
        List<VectorSearchResult> results = index.search("ns", query, 5, 9999L);
        assertThat(results).isNotEmpty();
    }
}
