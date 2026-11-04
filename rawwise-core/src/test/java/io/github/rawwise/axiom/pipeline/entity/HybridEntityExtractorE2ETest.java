package io.github.rawwise.axiom.pipeline.entity;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("HybridEntityExtractor E2E - LLM primary + NoOp fallback")
class HybridEntityExtractorE2ETest {

    @Test
    @DisplayName(
            "extract uses stub LLM as primary, returns PERSON entity with valid confidence and source label")
    void extractWithStubLlmAndNoOpFallback() throws Exception {
        EntityProvider primary = new LlmEntityProvider("stub://test", Duration.ofSeconds(1), 0);
        EntityProvider fallback = new NoOpEntityProvider();

        try (HybridEntityExtractor extractor = new HybridEntityExtractor(primary, fallback, true)) {
            String text = "Caroline went to LGBTQ Support Group in Seattle";
            EntityContext ctx = EntityContext.of("sample-1", "test_table", 0L);

            EntityView view = extractor.extract(text, ctx);

            assertThat(view).isNotNull();
            assertThat(view.status()).isEqualTo(ExtractionStatus.SUCCESS);
            assertThat(view.entities()).isNotEmpty();

            boolean foundCaroline =
                    view.entities().stream()
                            .anyMatch(
                                    e ->
                                            e.type() == EntityType.PERSON
                                                    && "Caroline".equalsIgnoreCase(e.name()));
            assertThat(foundCaroline).as("must extract Caroline as PERSON").isTrue();

            for (Entity e : view.entities()) {
                assertThat(e.confidence()).isBetween(0.75, 0.95);
            }

            assertThat(view.source()).isIn("llm-only", "hybrid");
        }
    }

    @Test
    @DisplayName(
            "when primary throws and fallback returns view, fallback view is returned with source=spacy-only")
    void primaryFailsFallbackSucceeds() throws Exception {
        EntityProvider primary =
                new EntityProvider() {
                    @Override
                    public String name() {
                        return "broken-llm";
                    }

                    @Override
                    public EntityView extract(String text, EntityContext context)
                            throws ExtractionException {
                        throw new ExtractionException("simulated llm outage");
                    }
                };
        EntityProvider fallback = new NoOpEntityProvider();

        try (HybridEntityExtractor extractor = new HybridEntityExtractor(primary, fallback, true)) {
            EntityView view =
                    extractor.extract("Caroline meets Bob", EntityContext.of("sid", "tid", 0L));

            assertThat(view).isNotNull();
            assertThat(view.source()).isEqualTo("spacy-only");
        }
    }
}
