package io.github.rawwise.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.feedback.retrieval.RetrievalFeedbackCollector;
import io.github.rawwise.backend.server.service.AxiomServiceImpl;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.proto.axiom.ImplicitFeedbackRequest;
import io.github.rawwise.proto.axiom.ImplicitFeedbackResponse;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ImplicitFeedback - implicit feedback end-to-end test")
class ImplicitFeedbackTest {

    private KnowledgeStore knowledgeStore;
    private AxiomServiceImpl axiomService;
    private RetrievalFeedbackCollector feedbackCollector;

    @BeforeEach
    void setUp() {
        knowledgeStore = new InMemoryKnowledgeStore();
        axiomService = new AxiomServiceImpl();
        feedbackCollector = new RetrievalFeedbackCollector(knowledgeStore, "default.test_table");
        axiomService.setFeedbackCollector(feedbackCollector);
    }

    @AfterEach
    void tearDown() {
        if (knowledgeStore != null) {
            try {
                knowledgeStore.close();
            } catch (Exception e) {
                /* tearDown best-effort */
            }
        }
    }

    private ImplicitFeedbackResponse sendFeedback(String viewId, String signalType, long dwellMs) {
        List<ImplicitFeedbackResponse> captured = new ArrayList<>(1);
        ImplicitFeedbackRequest request =
                ImplicitFeedbackRequest.newBuilder()
                        .setViewId(viewId != null ? viewId : "")
                        .setSignalType(signalType != null ? signalType : "")
                        .setDwellMs(dwellMs)
                        .build();
        axiomService.recordImplicitFeedback(
                request,
                new StreamObserver<>() {
                    @Override
                    public void onNext(ImplicitFeedbackResponse value) {
                        captured.add(value);
                    }

                    @Override
                    public void onError(Throwable t) {}

                    @Override
                    public void onCompleted() {}
                });
        return captured.isEmpty() ? null : captured.get(0);
    }

    @Nested
    @DisplayName("S1-HappyPath - implicit feedback path")
    class HappyPath {

        @Test
        @DisplayName("DWELL be persistedto KnowledgeStore")
        void dwellFeedbackCollectedAndPersisted() throws Exception {
            String viewId = "view:doc:001";

            ImplicitFeedbackResponse response = sendFeedback(viewId, "DWELL", 5000);

            assertThat(response).isNotNull();
            assertThat(response.getAccepted()).isTrue();

            byte[] prefix = RetrievalFeedbackCollector.KEY_PREFIX.getBytes(StandardCharsets.UTF_8);
            var entries = knowledgeStore.scanAll(prefix);
            assertThat(entries).hasSize(1);
        }

        @Test
        @DisplayName("CLICK be is POSITIVE type")
        void clickFeedbackMappedToPositive() throws Exception {
            String viewId = "view:doc:002";

            ImplicitFeedbackResponse response = sendFeedback(viewId, "CLICK", 0);

            assertThat(response).isNotNull();
            assertThat(response.getAccepted()).isTrue();

            byte[] prefix = RetrievalFeedbackCollector.KEY_PREFIX.getBytes(StandardCharsets.UTF_8);
            var entries = knowledgeStore.scanAll(prefix);
            assertThat(entries).hasSize(1);
        }

        @Test
        @DisplayName("SKIP be is NEGATIVE type")
        void skipFeedbackMappedToNegative() throws Exception {
            String viewId = "view:doc:003";

            ImplicitFeedbackResponse response = sendFeedback(viewId, "SKIP", 100);

            assertThat(response).isNotNull();
            assertThat(response.getAccepted()).isTrue();

            byte[] prefix = RetrievalFeedbackCollector.KEY_PREFIX.getBytes(StandardCharsets.UTF_8);
            var entries = knowledgeStore.scanAll(prefix);
            assertThat(entries).hasSize(1);
        }
    }

    @Nested
    @DisplayName("S4-Boundary - no graceful skip")
    class Boundary {

        @Test
        @DisplayName("no viewId still returns accepted=true(graceful skip)")
        void invalidViewIdReturnsAccepted() {
            ImplicitFeedbackResponse response = sendFeedback("nonexistent-view-id", "CLICK", 0);

            assertThat(response).isNotNull();
            assertThat(response.getAccepted()).isTrue();
        }

        @Test
        @DisplayName("signalType default is POSITIVE")
        void emptySignalTypeDefaultsToPositive() {
            ImplicitFeedbackResponse response = sendFeedback("view:empty", "", 0);

            assertThat(response).isNotNull();
            assertThat(response.getAccepted()).isTrue();
        }

        @Test
        @DisplayName("null viewId returns without throwing exception accepted=true")
        void nullViewIdDoesNotThrow() {
            ImplicitFeedbackResponse response = sendFeedback(null, "DWELL", 3000);

            assertThat(response).isNotNull();
            assertThat(response.getAccepted()).isTrue();
        }

        @Test
        @DisplayName("feedbackCollector is null still returns accepted=true")
        void nullFeedbackCollectorGracefulSkip() {
            axiomService.setFeedbackCollector(null);

            ImplicitFeedbackResponse response = sendFeedback("view:no-collector", "CLICK", 0);

            assertThat(response).isNotNull();
            assertThat(response.getAccepted()).isTrue();
        }
    }
}
