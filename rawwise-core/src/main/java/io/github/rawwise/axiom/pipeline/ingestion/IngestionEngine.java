package io.github.rawwise.axiom.pipeline.ingestion;

import io.github.rawwise.axiom.pipeline.feedback.FeedbackSignal;
import io.github.rawwise.model.record.FeedbackAnnotation;
import io.github.rawwise.model.record.Raw;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.SemanticRecord;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface IngestionEngine {
    SemanticRecord ingest(IngestionRequest request);

    Optional<Raw> readRaw(Record.Key key, long tableId, long snapshotId);

    void markImmutable(Record.Key key, long tableId);

    SemanticRecord ingestFeedback(FeedbackSignal signal, long tableId);

    boolean annotateView(String viewId, FeedbackAnnotation annotation);

    List<FeedbackAnnotation> getViewAnnotations(String viewId);

    record IngestionRequest(
            Record.Key key,
            Record.Body body,
            long tableId,
            boolean immutable,
            Map<String, String> properties,
            String tableIdentifier) {
        public IngestionRequest(
                Record.Key key,
                Record.Body body,
                long tableId,
                boolean immutable,
                Map<String, String> properties) {
            this(key, body, tableId, immutable, properties, null);
        }
    }
}
