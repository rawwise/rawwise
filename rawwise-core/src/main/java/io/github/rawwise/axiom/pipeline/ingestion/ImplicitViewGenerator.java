package io.github.rawwise.axiom.pipeline.ingestion;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.View;
import java.util.Optional;

public interface ImplicitViewGenerator {

    void generateImplicitView(Record.Key rawKey, long tableId, long snapshotId);

    boolean isViewGenerated(Record.Key rawKey, long tableId, long snapshotId);

    Optional<View> getImplicitView(Record.Key rawKey, long tableId, long snapshotId);

    Metastore metastore();
}
