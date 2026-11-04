package io.github.rawwise.axiom.pipeline;

import io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.op.WriteBuilder;
import java.util.Collection;
import java.util.Set;

public interface StorageWriteFacade {
    TableStore tableStore(String tableIdentifier);

    Collection<TableStore> allTableStores();

    void write(WriteBuilder builder);

    String findTableIdentifierByNumericId(long numericTableId);

    Set<String> getAllTableIdentifiers();

    Metastore getMetastore();

    SnapshotManager getSnapshotManager();

    EntityDerivationStage getEntityDerivationStage();
}
