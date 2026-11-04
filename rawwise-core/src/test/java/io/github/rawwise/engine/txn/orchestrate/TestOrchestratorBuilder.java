package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.store.VersionChainStore;
import io.github.rawwise.engine.txn.wal.ScanBackedTxnWALReader;
import io.github.rawwise.engine.txn.wal.TxnWALReader;
import io.github.rawwise.engine.txn.wal.TxnWALWriter;
import io.github.rawwise.metastore.Metastore;

public final class TestOrchestratorBuilder {

    private TestOrchestratorBuilder() {}

    public static TxnOrchestratorImpl createWithSharedWal(
            Metastore metastore,
            LmdbInstance lmdbInstance,
            TxnWALWriter walWriter,
            String catalog,
            String database) {
        LogicalClock clock = new AtomicLongClock(0);
        VersionChainStore versionChainStore = new VersionChainStore();
        ValidateStage validateStage = new ValidateStage();
        WriteStage writeStage = new DefaultWriteStage();
        DurableStage durableStage = new DefaultDurableStage();
        PublishStage publishStage = new DefaultPublishStage();

        TxnOrchestratorImpl orchestrator =
                new TxnOrchestratorImpl(
                        clock,
                        metastore,
                        lmdbInstance,
                        versionChainStore,
                        walWriter,
                        validateStage,
                        writeStage,
                        durableStage,
                        publishStage,
                        catalog,
                        database);
        TxnWALReader walReader = new ScanBackedTxnWALReader(walWriter);
        orchestrator.setWalReader(walReader);
        return orchestrator;
    }
}
