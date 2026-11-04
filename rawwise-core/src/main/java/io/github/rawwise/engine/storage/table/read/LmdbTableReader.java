package io.github.rawwise.engine.storage.table.read;

import io.github.rawwise.engine.storage.QueryCapability;
import io.github.rawwise.engine.storage.block.read.LmdbDataBlockReader;
import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactoryBootstrap;
import io.github.rawwise.engine.storage.reader.ReaderFactoryRegistry;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Read;
import io.github.rawwise.model.op.Scan;
import io.github.rawwise.model.record.RecordCodec;
import java.io.IOException;
import java.util.Set;

public class LmdbTableReader extends AbstractReader implements TableReader {
    private final LmdbDataBlockReader dataBlockReader;
    private final boolean ownReader;

    public LmdbTableReader(TableStore tableStore) {
        this.dataBlockReader = createDataBlockReader(tableStore);
        this.ownReader = true;
    }

    private static LmdbDataBlockReader createDataBlockReader(TableStore tableStore) {
        ReaderFactoryBootstrap.ensureInitialized();
        try {
            ReaderContext ctx = ReaderContext.empty().put("tableStore", tableStore);
            // Wire BodyCodec + SchemaResolver for projection-aware decode
            BodyCodec bc =
                    tableStore.getRecordCodec()
                                    instanceof
                                    io.github.rawwise.model.record.impl.DefaultRecordCodec drc
                            ? drc.getBodyCodec()
                            : null;
            RecordCodec.SchemaResolver sr = tableStore.getSchemaResolver();
            if (bc != null) {
                ctx.put("bodyCodec", bc);
            }
            if (sr != null) {
                ctx.put("schemaResolver", sr);
            }
            return ReaderFactoryRegistry.getInstance()
                    .create(
                            ReaderType.BLOCK_LMDB,
                            LmdbDataBlockReader.class,
                            ReaderConfig.builder(ReaderType.BLOCK_LMDB).build(),
                            ctx);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create LmdbDataBlockReader via factory", e);
        }
    }

    public LmdbTableReader(TableStore tableStore, LmdbDataBlockReader dataBlockReader) {
        this.dataBlockReader = dataBlockReader;
        this.ownReader = false;
    }

    @Override
    public Read.Result read(Read read) throws IOException {
        ensureOpen();
        if (read instanceof Find find) {
            return find(find);
        }
        if (read instanceof Scan scan) {
            return scan(scan);
        }
        return Scan.Result.error("Unsupported read type: " + read.getClass().getName(), null);
    }

    @Override
    public Find.Result find(Find find) throws IOException {
        ensureOpen();
        return dataBlockReader.find(find, find.getDataBlocks());
    }

    @Override
    public Scan.Result scan(Scan scan) throws IOException {
        ensureOpen();
        return dataBlockReader.scan(scan, scan.getDataBlocks());
    }

    @Override
    public Set<QueryCapability> capabilities() {
        return Set.of(
                QueryCapability.POINT_LOOKUP,
                QueryCapability.RANGE_SCAN,
                QueryCapability.FULL_READ,
                QueryCapability.PREDICATE_PUSHDOWN,
                QueryCapability.LIMIT_PUSHDOWN,
                QueryCapability.PARTITION_PRUNING);
    }

    @Override
    protected void doClose() throws IOException {
        if (ownReader) {
            dataBlockReader.close();
        }
    }
}
