package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public class NwlWALReaderFactory implements ReaderFactory<NwlWALReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.WAL;
    }

    @Override
    public Class<NwlWALReader> readerClass() {
        return NwlWALReader.class;
    }

    @Override
    public NwlWALReader create(ReaderConfig config, ReaderContext context) throws IOException {
        String basePath = config.filePath();
        if (basePath == null) {
            throw new IllegalStateException(
                    "filePath is required in ReaderConfig for NwlWALReader");
        }
        Configuration hadoopConf = context.get("hadoopConfiguration", Configuration.class);
        if (hadoopConf == null) {
            hadoopConf = new Configuration();
        }
        return new NwlWALReader(basePath, hadoopConf, context.metrics());
    }
}
