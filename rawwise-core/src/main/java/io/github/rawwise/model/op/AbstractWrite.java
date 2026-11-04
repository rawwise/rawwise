package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.writer.Initializable;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import java.util.Properties;

public abstract class AbstractWrite implements Write {
    protected final Properties config;
    protected final RecordWriter<?> writer;

    public AbstractWrite(RecordWriter<?> writer, Properties config) {
        this.writer = writer;
        this.config = config;
    }

    @Override
    public Write.Result exec() {
        try {
            if (writer instanceof Initializable initable) {
                initable.init(config);
            }

            doWrite();

            writer.flush();

            return Result.ok(writer.recordsWritten());

        } catch (Exception e) {
            return Result.error("Write failed: " + e.getMessage());
        } finally {
            try {
                writer.close();
            } catch (Exception ignored) {
            }
        }
    }

    protected abstract void doWrite() throws Exception;

    @Override
    public long writeSize() {
        return writer.recordsWritten();
    }
}
