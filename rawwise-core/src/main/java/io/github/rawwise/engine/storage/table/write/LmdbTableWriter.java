package io.github.rawwise.engine.storage.table.write;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.storage.writer.LmdbWriter;
import io.github.rawwise.model.op.Write;
import io.github.rawwise.model.op.WriteBuilder;
import java.io.IOException;
import java.util.Objects;

public class LmdbTableWriter implements TableWriter {

    @Override
    public Write.Result write(Write write) throws IOException {
        Objects.requireNonNull(write, "write cannot be null");
        var result = write.exec();
        if (result instanceof Write.Result writeResult) {
            return writeResult;
        }
        return Write.Result.error("Unexpected write result type: " + result.getClass().getName());
    }

    @Override
    public Write.Result write(WriteBuilder builder, WriteContext context) throws IOException {
        Objects.requireNonNull(builder, "builder cannot be null");
        Objects.requireNonNull(context, "context cannot be null");
        if (!(context instanceof LmdbWriteContext lmdbWriteContext)) {
            throw new IOException("Unsupported write context: " + context.getClass().getName());
        }
        builder.withWriter(
                new LmdbWriter(lmdbWriteContext.instance(), null, lmdbWriteContext.keyCodec()));
        return write(builder.build());
    }

    public record LmdbWriteContext(LmdbInstance instance, KeyCodec keyCodec)
            implements WriteContext {
        public LmdbWriteContext {
            Objects.requireNonNull(instance, "instance cannot be null");
            Objects.requireNonNull(keyCodec, "keyCodec cannot be null");
        }
    }
}
