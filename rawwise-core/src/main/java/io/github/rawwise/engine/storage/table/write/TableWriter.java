package io.github.rawwise.engine.storage.table.write;

import io.github.rawwise.model.op.Write;
import io.github.rawwise.model.op.WriteBuilder;
import java.io.IOException;

public interface TableWriter {

    Write.Result write(Write write) throws IOException;

    Write.Result write(WriteBuilder builder, WriteContext context) throws IOException;

    interface WriteContext {}
}
