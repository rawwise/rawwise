package io.github.rawwise.engine.storage.writer;

public final class WriterFactoryBootstrap {

    private WriterFactoryBootstrap() {}

    private static volatile boolean registered = false;

    public static void registerAll() {
        if (registered) {
            return;
        }
        WriterFactoryRegistry registry = WriterFactoryRegistry.getInstance();

        registry.register(new LmdbWriterFactory());
        registry.register(new ParquetWriterFactory());
        registry.register(new LocalWALWriterFactory());
        registry.register(new HdfsWALWriterFactory());
        registry.register(new ManifestFileWriterFactory());
        registry.register(new ManifestListWriterFactory());
        registered = true;
    }
}
