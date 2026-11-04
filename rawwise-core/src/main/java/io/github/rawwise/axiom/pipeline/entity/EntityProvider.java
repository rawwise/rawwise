package io.github.rawwise.axiom.pipeline.entity;

public interface EntityProvider extends AutoCloseable {

    String name();

    EntityView extract(String text, EntityContext context) throws ExtractionException;

    @Override
    default void close() {}
}
