package io.github.rawwise.model.record;

public record Field(boolean required, int id, String name, Type type, String doc) {}
