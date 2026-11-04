package io.github.rawwise.model.record;

import java.io.Serializable;

/**
 * UNIQUE constraint metadata.
 *
 * @param columns unique column set
 */
public record UniqueConstraintMeta(java.util.List<String> columns) implements Serializable {}
