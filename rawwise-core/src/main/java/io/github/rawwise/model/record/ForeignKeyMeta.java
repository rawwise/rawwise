package io.github.rawwise.model.record;

import java.io.Serializable;

/**
 * foreign key constraint metadata.
 *
 * @param columns child table columns
 * @param parent Table referencing parent table identifier
 * @param parent Columns referencing parent table columns
 */
public record ForeignKeyMeta(
        java.util.List<String> columns, String parentTable, java.util.List<String> parentColumns)
        implements Serializable {}
