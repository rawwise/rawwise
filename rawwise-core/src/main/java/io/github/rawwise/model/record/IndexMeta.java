package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.List;

/**
 * secondary index metadata.
 *
 * @param index Name index(globally unique, {@code idx_orders_w_d})
 * @param table Id belonging to table
 * @param columns index columns(prefix order)
 * @param storing covering output columns(STORING clause),index columns
 */
public record IndexMeta(
        String indexName, TableId tableId, List<String> columns, List<String> storing)
        implements Serializable {}
