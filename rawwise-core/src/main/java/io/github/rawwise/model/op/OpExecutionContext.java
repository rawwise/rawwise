package io.github.rawwise.model.op;

import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableMeta;

public record OpExecutionContext(
        String tableId, long snapshotId, Schema schema, TableMeta tableMeta) {}
