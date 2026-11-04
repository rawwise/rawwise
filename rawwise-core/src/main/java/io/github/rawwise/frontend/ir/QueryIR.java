package io.github.rawwise.frontend.ir;

import java.io.Serializable;
import java.util.Set;

public sealed interface QueryIR extends Serializable
        permits PointQueryIR,
                BatchPointQueryIR,
                MetadataQueryIR,
                ScanQueryIR,
                AggregateQueryIR,
                JoinQueryIR,
                WriteQueryIR,
                UpdateRangeIR,
                DeleteRangeIR,
                UpdateQueryIR,
                DeleteQueryIR,
                DDLIR {

    Set<String> tableIdentifiers();

    IRType type();
}
