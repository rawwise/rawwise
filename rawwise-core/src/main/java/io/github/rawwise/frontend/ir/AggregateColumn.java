package io.github.rawwise.frontend.ir;

import java.io.Serializable;

public record AggregateColumn(AggregateFunction function, String column, String alias)
        implements Serializable {}
