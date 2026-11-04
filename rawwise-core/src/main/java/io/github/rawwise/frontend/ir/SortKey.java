package io.github.rawwise.frontend.ir;

import java.io.Serializable;

public record SortKey(String column, boolean ascending, boolean nullsFirst)
        implements Serializable {
    public SortKey(String column, boolean ascending) {
        this(column, ascending, true);
    }
}
