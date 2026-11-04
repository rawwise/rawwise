package io.github.rawwise.model.record;

import java.io.Serial;
import java.io.Serializable;

public record DefaultRecord(Key key, Body body, Header header) implements Record, Serializable {
    @Serial private static final long serialVersionUID = 1L;
}
