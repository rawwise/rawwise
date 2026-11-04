package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public sealed interface SemanticRecord extends Serializable permits Record {
    Record.Key key();

    Record.Header header();

    Raw raw();

    List<Chunk> chunks();

    List<View> views();

    Optional<Semantic> semantics();

    Optional<Action> actions();
}
