package io.github.rawwise.axiom.pipeline.ontology;

import io.github.rawwise.model.record.ForeignKeyMeta;

public record OntologyObjectProperty(
        String propertyName,
        String propertyLabel,
        String domainClass,
        String rangeClass,
        ForeignKeyMeta foreignKeyMeta) {}
