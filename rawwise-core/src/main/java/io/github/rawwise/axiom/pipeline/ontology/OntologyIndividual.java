package io.github.rawwise.axiom.pipeline.ontology;

import java.util.Map;

public record OntologyIndividual(
        String individualId, String className, Map<String, Object> propertyValues) {}
