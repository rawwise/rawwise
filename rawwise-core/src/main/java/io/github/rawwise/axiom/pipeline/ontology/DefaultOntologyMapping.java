package io.github.rawwise.axiom.pipeline.ontology;

import java.util.List;

public record DefaultOntologyMapping(
        OntologyClass ontologyClass,
        List<OntologyDataProperty> dataProperties,
        List<OntologyObjectProperty> objectProperties,
        List<OntologyAxiom> axioms)
        implements OntologyMapping {}
