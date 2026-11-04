package io.github.rawwise.axiom.pipeline.entity;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * PBT-style E2E test for Entity.deterministic ID generation.
 *
 * <p>Invariants verified (Pbt_EntityIdDeterministic):
 *
 * <ul>
 *   <li>Same (type, canonical-name, sampleId) -> same id (deterministic).
 *   <li>Different surface but same canonical name+type+sample -> same id.
 *   <li>Different case (CAROLINE / caroline / Caroline) -> same id (canonicalize lowercases).
 *   <li>Different sampleId -> different id.
 * </ul>
 */
@DisplayName("Entity.deterministic id PBT invariants")
class EntityIdDeterminismE2ETest {

    @Test
    @DisplayName("same args twice -> same id")
    void sameArgsProduceSameId() {
        Entity a = Entity.deterministic(EntityType.PERSON, "Caroline", "sample1", "surface1", 0.9);
        Entity b = Entity.deterministic(EntityType.PERSON, "Caroline", "sample1", "surface1", 0.9);
        assertThat(a.id()).isEqualTo(b.id());
    }

    @Test
    @DisplayName("different surface, same canonical name+type+sample -> same id")
    void differentSurfaceSameCanonical() {
        Entity a = Entity.deterministic(EntityType.PERSON, "Caroline", "sample1", "surfaceA", 0.9);
        Entity b = Entity.deterministic(EntityType.PERSON, "Caroline", "sample1", "Caroline,", 0.9);
        assertThat(a.id()).isEqualTo(b.id());
    }

    @Test
    @DisplayName("different case (upper/lower) -> same id (canonicalize lowercase)")
    void differentCaseSameId() {
        Entity upper = Entity.deterministic(EntityType.PERSON, "CAROLINE", "sample1", "x", 0.9);
        Entity lower = Entity.deterministic(EntityType.PERSON, "caroline", "sample1", "x", 0.9);
        Entity mixed = Entity.deterministic(EntityType.PERSON, "Caroline", "sample1", "x", 0.9);
        assertThat(upper.id()).isEqualTo(lower.id());
        assertThat(lower.id()).isEqualTo(mixed.id());
    }

    @Test
    @DisplayName("different sampleId -> different id")
    void differentSampleIdDifferentId() {
        Entity a = Entity.deterministic(EntityType.PERSON, "Caroline", "sample1", "x", 0.9);
        Entity b = Entity.deterministic(EntityType.PERSON, "Caroline", "sample2", "x", 0.9);
        assertThat(a.id()).isNotEqualTo(b.id());
    }

    @Test
    @DisplayName("different type -> different id (even same canonical name + sample)")
    void differentTypeDifferentId() {
        Entity person = Entity.deterministic(EntityType.PERSON, "Apple", "sample1", "x", 0.9);
        Entity org = Entity.deterministic(EntityType.ORG, "Apple", "sample1", "x", 0.9);
        assertThat(person.id()).isNotEqualTo(org.id());
    }
}
