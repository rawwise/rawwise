package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Task 1.5 - Depth enum")
class DepthTest {

    @Test
    @DisplayName("Depth carries ABSTRACT (View) and DETAIL (Raw body) values")
    void enumConstantsPresent() {
        assertThat(Depth.values()).containsExactlyInAnyOrder(Depth.ABSTRACT, Depth.DETAIL);
    }

    @Test
    @DisplayName("Depth names are stable")
    void enumNames() {
        assertThat(Depth.ABSTRACT.name()).isEqualTo("ABSTRACT");
        assertThat(Depth.DETAIL.name()).isEqualTo("DETAIL");
        assertThat(Depth.ABSTRACT).isSameAs(Depth.valueOf("ABSTRACT"));
        assertThat(Depth.DETAIL).isSameAs(Depth.valueOf("DETAIL"));
    }

    @Test
    @DisplayName("resolve maps ABSTRACT/DETAIL literals")
    void resolveLiteral() {
        assertThat(Depth.resolve("ABSTRACT")).isSameAs(Depth.ABSTRACT);
        assertThat(Depth.resolve("abstract")).isSameAs(Depth.ABSTRACT);
        assertThat(Depth.resolve("DETAIL")).isSameAs(Depth.DETAIL);
        assertThat(Depth.resolve("detail")).isSameAs(Depth.DETAIL);
    }

    @Test
    @DisplayName("resolve maps legacy load tiers: L0->ABSTRACT, L1/L2->DETAIL")
    void resolveLegacyLoadTier() {
        assertThat(Depth.resolve("L0")).isSameAs(Depth.ABSTRACT);
        assertThat(Depth.resolve("l0")).isSameAs(Depth.ABSTRACT);
        assertThat(Depth.resolve("L1")).isSameAs(Depth.DETAIL);
        assertThat(Depth.resolve("L2")).isSameAs(Depth.DETAIL);
    }

    @Test
    @DisplayName("resolve defaults to ABSTRACT on null/blank (anti-NPE)")
    void resolveDefaultsToAbstract() {
        assertThat(Depth.resolve(null)).isSameAs(Depth.ABSTRACT);
        assertThat(Depth.resolve("")).isSameAs(Depth.ABSTRACT);
        assertThat(Depth.resolve("   ")).isSameAs(Depth.ABSTRACT);
    }

    @Test
    @DisplayName("resolve rejects invalid depth")
    void resolveRejectsInvalid() {
        assertThatThrownBy(() -> Depth.resolve("L3")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Depth.resolve("FULL"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
