package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class ColumnGroupSchemaEvolutionTest {

    @Test
    void shouldRejectModifyingExistingColumnGroupColumns() {
        Schema v2 =
                Schema.builder()
                        .field("pk", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .field("age", PrimitiveType.INT)
                        .field("email", PrimitiveType.STRING)
                        .field("login_count", PrimitiveType.INT)
                        .field("score", PrimitiveType.DOUBLE)
                        .field("level", PrimitiveType.STRING)
                        .build()
                        .withColumnGroups(
                                List.of(
                                        new ColumnGroup(
                                                "cg_stats",
                                                List.of("age", "login_count", "score"),
                                                ColumnGroup.Encoding.FIXED_OFFSET)));

        ColumnGroup cgStatsModified =
                new ColumnGroup(
                        "cg_stats",
                        List.of("age", "login_count", "score", "level"),
                        ColumnGroup.Encoding.FIXED_OFFSET);

        assertThatThrownBy(() -> v2.withColumnGroups(List.of(cgStatsModified)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support internal column changes");
    }

    @Test
    void shouldRejectModifyingExistingColumnGroupEncoding() {
        Schema v2 =
                Schema.builder()
                        .field("pk", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .field("age", PrimitiveType.INT)
                        .build()
                        .withColumnGroups(
                                List.of(
                                        new ColumnGroup(
                                                "cg_basic",
                                                List.of("name"),
                                                ColumnGroup.Encoding.FIXED_OFFSET)));

        ColumnGroup cgBasicTlv =
                new ColumnGroup("cg_basic", List.of("name"), ColumnGroup.Encoding.TLV);

        assertThatThrownBy(() -> v2.withColumnGroups(List.of(cgBasicTlv)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support internal column changes");
    }
}
