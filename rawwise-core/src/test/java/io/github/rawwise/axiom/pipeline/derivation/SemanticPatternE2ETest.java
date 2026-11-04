package io.github.rawwise.axiom.pipeline.derivation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.model.record.Chunk;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("SemanticPattern E2E - scenariotest")
class SemanticPatternE2ETest {

    private SemanticPatternRenderer renderer;

    @BeforeEach
    void setUp() {
        renderer = new DefaultSemanticPatternRenderer();
    }

    @Nested
    @DisplayName("S1-SimpleFieldSubstitution - ${field} singlefield")
    class SimpleFieldSubstitution {

        @Test
        @DisplayName("multifieldsame , package allfieldvalue")
        void renderMultipleFields() {
            String pattern = "用户${name}的年龄是${age}，城市是${city}";
            Map<String, Object> fields = new HashMap<>();
            fields.put("name", "Alice");
            fields.put("age", 30);
            fields.put("city", "北京");

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("用户Alice的年龄是30，城市是北京");
        }

        @Test
        @DisplayName("fieldwhen not exists is")
        void renderMissingFieldBecomesEmpty() {
            String pattern = "名字是${name}，邮箱是${email}";
            Map<String, Object> fields = Map.of("name", "Bob");

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("名字是Bob，邮箱是");
        }

        @Test
        @DisplayName("null valuefield is")
        void renderNullFieldBecomesEmpty() {
            String pattern = "名字是${name}";
            Map<String, Object> fields = new HashMap<>();
            fields.put("name", null);

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("名字是");
        }
    }

    @Nested
    @DisplayName("S2-TypeAnnotation - ${field:type} typeannotation")
    class TypeAnnotation {

        @Test
        @DisplayName("int typeannotationdoes not affect value")
        void renderWithIntType() {
            String pattern = "年龄${age:int}";
            Map<String, Object> fields = Map.of("age", 30);

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("年龄30");
        }

        @Test
        @DisplayName("string typeannotationdoes not affect value")
        void renderWithStringType() {
            String pattern = "名字${name:string}";
            Map<String, Object> fields = Map.of("name", "Alice");

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("名字Alice");
        }
    }

    @Nested
    @DisplayName("S3-DescAnnotation - ${field:desc} descriptionannotation")
    class DescAnnotation {

        @Test
        @DisplayName("in descriptionannotationdoes not affect value")
        void renderWithChineseDesc() {
            String pattern = "年龄${age:年龄}";
            Map<String, Object> fields = Map.of("age", 30);

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("年龄30");
        }

        @Test
        @DisplayName("descriptionannotationdoes not affect value")
        void renderWithEnglishDesc() {
            String pattern = "${price:price_in_usd} dollars";
            Map<String, Object> fields = Map.of("price", 99.9);

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("99.9 dollars");
        }
    }

    @Nested
    @DisplayName("S4-JoinFunction - ${join(field, sep)} columntablejoin")
    class JoinFunction {

        @Test
        @DisplayName("List field join")
        void joinListWithComma() {
            String pattern = "标签: ${join(tags, \", \")}";
            Map<String, Object> fields = new HashMap<>();
            fields.put("tags", List.of("java", "python", "rust"));

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("标签: java, python, rust");
        }

        @Test
        @DisplayName("single List cancorrect")
        void joinSingleElementList() {
            String pattern = "标签: ${join(tags, \", \")}";
            Map<String, Object> fields = new HashMap<>();
            fields.put("tags", List.of("java"));

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("标签: java");
        }

        @Test
        @DisplayName("List is")
        void joinEmptyList() {
            String pattern = "标签: ${join(tags, \", \")}";
            Map<String, Object> fields = new HashMap<>();
            fields.put("tags", List.of());

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("标签: ");
        }
    }

    @Nested
    @DisplayName("S5-IfFunction - ${if(field, true, false)} condition")
    class IfFunction {

        @Test
        @DisplayName("fieldhasvalue true branch")
        void ifFieldPresent() {
            String pattern = "${if(email, \"有邮箱\", \"无邮箱\")}";
            Map<String, Object> fields = Map.of("email", "a@b.com");

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("有邮箱");
        }

        @Test
        @DisplayName("fieldis null false branch")
        void ifFieldNull() {
            String pattern = "${if(email, \"有邮箱\", \"无邮箱\")}";
            Map<String, Object> fields = new HashMap<>();
            fields.put("email", null);

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("无邮箱");
        }

        @Test
        @DisplayName("fieldis false branch")
        void ifFieldEmptyString() {
            String pattern = "${if(name, \"有名字\", \"无名字\")}";
            Map<String, Object> fields = new HashMap<>();
            fields.put("name", "");

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("无名字");
        }
    }

    @Nested
    @DisplayName("S6-InferDefaultPattern - from schema generatedefault pattern")
    class InferDefaultPattern {

        @Test
        @DisplayName("multicolumngeneratedefault pattern")
        void inferPatternFromMultipleColumns() {
            String pattern =
                    renderer.inferDefaultPattern("default.users", List.of("id", "name", "age"));

            assertThat(pattern).isEqualTo("id为${id}，name为${name}，age为${age}的记录");
        }

        @Test
        @DisplayName("columncolumntablereturns empty")
        void inferPatternFromEmptyColumns() {
            String pattern = renderer.inferDefaultPattern("default.empty", List.of());

            assertThat(pattern).isEmpty();
        }

        @Test
        @DisplayName("generateofdefault pattern cancorrect")
        void inferredPatternRendersCorrectly() {
            String pattern = renderer.inferDefaultPattern("default.users", List.of("id", "name"));
            Map<String, Object> fields = Map.of("id", 1, "name", "Alice");

            String rendered = renderer.render(pattern, fields);

            assertThat(rendered).isEqualTo("id为1，name为Alice的记录");
        }
    }

    @Nested
    @DisplayName("S7-PatternBasedChunkingStrategy - pattern end-to-end")
    class PatternBasedChunkingStrategyE2E {

        private PatternBasedChunkingStrategy strategy;

        @BeforeEach
        void setUp() {
            strategy = new PatternBasedChunkingStrategy();
        }

        @Test
        @DisplayName("has pattern + fields returns when")
        void splitWithPatternAndFields() {
            Map<String, Object> params = new HashMap<>();
            params.put("semantic.pattern", "用户${name}，年龄${age}");
            params.put("record.fields", Map.of("name", "Bob", "age", 25));

            List<byte[]> chunks =
                    strategy.split("fallback".getBytes(StandardCharsets.UTF_8), params);

            assertThat(chunks).hasSize(1);
            String rendered = new String(chunks.get(0), StandardCharsets.UTF_8);
            assertThat(rendered).isEqualTo("用户Bob，年龄25");
        }

        @Test
        @DisplayName("renderedContent before pattern + fields")
        void splitPrefersRenderedContent() {
            Map<String, Object> params = new HashMap<>();
            params.put("semantic.pattern", "用户${name}");
            params.put("record.fields", Map.of("name", "Bob"));
            params.put("renderedContent", "预渲染内容".getBytes(StandardCharsets.UTF_8));

            List<byte[]> chunks =
                    strategy.split("fallback".getBytes(StandardCharsets.UTF_8), params);

            assertThat(chunks).hasSize(1);
            String rendered = new String(chunks.get(0), StandardCharsets.UTF_8);
            assertThat(rendered).isEqualTo("预渲染内容");
        }

        @Test
        @DisplayName("no pattern returns whenoriginal content")
        void splitWithoutPatternReturnsOriginal() {
            Map<String, Object> params = new HashMap<>();
            byte[] original = "原始内容".getBytes(StandardCharsets.UTF_8);

            List<byte[]> chunks = strategy.split(original, params);

            assertThat(chunks).hasSize(1);
            assertThat(chunks.get(0)).isEqualTo(original);
        }

        @Test
        @DisplayName("strategy name and type correct")
        void strategyNameAndType() {
            assertThat(strategy.name()).isEqualTo("pattern");
            assertThat(strategy.supportedType()).isEqualTo(Chunk.ChunkType.TEXT_SEGMENT);
        }
    }

    @Nested
    @DisplayName("S8-EdgeCases - boundary")
    class EdgeCases {

        @Test
        @DisplayName("pattern returns empty")
        void emptyPatternReturnsEmpty() {
            String result = renderer.render("", Map.of("name", "Alice"));
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("null pattern returns empty")
        void nullPatternReturnsEmpty() {
            String result = renderer.render(null, Map.of("name", "Alice"));
            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("fields original pattern")
        void emptyFieldsReturnOriginalPattern() {
            String pattern = "无变量文本";
            String result = renderer.render(pattern, Map.of());
            assertThat(result).isEqualTo("无变量文本");
        }

        @Test
        @DisplayName(": field + type + join + if same")
        void mixedSyntax() {
            String pattern =
                    "用户${name:string}，年龄${age:int}，标签${join(tags, \", \")}，${if(email, \"有邮箱\", \"无邮箱\")}";
            Map<String, Object> fields = new HashMap<>();
            fields.put("name", "Alice");
            fields.put("age", 30);
            fields.put("tags", List.of("vip", "active"));
            fields.put("email", "a@b.com");

            String result = renderer.render(pattern, fields);

            assertThat(result).isEqualTo("用户Alice，年龄30，标签vip, active，有邮箱");
        }
    }
}
