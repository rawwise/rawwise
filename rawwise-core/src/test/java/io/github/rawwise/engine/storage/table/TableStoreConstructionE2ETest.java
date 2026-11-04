package io.github.rawwise.engine.storage.table;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Types;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TableStoreConstructionE2ETest {

    private static final String TABLE_ID = "db.construction_test";
    private static final Schema TEST_SCHEMA =
            Schema.builder()
                    .field("id", Types.int64(false))
                    .field("name", Types.string())
                    .primaryKey("id")
                    .version(1L)
                    .build();

    @Test
    @DisplayName("construct -> BodyCodec non- null")
    void shouldResolveBodyCodecWithFullConstruction() {
        TableStore store =
                new TableStore(
                        TABLE_ID,
                        2,
                        TableStore.createKeyCodec(List.of("id")),
                        () -> (tableId, schemaVersion) -> TEST_SCHEMA,
                        "fixed-offset",
                        42L,
                        TEST_SCHEMA);

        RecordCodec codec = store.getRecordCodec();
        assertThat(codec).isInstanceOf(DefaultRecordCodec.class);
        BodyCodec bodyCodec = ((DefaultRecordCodec) codec).getBodyCodec();
        assertThat(bodyCodec).isNotNull();
        assertThat(bodyCodec.name()).isEqualTo("fixed-offset");
    }

    @Test
    @DisplayName("no schemaResolver has initialSchema -> BodyCodec can")
    void shouldFallbackWhenNoSchemaResolverButHasInitialSchema() {
        TableStore store =
                new TableStore(
                        TABLE_ID,
                        2,
                        TableStore.createKeyCodec(List.of("id")),
                        null,
                        "fixed-offset",
                        42L,
                        TEST_SCHEMA);

        RecordCodec codec = store.getRecordCodec();
        assertThat(codec).isInstanceOf(DefaultRecordCodec.class);
        BodyCodec bodyCodec = ((DefaultRecordCodec) codec).getBodyCodec();
        assertThat(bodyCodec).isNotNull();
        assertThat(bodyCodec.name()).isEqualTo("fixed-offset");
    }

    @Test
    @DisplayName("construct -> BodyCodec is null(ValueCodec path)")
    void shouldReturnNullBodyCodecWhenNoInfo() {
        TableStore store = new TableStore(TABLE_ID, 2);

        RecordCodec codec = store.getRecordCodec();
        assertThat(codec).isInstanceOf(DefaultRecordCodec.class);
        BodyCodec bodyCodec = ((DefaultRecordCodec) codec).getBodyCodec();
        assertThat(bodyCodec).isNull();
    }
}
