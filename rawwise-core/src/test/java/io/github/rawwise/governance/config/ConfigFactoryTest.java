package io.github.rawwise.governance.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ConfigFactory Unit-Aware Coercion")
class ConfigFactoryTest {

    @Test
    @DisplayName("MIB key rejects memory-size literal like '1g'")
    void mibKeyRejectsMemorySizeLiteral() {
        assertThatThrownBy(
                        () ->
                                ConfigFactory.coerceValue(
                                        "rawwise.storage.lmdb.initial-map-size-mb",
                                        "1g",
                                        Long.class))
                .isInstanceOf(ConfigException.TypeCoercionError.class);
    }

    @Test
    @DisplayName("MIB key accepts plain number")
    void mibKeyAcceptsPlainNumber() {
        Object result =
                ConfigFactory.coerceValue(
                        "rawwise.storage.lmdb.initial-map-size-mb", "1024", Long.class);
        assertThat(result).isEqualTo(1024L);
    }

    @Test
    @DisplayName("BYTES key accepts memory-size literal like '1g'")
    void bytesKeyAcceptsMemorySizeLiteral() {
        Object result =
                ConfigFactory.coerceValue(
                        "rawwise.storage.flush.threshold-bytes", "1g", Long.class);
        assertThat(result).isEqualTo(1073741824L);
    }

    @Test
    @DisplayName("BYTES key accepts plain number")
    void bytesKeyAcceptsPlainNumber() {
        Object result =
                ConfigFactory.coerceValue(
                        "rawwise.storage.flush.threshold-bytes", "1073741824", Long.class);
        assertThat(result).isEqualTo(1073741824L);
    }

    @Test
    @DisplayName("PLAIN long key accepts memory-size literal for backward compat")
    void plainLongKeyAcceptsMemorySizeLiteral() {
        Object result = ConfigFactory.coerceValue("rawwise.some.plain.key", "1g", Long.class);
        assertThat(result).isEqualTo(1073741824L);
    }

    @Test
    @DisplayName("KIB key rejects memory-size literal like '1m'")
    void kibKeyRejectsMemorySizeLiteral() {
        assertThatThrownBy(
                        () ->
                                ConfigFactory.coerceValue(
                                        "rawwise.storage.flush.parquet-page-size-kb",
                                        "1m",
                                        Integer.class))
                .isInstanceOf(ConfigException.TypeCoercionError.class);
    }

    @Test
    @DisplayName("KIB key accepts plain number")
    void kibKeyAcceptsPlainNumber() {
        Object result =
                ConfigFactory.coerceValue(
                        "rawwise.storage.flush.parquet-page-size-kb", "1024", Integer.class);
        assertThat(result).isEqualTo(1024);
    }

    @Test
    @DisplayName("fromMap enforces cross-field constraints")
    void fromMapEnforcesCrossFieldConstraints() {
        Map<String, String> map = Map.of("rawwise.storage.durability-level", "NODE");
        assertThatThrownBy(() -> ConfigFactory.fromMap(map))
                .isInstanceOf(ConfigException.ValidationError.class);
    }

    @Test
    @DisplayName("fromMap fills default values for missing keys")
    void fromMapFillsDefaults() {
        RawWiseConfig config = ConfigFactory.fromMap(Map.of());
        assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isNotNull();
        assertThat(config.get(RawWiseOptions.STORAGE_DURABILITY_LEVEL)).isNotNull();
    }

    @Test
    @DisplayName("buildValidated with null value and fillDefaults=true does not throw")
    void buildValidatedSkipsValidateForNullValues() {
        Map<String, Object> values = new HashMap<>();
        values.put("rawwise.cluster.id", null);
        RawWiseConfig config = ConfigFactory.buildValidated(values, false, true);
        assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("default");
    }
}
