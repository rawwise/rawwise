package io.github.rawwise.governance.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ConfigConstraints")
class ConfigConstraintsTest {

    private ConfigFactory.Builder defaultBuilder() {
        return ConfigFactory.builder()
                .set(RawWiseOptions.CLUSTER_ID, "default")
                .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                .set(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB, 128L)
                .set(RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB, 4096L)
                .set(RawWiseOptions.BACKPRESSURE_WARNING_THRESHOLD, 0.7)
                .set(RawWiseOptions.BACKPRESSURE_THROTTLE_THRESHOLD, 0.9)
                .set(RawWiseOptions.BACKPRESSURE_REJECT_THRESHOLD, 0.95)
                .set(RawWiseOptions.COMPACTION_MAX_LEVEL, 3)
                .set(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES, 256L * 1024 * 1024)
                .set(RawWiseOptions.BACKPRESSURE_QUEUE_WEIGHT, 0.5)
                .set(RawWiseOptions.BACKPRESSURE_CPU_WEIGHT, 0.15)
                .set(RawWiseOptions.BACKPRESSURE_MEMORY_WEIGHT, 0.15)
                .set(RawWiseOptions.BACKPRESSURE_DISK_WEIGHT, 0.1)
                .set(RawWiseOptions.BACKPRESSURE_NETWORK_WEIGHT, 0.1);
    }

    @Test
    @DisplayName("LSN binding enabled requires auto-generate snapshot")
    void lsnBindingEnabledRequiresAutoGenerateSnapshot() {
        assertThatThrownBy(
                        () ->
                                defaultBuilder()
                                        .set(RawWiseOptions.CHECKPOINT_LSN_BINDING_ENABLED, true)
                                        .set(
                                                RawWiseOptions.SNAPSHOT_AUTO_GENERATE_ON_CHECKPOINT,
                                                false)
                                        .build())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("LSN binding");
    }

    @Test
    @DisplayName("LSN binding enabled with auto-generate snapshot passes")
    void lsnBindingEnabledWithAutoGenerateSnapshotPasses() {
        RawWiseConfig config =
                defaultBuilder()
                        .set(RawWiseOptions.CHECKPOINT_LSN_BINDING_ENABLED, true)
                        .set(RawWiseOptions.SNAPSHOT_AUTO_GENERATE_ON_CHECKPOINT, true)
                        .build();
        assertThat(config.get(RawWiseOptions.CHECKPOINT_LSN_BINDING_ENABLED)).isTrue();
    }

    @Test
    @DisplayName("LSN binding disabled allows auto-generate snapshot off")
    void lsnBindingDisabledAllowsAutoGenerateSnapshotOff() {
        RawWiseConfig config =
                defaultBuilder()
                        .set(RawWiseOptions.CHECKPOINT_LSN_BINDING_ENABLED, false)
                        .set(RawWiseOptions.SNAPSHOT_AUTO_GENERATE_ON_CHECKPOINT, false)
                        .build();
        assertThat(config.get(RawWiseOptions.SNAPSHOT_AUTO_GENERATE_ON_CHECKPOINT)).isFalse();
    }

    @Test
    @DisplayName("Admin enabled requires non-empty token")
    void adminEnabledRequiresNonEmptyToken() {
        assertThatThrownBy(
                        () ->
                                defaultBuilder()
                                        .set(RawWiseOptions.CONFIG_ADMIN_ENABLED, true)
                                        .set(RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN, "")
                                        .build())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("bearer token is empty");
    }

    @Test
    @DisplayName("Admin enabled requires minimum token length")
    void adminEnabledRequiresMinTokenLength() {
        assertThatThrownBy(
                        () ->
                                defaultBuilder()
                                        .set(RawWiseOptions.CONFIG_ADMIN_ENABLED, true)
                                        .set(RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN, "short8char")
                                        .build())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("bearer token too short");
    }

    @Test
    @DisplayName("Admin disabled allows empty token")
    void adminDisabledAllowsEmptyToken() {
        RawWiseConfig config =
                defaultBuilder()
                        .set(RawWiseOptions.CONFIG_ADMIN_ENABLED, false)
                        .set(RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN, "")
                        .build();
        assertThat(config.get(RawWiseOptions.CONFIG_ADMIN_ENABLED)).isFalse();
    }

    @Test
    @DisplayName("Admin enabled rejects invalid CIDR")
    void adminEnabledRejectsInvalidCidr() {
        assertThatThrownBy(
                        () ->
                                defaultBuilder()
                                        .set(RawWiseOptions.CONFIG_ADMIN_ENABLED, true)
                                        .set(
                                                RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN,
                                                "a-valid-long-token-here")
                                        .set(
                                                RawWiseOptions.CONFIG_ADMIN_ALLOWED_CIDRS,
                                                List.of("not-a-cidr"))
                                        .build())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("invalid CIDR format");
    }

    @Test
    @DisplayName("Admin enabled accepts valid CIDR and long token")
    void adminEnabledAcceptsValidCidrAndLongToken() {
        RawWiseConfig config =
                defaultBuilder()
                        .set(RawWiseOptions.CONFIG_ADMIN_ENABLED, true)
                        .set(RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN, "a-valid-long-token-here")
                        .set(
                                RawWiseOptions.CONFIG_ADMIN_ALLOWED_CIDRS,
                                List.of("127.0.0.1/32", "10.0.0.0/8"))
                        .build();
        assertThat(config.get(RawWiseOptions.CONFIG_ADMIN_ENABLED)).isTrue();
    }

    @Test
    @DisplayName("allKeys matches KEY_MAP keySet")
    void allKeysMatchesKeyMapKeySet() throws Exception {
        Map<String, ConfigOption<?>> keyMap;
        Map<String, ConfigOption<?>> map = new java.util.HashMap<>();
        for (Field f : RawWiseOptions.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())
                    && ConfigOption.class.isAssignableFrom(f.getType())) {
                ConfigOption<?> opt = (ConfigOption<?>) f.get(null);
                if (opt != null) {
                    map.putIfAbsent(opt.key(), opt);
                }
            }
        }
        keyMap = map;

        Set<String> allKeys = RawWiseOptions.allKeys();
        assertThat(allKeys).containsExactlyInAnyOrderElementsOf(keyMap.keySet());
    }
}
