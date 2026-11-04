package io.github.rawwise.engine.storage.writer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

public final class WriterConfig implements Serializable {
    @Serial private static final long serialVersionUID = 2L;

    private final String type;
    private final String filePath;
    private final String fileScheme;
    private final Long maxFileSize;
    private final Long syncIntervalMs;
    private final byte[] serializedSchema;
    private final Map<String, String> extraProps;

    private WriterConfig(Builder builder) {
        this.type = builder.type;
        this.filePath = builder.filePath;
        this.fileScheme = builder.fileScheme;
        this.maxFileSize = builder.maxFileSize;
        this.syncIntervalMs = builder.syncIntervalMs;
        this.serializedSchema =
                builder.serializedSchema != null ? builder.serializedSchema.clone() : null;
        this.extraProps =
                builder.extraProps != null
                        ? Map.copyOf(builder.extraProps)
                        : Collections.emptyMap();
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (serializedSchema != null) {
            try {
                Field f = WriterConfig.class.getDeclaredField("serializedSchema");
                f.setAccessible(true);
                f.set(this, serializedSchema.clone());
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new IOException("Failed to defensively copy serializedSchema", e);
            }
        }
        if (extraProps != null) {
            try {
                Field f = WriterConfig.class.getDeclaredField("extraProps");
                f.setAccessible(true);
                f.set(this, Collections.unmodifiableMap(new HashMap<>(extraProps)));
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new IOException("Failed to defensively copy extraProps", e);
            }
        }
    }

    public static Builder builder(String type) {
        return new Builder(type);
    }

    public String type() {
        return type;
    }

    public String filePath() {
        return filePath;
    }

    public String fileScheme() {
        return fileScheme;
    }

    public OptionalLong maxFileSize() {
        return maxFileSize != null ? OptionalLong.of(maxFileSize) : OptionalLong.empty();
    }

    public OptionalLong syncIntervalMs() {
        return syncIntervalMs != null ? OptionalLong.of(syncIntervalMs) : OptionalLong.empty();
    }

    public byte[] serializedSchema() {
        return serializedSchema != null ? serializedSchema.clone() : null;
    }

    public Map<String, String> extraProps() {
        return extraProps;
    }

    public String extraProp(String key) {
        return extraProps.get(key);
    }

    public static final class Builder {
        private final String type;
        private String filePath;
        private String fileScheme;
        private Long maxFileSize;
        private Long syncIntervalMs;
        private byte[] serializedSchema;
        private Map<String, String> extraProps;

        private Builder(String type) {
            this.type = Objects.requireNonNull(type, "type must not be null");
        }

        public Builder filePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder fileScheme(String fileScheme) {
            this.fileScheme = fileScheme;
            return this;
        }

        public Builder maxFileSize(Long maxFileSize) {
            this.maxFileSize = maxFileSize;
            return this;
        }

        public Builder syncIntervalMs(Long syncIntervalMs) {
            this.syncIntervalMs = syncIntervalMs;
            return this;
        }

        public Builder serializedSchema(byte[] serializedSchema) {
            this.serializedSchema = serializedSchema != null ? serializedSchema.clone() : null;
            return this;
        }

        public Builder extraProps(Map<String, String> extraProps) {
            this.extraProps = extraProps;
            return this;
        }

        public Builder extraProp(String key, String value) {
            if (this.extraProps == null) this.extraProps = new HashMap<>();
            this.extraProps.put(key, value);
            return this;
        }

        public WriterConfig build() {
            return new WriterConfig(this);
        }
    }
}
