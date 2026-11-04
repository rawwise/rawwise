package io.github.rawwise.engine.storage.reader;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

public final class ReaderConfig implements Serializable {
    private static final long serialVersionUID = 3L;

    private final ReaderType type;
    private final String fileScheme;
    private final String filePath;
    private final Long snapshotId;
    private final byte[] serializedSchema;
    private final Map<String, String> extraProps;

    private ReaderConfig(Builder builder) {
        this.type = Objects.requireNonNull(builder.type, "type must not be null");
        this.fileScheme = builder.fileScheme;
        this.filePath = builder.filePath;
        this.snapshotId = builder.snapshotId;
        this.serializedSchema =
                builder.serializedSchema != null ? builder.serializedSchema.clone() : null;
        this.extraProps =
                builder.extraProps != null
                        ? Collections.unmodifiableMap(new HashMap<>(builder.extraProps))
                        : Collections.emptyMap();
    }

    public ReaderType type() {
        return type;
    }

    public Optional<String> fileScheme() {
        return Optional.ofNullable(fileScheme);
    }

    public String filePath() {
        return filePath;
    }

    public OptionalLong snapshotId() {
        return snapshotId != null ? OptionalLong.of(snapshotId) : OptionalLong.empty();
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

    public String extraProp(String key, String defaultValue) {
        return extraProps.getOrDefault(key, defaultValue);
    }

    public boolean isLocalFile() {
        if (fileScheme != null) return "file".equals(fileScheme);
        return filePath != null && filePath.startsWith("file://");
    }

    public boolean isHdfsFile() {
        if (fileScheme != null) return "hdfs".equals(fileScheme) || "viewfs".equals(fileScheme);
        return filePath != null
                && (filePath.startsWith("hdfs://") || filePath.startsWith("viewfs://"));
    }

    public static Builder builder(ReaderType type) {
        return new Builder(type);
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        throw new InvalidObjectException("Use SerializationProxy");
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 3L;
        private final ReaderType type;
        private final String fileScheme;
        private final String filePath;
        private final Long snapshotId;
        private final byte[] serializedSchema;
        private final Map<String, String> extraProps;

        SerializationProxy(ReaderConfig config) {
            this.type = config.type;
            this.fileScheme = config.fileScheme;
            this.filePath = config.filePath;
            this.snapshotId = config.snapshotId;
            this.serializedSchema = config.serializedSchema();
            this.extraProps = config.extraProps;
        }

        private Object readResolve() {
            ReaderConfig.Builder b =
                    ReaderConfig.builder(type).fileScheme(fileScheme).filePath(filePath);
            if (snapshotId != null) {
                b.snapshotId(snapshotId);
            }
            return b.serializedSchema(serializedSchema).extraProps(extraProps).build();
        }
    }

    public static final class Builder {
        private final ReaderType type;
        private String fileScheme;
        private String filePath;
        private Long snapshotId;
        private byte[] serializedSchema;
        private Map<String, String> extraProps;

        private Builder(ReaderType type) {
            this.type = type;
        }

        public Builder fileScheme(String fileScheme) {
            this.fileScheme = fileScheme;
            return this;
        }

        public Builder filePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder filePath(String scheme, String path) {
            this.fileScheme = scheme;
            this.filePath = path;
            return this;
        }

        public Builder snapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder serializedSchema(byte[] serializedSchema) {
            this.serializedSchema = serializedSchema != null ? serializedSchema.clone() : null;
            return this;
        }

        public Builder extraProp(String key, String value) {
            if (this.extraProps == null) {
                this.extraProps = new HashMap<>();
            }
            this.extraProps.put(key, value);
            return this;
        }

        public Builder extraProps(Map<String, String> extraProps) {
            this.extraProps = extraProps != null ? new HashMap<>(extraProps) : null;
            return this;
        }

        public ReaderConfig build() {
            return new ReaderConfig(this);
        }
    }
}
