package io.github.rawwise.governance.context;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.rawwise.axiom.memory.Depth;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record ContextEntry(
        String key,
        String value,
        String contentType,
        String encoding,
        int version,
        Map<String, String> metadata,
        long createdAt,
        long updatedAt,
        String l0Abstract,
        String l1Overview,
        boolean l0Generated,
        boolean l1Generated,
        long l0TokenCount,
        long l1TokenCount,
        int importance,
        float decayFactor,
        String summarizationModel,
        long summarizationTimestamp,
        long l2ByteSize,
        long lastAccessedAt,
        int accessCount) {

    public ContextEntry {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty");
        }
        if (encoding == null) {
            encoding = "utf-8";
        }
        if (metadata == null) {
            metadata = Map.of();
        } else {
            metadata = Map.copyOf(metadata);
        }
        if (l0Abstract == null) {
            l0Abstract = "";
        }
        if (l1Overview == null) {
            l1Overview = "";
        }
        if (summarizationModel == null) {
            summarizationModel = "";
        }
        if (importance < 0 || importance > 10) {
            throw new IllegalArgumentException("importance must be in [0, 10], got: " + importance);
        }
        if (decayFactor < 0.0f || decayFactor > 1.0f) {
            throw new IllegalArgumentException(
                    "decayFactor must be in [0.0, 1.0], got: " + decayFactor);
        }
    }

    @Override
    public Map<String, String> metadata() {
        return metadata;
    }

    @JsonIgnore
    public boolean isTieredReady() {
        return l0Generated && l1Generated;
    }

    public Optional<String> getContentAtTier(Depth depth) {
        return switch (depth) {
            case ABSTRACT -> {
                if (!l0Abstract.isEmpty()) {
                    yield Optional.of(l0Abstract);
                }
                yield l1Overview.isEmpty() ? Optional.empty() : Optional.of(l1Overview);
            }
            case DETAIL -> value == null ? Optional.empty() : Optional.of(value);
        };
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ContextEntry that
                && key.equals(that.key)
                && Objects.equals(value, that.value)
                && version == that.version
                && createdAt == that.createdAt
                && updatedAt == that.updatedAt;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, version, createdAt, updatedAt);
    }

    @Override
    public String toString() {
        return "ContextEntry[key="
                + key
                + ", version="
                + version
                + ", contentType="
                + contentType
                + ", importance="
                + importance
                + ", l0Generated="
                + l0Generated
                + ", l1Generated="
                + l1Generated
                + "]";
    }

    public static Builder builder(String key, String value) {
        return new Builder(key, value);
    }

    public static class Builder {
        private final String key;
        private final String value;
        private String contentType = "text/plain";
        private String encoding = "utf-8";
        private int version = 1;
        private Map<String, String> metadata = Map.of();
        private long createdAt = System.currentTimeMillis();
        private long updatedAt = System.currentTimeMillis();
        private String l0Abstract = "";
        private String l1Overview = "";
        private boolean l0Generated;
        private boolean l1Generated;
        private long l0TokenCount;
        private long l1TokenCount;
        private int importance = 5;
        private float decayFactor = 1.0f;
        private String summarizationModel = "";
        private long summarizationTimestamp;
        private long l2ByteSize;
        private long lastAccessedAt;
        private int accessCount;

        private Builder(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder version(int version) {
            this.version = version;
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder createdAt(long createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder updatedAt(long updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder l0Abstract(String l0Abstract) {
            this.l0Abstract = l0Abstract;
            return this;
        }

        public Builder l1Overview(String l1Overview) {
            this.l1Overview = l1Overview;
            return this;
        }

        public Builder l0Generated(boolean l0Generated) {
            this.l0Generated = l0Generated;
            return this;
        }

        public Builder l1Generated(boolean l1Generated) {
            this.l1Generated = l1Generated;
            return this;
        }

        public Builder l0TokenCount(long l0TokenCount) {
            this.l0TokenCount = l0TokenCount;
            return this;
        }

        public Builder l1TokenCount(long l1TokenCount) {
            this.l1TokenCount = l1TokenCount;
            return this;
        }

        public Builder importance(int importance) {
            this.importance = importance;
            return this;
        }

        public Builder decayFactor(float decayFactor) {
            this.decayFactor = decayFactor;
            return this;
        }

        public Builder summarizationModel(String summarizationModel) {
            this.summarizationModel = summarizationModel;
            return this;
        }

        public Builder summarizationTimestamp(long summarizationTimestamp) {
            this.summarizationTimestamp = summarizationTimestamp;
            return this;
        }

        public Builder l2ByteSize(long l2ByteSize) {
            this.l2ByteSize = l2ByteSize;
            return this;
        }

        public Builder lastAccessedAt(long lastAccessedAt) {
            this.lastAccessedAt = lastAccessedAt;
            return this;
        }

        public Builder accessCount(int accessCount) {
            this.accessCount = accessCount;
            return this;
        }

        public ContextEntry build() {
            return new ContextEntry(
                    key,
                    value,
                    contentType,
                    encoding,
                    version,
                    metadata,
                    createdAt,
                    updatedAt,
                    l0Abstract,
                    l1Overview,
                    l0Generated,
                    l1Generated,
                    l0TokenCount,
                    l1TokenCount,
                    importance,
                    decayFactor,
                    summarizationModel,
                    summarizationTimestamp,
                    l2ByteSize,
                    lastAccessedAt,
                    accessCount);
        }
    }
}
