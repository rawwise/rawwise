package io.github.rawwise.engine.storage.reader;

import java.io.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class ReaderType implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String value;

    private ReaderType(String value) {
        this.value = Objects.requireNonNull(value);
    }

    public String value() {
        return value;
    }

    public static final ReaderType BLOCK_PARQUET = new ReaderType("block-parquet");
    public static final ReaderType BLOCK_ORC = new ReaderType("block-orc");
    public static final ReaderType BLOCK_LMDB = new ReaderType("block-lmdb");
    public static final ReaderType TABLE_ROUTING = new ReaderType("table-routing");
    public static final ReaderType TABLE_LMDB = new ReaderType("table-lmdb");
    public static final ReaderType WAL = new ReaderType("wal");
    public static final ReaderType MANIFEST_FILE = new ReaderType("manifest-file");
    public static final ReaderType MANIFEST_LIST = new ReaderType("manifest-list");
    public static final ReaderType UPDATE_LOG = new ReaderType("update-log");
    public static final ReaderType SORTED_FILE_PARQUET = new ReaderType("sorted-file-parquet");
    public static final ReaderType BLOCK_COLD = new ReaderType("block-cold");

    private static final ConcurrentHashMap<String, ReaderType> CACHE = new ConcurrentHashMap<>();

    static {
        CACHE.put("block-parquet", BLOCK_PARQUET);
        CACHE.put("block-orc", BLOCK_ORC);
        CACHE.put("block-lmdb", BLOCK_LMDB);
        CACHE.put("table-routing", TABLE_ROUTING);
        CACHE.put("table-lmdb", TABLE_LMDB);
        CACHE.put("wal", WAL);
        CACHE.put("manifest-file", MANIFEST_FILE);
        CACHE.put("manifest-list", MANIFEST_LIST);
        CACHE.put("update-log", UPDATE_LOG);
        CACHE.put("sorted-file-parquet", SORTED_FILE_PARQUET);
        CACHE.put("block-cold", BLOCK_COLD);
    }

    public static ReaderType of(String value) {
        return CACHE.computeIfAbsent(value, ReaderType::new);
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        throw new InvalidObjectException("Use SerializationProxy");
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String value;

        SerializationProxy(ReaderType rt) {
            this.value = rt.value;
        }

        private Object readResolve() {
            return ReaderType.of(value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReaderType that)) return false;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
