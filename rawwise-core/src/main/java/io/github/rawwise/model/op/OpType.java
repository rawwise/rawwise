package io.github.rawwise.model.op;

public enum OpType {
    FIND("FIND"),
    KV_SCAN("KV_SCAN"),
    BATCH_SCAN("BATCH_SCAN"),
    KV_WRITE("KV_WRITE"),
    BATCH_WRITE("BATCH_WRITE"),
    APPEND_ONLY_WRITE("APPEND_ONLY_WRITE"),
    INSERT("INSERT"),
    OVERWRITE("OVERWRITE"),
    UPDATE("UPDATE"),
    DELETE("DELETE"),
    COMMIT("COMMIT"),
    ABORT("ABORT"),
    REFRESH("REFRESH"),
    CREATE_TABLE("CREATE_TABLE"),
    DROP_TABLE("DROP_TABLE"),
    ALTER_SCHEMA("ALTER_SCHEMA"),
    MANIFEST_WRITE("MANIFEST_WRITE"),
    SNAPSHOT_UPDATE("SNAPSHOT_UPDATE"),
    FILE_REGISTER("FILE_REGISTER"),
    FILE_UNREGISTER("FILE_UNREGISTER"),
    MANIFEST_DELETE("MANIFEST_DELETE");

    private final String wireName;

    OpType(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    public static OpType fromWireName(String wireName) {
        for (OpType value : values()) {
            if (value.wireName.equals(wireName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown operation type: " + wireName);
    }
}
