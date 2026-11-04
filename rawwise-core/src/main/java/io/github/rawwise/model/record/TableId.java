package io.github.rawwise.model.record;

public record TableId(String database, String tableName) {

    public static TableId parse(String fullName) {
        if (fullName == null || fullName.isBlank()) {
            throw new IllegalArgumentException("fullName must not be blank");
        }
        int dot = fullName.indexOf('.');
        if (dot <= 0 || dot == fullName.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid table identifier: " + fullName + " (expected 'database.table')");
        }
        return new TableId(fullName.substring(0, dot), fullName.substring(dot + 1));
    }

    /**
     * Lenient parser that extracts (database, table Name) from various identifier formats:
     *
     * <ul>
     *   <li>4-segment (uniq Id): {@code catalog.database.table.numeric Id} → (database, table)
     *   <li>3-segment (full Name): {@code catalog.database.table} → (database, table)
     *   <li>2-segment: {@code database.table} → (database, table)
     *   <li>1-segment: {@code table} → ("default", table)
     * </ul>
     */
    public static TableId parseLenient(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException("identifier must not be blank");
        }
        String[] parts = identifier.split("\\.");
        if (parts.length >= 3) {
            return new TableId(parts[1], parts[2]);
        }
        if (parts.length == 2) {
            return new TableId(parts[0], parts[1]);
        }
        return new TableId("default", identifier);
    }

    public String toPath() {
        return database + "/" + tableName;
    }
}
