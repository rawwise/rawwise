package io.github.rawwise.common.exception;

public final class TableNotFoundException extends CatalogException {

    private static final long serialVersionUID = 1L;

    private final String tableIdentifier;

    public TableNotFoundException(String message) {
        super(message);
        this.tableIdentifier = null;
    }

    public TableNotFoundException(String message, String tableIdentifier) {
        super(message);
        this.tableIdentifier = tableIdentifier;
    }

    public TableNotFoundException(String message, Throwable cause) {
        super(message, cause);
        this.tableIdentifier = null;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }
}
