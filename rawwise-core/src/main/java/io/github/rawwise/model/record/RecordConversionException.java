package io.github.rawwise.model.record;

public class RecordConversionException extends RuntimeException {
    private final int rowIndex;

    public RecordConversionException(int rowIndex, String message, Throwable cause) {
        super("Row " + rowIndex + ": " + message, cause);
        this.rowIndex = rowIndex;
    }

    public int rowIndex() {
        return rowIndex;
    }
}
