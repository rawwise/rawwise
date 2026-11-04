package io.github.rawwise.axiom.pipeline.entity;

public class ExtractionException extends Exception {

    public ExtractionException(String message) {
        super(message);
    }

    public ExtractionException(String message, Throwable cause) {
        super(message, cause);
    }
}
