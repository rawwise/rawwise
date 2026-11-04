package io.github.rawwise.frontend.ir.adaptor;

public class IRAdaptException extends RuntimeException {
    private final String irType;
    private final String field;
    private final String reason;

    public IRAdaptException(String irType, String field, String reason) {
        super(String.format("IR adaptation failed for %s.%s: %s", irType, field, reason));
        this.irType = irType;
        this.field = field;
        this.reason = reason;
    }

    public String irType() {
        return irType;
    }

    public String field() {
        return field;
    }

    public String reason() {
        return reason;
    }
}
