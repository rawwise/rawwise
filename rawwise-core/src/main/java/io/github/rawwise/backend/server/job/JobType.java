package io.github.rawwise.backend.server.job;

public enum JobType {
    READ,
    WRITE,
    DDL;

    public static JobType fromString(String value) {
        if (value == null) {
            return READ;
        }
        try {
            return JobType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return READ;
        }
    }
}
