package io.github.rawwise.governance.config;

public interface ConfigConstraint {
    String description();

    boolean validate(RawWiseConfig config);

    String errorMessage(RawWiseConfig config);
}
