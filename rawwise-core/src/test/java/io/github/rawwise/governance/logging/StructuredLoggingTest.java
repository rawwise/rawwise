package io.github.rawwise.governance.logging;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.governance.config.RawWiseOptions;
import org.junit.jupiter.api.Test;

class StructuredLoggingTest {

    @Test
    void loggingFormatDefaultIsText() {
        assertEquals("TEXT", RawWiseOptions.LOGGING_FORMAT.defaultValue());
    }

    @Test
    void loggingFormatAcceptsValidValues() {
        assertDoesNotThrow(() -> RawWiseOptions.LOGGING_FORMAT.validate("JSON"));
        assertDoesNotThrow(() -> RawWiseOptions.LOGGING_FORMAT.validate("TEXT"));
        assertDoesNotThrow(() -> RawWiseOptions.LOGGING_FORMAT.validate("json"));
        assertDoesNotThrow(() -> RawWiseOptions.LOGGING_FORMAT.validate("text"));
    }

    @Test
    void loggingFormatRejectsInvalidValues() {
        assertThrows(Exception.class, () -> RawWiseOptions.LOGGING_FORMAT.validate("XML"));
        assertThrows(Exception.class, () -> RawWiseOptions.LOGGING_FORMAT.validate("random"));
    }

    @Test
    void loggingFormatConfigKey() {
        assertEquals("rawwise.logging.format", RawWiseOptions.LOGGING_FORMAT.key());
    }

    @Test
    void loggingSqlLevelConfigKey() {
        assertEquals("rawwise.logging.sql-level", RawWiseOptions.LOGGING_SQL_LEVEL.key());
    }
}
