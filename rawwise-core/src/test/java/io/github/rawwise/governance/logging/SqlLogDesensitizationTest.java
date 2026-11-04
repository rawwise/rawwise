package io.github.rawwise.governance.logging;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.governance.config.RawWiseOptions;
import org.junit.jupiter.api.Test;

class SqlLogDesensitizationTest {

    @Test
    void sqlLevelConfigDefaultIsDebug() {
        assertEquals("DEBUG", RawWiseOptions.LOGGING_SQL_LEVEL.defaultValue());
    }

    @Test
    void sqlLevelConfigAcceptsValidValues() {
        assertDoesNotThrow(() -> RawWiseOptions.LOGGING_SQL_LEVEL.validate("DEBUG"));
        assertDoesNotThrow(() -> RawWiseOptions.LOGGING_SQL_LEVEL.validate("INFO"));
    }

    @Test
    void sqlLevelConfigRejectsInvalidValues() {
        assertThrows(Exception.class, () -> RawWiseOptions.LOGGING_SQL_LEVEL.validate("TRACE"));
        assertThrows(Exception.class, () -> RawWiseOptions.LOGGING_SQL_LEVEL.validate("WARN"));
        assertThrows(Exception.class, () -> RawWiseOptions.LOGGING_SQL_LEVEL.validate("random"));
    }

    @Test
    void sqlLevelConfigKey() {
        assertEquals("rawwise.logging.sql-level", RawWiseOptions.LOGGING_SQL_LEVEL.key());
    }
}
