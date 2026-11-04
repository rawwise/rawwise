package io.github.rawwise.client.repl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandExecutor {

    public enum CommandType {
        SQL,
        META,
        SESSION,
        CONTROL
    }

    private final SqlCommandHandler sqlHandler;
    private final MetaCommandHandler metaHandler;
    private final SessionCommandHandler sessionHandler;
    private final ControlCommandHandler controlHandler;

    public CommandExecutor(ReplSession session, FrontendHttpClient client) {
        this.sqlHandler = new SqlCommandHandler(session, client);
        this.metaHandler = new MetaCommandHandler(session, client);
        this.sessionHandler = new SessionCommandHandler(session);
        this.controlHandler = new ControlCommandHandler(session);
    }

    public CommandResult execute(String input) {
        String trimmed = input.trim();
        if (trimmed.isEmpty()) {
            return CommandResult.of("");
        }

        trimmed = stripTrailingSemicolon(trimmed);

        CommandType type = classify(trimmed);
        return switch (type) {
            case SQL -> sqlHandler.handle(trimmed);
            case META -> metaHandler.handle(trimmed);
            case SESSION -> sessionHandler.handle(trimmed);
            case CONTROL -> controlHandler.handle(trimmed);
        };
    }

    CommandType classify(String input) {
        String trimmed = input.trim();
        String upper = trimmed.toUpperCase();

        if (trimmed.startsWith("\\")) {
            if (trimmed.startsWith("\\q")
                    || trimmed.startsWith("\\?")
                    || trimmed.startsWith("\\timing")
                    || trimmed.startsWith("\\format")
                    || trimmed.startsWith("\\!")) {
                return CommandType.CONTROL;
            }
            return CommandType.META;
        }

        if (upper.equals("EXIT") || upper.equals("QUIT")) {
            return CommandType.CONTROL;
        }

        if (upper.startsWith("USE ")) {
            return CommandType.SESSION;
        }
        if (upper.startsWith("SET ")) {
            return CommandType.SESSION;
        }
        if (upper.startsWith("SHOW ")) {
            String showWhat = upper.substring(5).trim();
            if (showWhat.equals("VARIABLES")) {
                return CommandType.SESSION;
            }
            return CommandType.SQL;
        }

        if (upper.startsWith("SELECT")
                || upper.startsWith("INSERT")
                || upper.startsWith("UPDATE")
                || upper.startsWith("DELETE")
                || upper.startsWith("CREATE")
                || upper.startsWith("ALTER")
                || upper.startsWith("DROP")
                || upper.startsWith("TRUNCATE")
                || upper.startsWith("WITH")
                || upper.startsWith("DESCRIBE")
                || upper.startsWith("EXPLAIN")
                || upper.startsWith("ANALYZE")
                || upper.startsWith("MERGE")) {
            return CommandType.SQL;
        }

        return CommandType.SQL;
    }

    private String stripTrailingSemicolon(String input) {
        String trimmed = input.trim();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1).trim();
        }
        return trimmed;
    }
}
