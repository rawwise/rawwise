package io.github.rawwise.client.repl;

import static io.github.rawwise.client.repl.AnsiColors.*;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionCommandHandler implements CommandHandler {

    private final ReplSession session;

    public SessionCommandHandler(ReplSession session) {
        this.session = session;
    }

    @Override
    public CommandResult handle(String input) {
        String trimmed = input.trim();
        String upper = trimmed.toUpperCase();

        if (upper.startsWith("USE ")) {
            String database = trimmed.substring(4).trim();
            if (database.isEmpty()) {
                return CommandResult.of(
                        DIM + "  Usage: " + RESET + BRIGHT_WHITE + "USE <database>" + RESET);
            }
            database = database.replace(";", "").trim();
            session.setCurrentDatabase(database);
            return CommandResult.of(
                    DIM + "  Database changed to: " + RESET + BRIGHT_MAGENTA + database + RESET);
        }

        if (upper.startsWith("SET ")) {
            String assignment = trimmed.substring(4).trim();
            String[] parts = assignment.split("=", 2);
            if (parts.length != 2) {
                return CommandResult.of(
                        DIM + "  Usage: " + RESET + BRIGHT_WHITE + "SET <key>=<value>" + RESET);
            }
            String key = parts[0].trim();
            String value = parts[1].trim();
            session.setVariable(key, value);
            return CommandResult.of(
                    "  "
                            + BRIGHT_CYAN
                            + key
                            + RESET
                            + DIM
                            + " = "
                            + RESET
                            + BRIGHT_WHITE
                            + value
                            + RESET);
        }

        if (upper.equals("SHOW VARIABLES")) {
            Map<String, String> variables = session.getVariables();
            if (variables.isEmpty()) {
                return CommandResult.of(DIM + "  No session variables set" + RESET);
            }
            StringBuilder sb = new StringBuilder();
            sb.append(BRIGHT_CYAN)
                    .append(BOLD)
                    .append("  Session Variables")
                    .append(RESET)
                    .append("\n");
            sb.append(DIM).append("  ──────────────────").append(RESET).append("\n");
            variables.forEach(
                    (k, v) ->
                            sb.append("  ")
                                    .append(BRIGHT_CYAN)
                                    .append(k)
                                    .append(RESET)
                                    .append(DIM)
                                    .append(" = ")
                                    .append(RESET)
                                    .append(v)
                                    .append('\n'));
            return CommandResult.of(sb.toString().trim());
        }

        return CommandResult.of(DIM + "  Unknown session command: " + input + RESET);
    }
}
