package io.github.rawwise.client.repl;

import static io.github.rawwise.client.repl.AnsiColors.*;

import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetaCommandHandler implements CommandHandler {

    private final ReplSession session;
    private final FrontendHttpClient client;

    public MetaCommandHandler(ReplSession session, FrontendHttpClient client) {
        this.session = session;
        this.client = client;
    }

    @Override
    public CommandResult handle(String input) {
        String trimmed = input.trim();

        if (trimmed.equals("\\l")) {
            return CommandResult.of(handleListDatabases());
        }
        if (trimmed.equals("\\dt")) {
            return CommandResult.of(handleListTables());
        }
        if (trimmed.startsWith("\\d ")
                && !trimmed.startsWith("\\dt")
                && !trimmed.startsWith("\\dp")) {
            String tableName = trimmed.substring(3).trim();
            return CommandResult.of(handleDescribeTable(tableName));
        }
        if (trimmed.startsWith("\\dp ")) {
            String tableName = trimmed.substring(4).trim();
            return CommandResult.of(handleShowPartitions(tableName));
        }
        if (trimmed.equals("\\du")) {
            return CommandResult.of(handleSessionInfo());
        }

        return CommandResult.of(
                DIM
                        + "Unknown command: "
                        + trimmed
                        + ". Type "
                        + RESET
                        + BRIGHT_WHITE
                        + "\\?"
                        + RESET
                        + DIM
                        + " for help."
                        + RESET);
    }

    private String handleListDatabases() {
        List<String> databases = client.listDatabases();
        StringBuilder sb = new StringBuilder();
        sb.append(BRIGHT_CYAN).append(BOLD).append("  Databases").append(RESET).append("\n");
        sb.append(DIM).append("  ─────────").append(RESET).append("\n");
        for (String db : databases) {
            if (db.equals(session.getCurrentDatabase())) {
                sb.append("  ")
                        .append(BRIGHT_GREEN)
                        .append(db)
                        .append(RESET)
                        .append(DIM)
                        .append(" (current)")
                        .append(RESET);
            } else {
                sb.append("  ").append(db);
            }
            sb.append('\n');
        }
        return sb.toString().trim();
    }

    private String handleListTables() {
        List<String> tables = client.listTables(session.getCurrentDatabase());
        if (tables.isEmpty()) {
            return DIM
                    + "  No tables found in "
                    + BRIGHT_CYAN
                    + session.getCurrentDatabase()
                    + RESET;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(BRIGHT_CYAN)
                .append(BOLD)
                .append("  Tables in ")
                .append(BRIGHT_MAGENTA)
                .append(session.getCurrentDatabase())
                .append(RESET)
                .append("\n");
        sb.append(DIM).append("  ──────────────────────").append(RESET).append("\n");
        for (String table : tables) {
            sb.append("  ").append(table).append('\n');
        }
        return sb.toString().trim();
    }

    private String handleDescribeTable(String tableName) {
        return DIM
                + "  Describe table "
                + RESET
                + BRIGHT_CYAN
                + tableName
                + RESET
                + DIM
                + " (not yet implemented via HTTP API)"
                + RESET;
    }

    private String handleShowPartitions(String tableName) {
        return DIM
                + "  Partition info for "
                + RESET
                + BRIGHT_CYAN
                + tableName
                + RESET
                + DIM
                + " (not yet implemented)"
                + RESET;
    }

    private String handleSessionInfo() {
        return BRIGHT_CYAN
                + BOLD
                + "  Session Info"
                + RESET
                + "\n"
                + DIM
                + "  ────────────"
                + RESET
                + "\n"
                + "  Database: "
                + BRIGHT_MAGENTA
                + session.getCurrentDatabase()
                + RESET
                + "\n"
                + "  Format:   "
                + BRIGHT_CYAN
                + session.getOutputFormat().name().toLowerCase()
                + RESET
                + "\n"
                + "  Timing:   "
                + (session.isTimingEnabled() ? BRIGHT_GREEN + "on" : DIM + "off")
                + RESET;
    }
}
