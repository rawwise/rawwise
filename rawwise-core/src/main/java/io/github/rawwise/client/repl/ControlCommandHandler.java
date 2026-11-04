package io.github.rawwise.client.repl;

import static io.github.rawwise.client.repl.AnsiColors.*;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ControlCommandHandler implements CommandHandler {

    private static final long SHELL_TIMEOUT_SECONDS = 30;

    private final ReplSession session;

    public ControlCommandHandler(ReplSession session) {
        this.session = session;
    }

    @Override
    public CommandResult handle(String input) {
        String trimmed = input.trim();

        if (trimmed.equals("\\q")
                || trimmed.equalsIgnoreCase("exit")
                || trimmed.equalsIgnoreCase("quit")) {
            return CommandResult.exit();
        }

        if (trimmed.equals("\\?")) {
            return CommandResult.of(getHelpText());
        }

        if (trimmed.equals("\\timing")) {
            boolean enabled = session.toggleTiming();
            return CommandResult.of(
                    "  Timing " + (enabled ? BRIGHT_GREEN + "on" : DIM + "off") + RESET);
        }

        if (trimmed.startsWith("\\format")) {
            String[] parts = trimmed.split("\\s+");
            if (parts.length < 2) {
                return CommandResult.of(
                        DIM
                                + "  Usage: "
                                + RESET
                                + BRIGHT_WHITE
                                + "\\format table|csv|json"
                                + RESET);
            }
            try {
                ReplSession.Format format = ReplSession.Format.valueOf(parts[1].toUpperCase());
                session.setOutputFormat(format);
                return CommandResult.of(
                        "  Format changed to: "
                                + BRIGHT_CYAN
                                + format.name().toLowerCase()
                                + RESET);
            } catch (IllegalArgumentException e) {
                return CommandResult.of(
                        DIM
                                + "  Unknown format: "
                                + RESET
                                + parts[1]
                                + DIM
                                + ". Use: table, csv, json"
                                + RESET);
            }
        }

        if (trimmed.startsWith("\\!")) {
            String command = trimmed.substring(2).trim();
            if (command.isEmpty()) {
                return CommandResult.of(
                        DIM + "  Usage: " + RESET + BRIGHT_WHITE + "\\! <command>" + RESET);
            }
            try {
                Process process =
                        new ProcessBuilder("sh", "-c", command).redirectErrorStream(true).start();
                boolean finished = process.waitFor(SHELL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!finished) {
                    process.destroyForcibly();
                    return CommandResult.of(
                            BRIGHT_YELLOW
                                    + "  Command timed out after "
                                    + SHELL_TIMEOUT_SECONDS
                                    + "s"
                                    + RESET);
                }
                String output = new String(process.getInputStream().readAllBytes());
                if (output.isEmpty()) {
                    return CommandResult.of(
                            process.exitValue() == 0
                                    ? ""
                                    : DIM + "  Exit code: " + process.exitValue() + RESET);
                }
                return CommandResult.of(
                        output.trim()
                                + (process.exitValue() != 0
                                        ? DIM + "\n  Exit code: " + process.exitValue() + RESET
                                        : ""));
            } catch (Exception e) {
                return CommandResult.of(
                        BRIGHT_RED + "  Failed to execute: " + e.getMessage() + RESET);
            }
        }

        return CommandResult.of(DIM + "  Unknown command: " + input + RESET);
    }

    private String getHelpText() {
        return "\n"
                + BRIGHT_MAGENTA
                + BOLD
                + "  RawWise REPL Commands"
                + RESET
                + "\n"
                + DIM
                + "  ──────────────────────"
                + RESET
                + "\n"
                + "\n"
                + BRIGHT_CYAN
                + "  Query:"
                + RESET
                + "\n"
                + "    SQL           Execute SQL query\n"
                + "\n"
                + BRIGHT_CYAN
                + "  Meta:"
                + RESET
                + "\n"
                + "    \\l            List databases\n"
                + "    \\dt           List tables\n"
                + "    \\d <table>    Describe table\n"
                + "    \\dp <table>   Show partitions\n"
                + "    \\du           Show session info\n"
                + "\n"
                + BRIGHT_CYAN
                + "  Session:"
                + RESET
                + "\n"
                + "    USE <db>      Switch database\n"
                + "    SET k=v       Set session variable\n"
                + "    \\timing       Toggle execution time display\n"
                + "    \\format       Set output format (table|csv|json)\n"
                + "\n"
                + BRIGHT_CYAN
                + "  Other:"
                + RESET
                + "\n"
                + "    \\! <cmd>      Execute system command\n"
                + "    \\q            "
                + BRIGHT_YELLOW
                + "Exit REPL"
                + RESET
                + "\n";
    }
}
