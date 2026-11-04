package io.github.rawwise.client.repl;

import static io.github.rawwise.client.repl.AnsiColors.*;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawWiseRepl {

    private static final String VERSION = "0.1.0";

    private static final String BANNER =
            "\n"
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "  ╔══════════════════════════════════════╗"
                    + RESET
                    + "\n"
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "  ║"
                    + RESET
                    + BRIGHT_CYAN
                    + BOLD
                    + "  _   _      _ _          _  "
                    + RESET
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "║"
                    + RESET
                    + "\n"
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "  ║"
                    + RESET
                    + MAGENTA
                    + BOLD
                    + " | \\ | | ___| | | ___  __| | "
                    + RESET
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "║"
                    + RESET
                    + DIM
                    + " v"
                    + VERSION
                    + "\n"
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "  ║"
                    + RESET
                    + CYAN
                    + BOLD
                    + " |  \\| |/ _ \\ | |/ _ \\/ _` | "
                    + RESET
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "║"
                    + RESET
                    + "\n"
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "  ║"
                    + RESET
                    + BLUE
                    + BOLD
                    + " | |\\  |  __/ | |  __/ (_| | "
                    + RESET
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "║"
                    + RESET
                    + "\n"
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "  ║"
                    + RESET
                    + BRIGHT_BLUE
                    + BOLD
                    + " |_| \\_|\\___|_|_|\\___|\\__,_| "
                    + RESET
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "║"
                    + RESET
                    + "\n"
                    + BRIGHT_MAGENTA
                    + BOLD
                    + "  ╚══════════════════════════════════════╝"
                    + RESET
                    + "\n"
                    + "\n"
                    + DIM
                    + "  Type "
                    + RESET
                    + BRIGHT_WHITE
                    + "\\?"
                    + RESET
                    + DIM
                    + " for help, "
                    + RESET
                    + BRIGHT_WHITE
                    + "\\q"
                    + RESET
                    + DIM
                    + " to exit."
                    + RESET
                    + "\n";

    public static void main(String[] args) {
        String host = getArg(args, "--host", "localhost");
        int port = parsePort(getArg(args, "--port", "8080"));
        String database = getArg(args, "--database", "default");

        ReplSession session = new ReplSession();
        session.setCurrentDatabase(database);

        FrontendHttpClient client = new FrontendHttpClient(host, port);

        log.info(
                "{}Connecting to {}rawwise{} at {}:{}... ",
                DIM,
                RESET + BRIGHT_MAGENTA,
                RESET + DIM,
                host,
                port);

        if (!client.healthCheck()) {
            log.error("{}FAILED{}", BRIGHT_RED + BOLD, RESET);
            log.error("  Cannot connect to Frontend Server at {}:{}", host, port);
            log.error("  Make sure the Frontend Server is running.");
            System.exit(1);
        }

        log.info("{}OK{}", BRIGHT_GREEN + BOLD, RESET);

        if (!client.readyCheck()) {
            log.warn(
                    "{}  Backend Server is not ready. Some features may not work.{}",
                    BRIGHT_YELLOW,
                    RESET);
        }

        log.info(BANNER);

        try (ReplTerminal terminal = new ReplTerminal(session);
                FrontendHttpClient ignored = client) {

            CommandExecutor executor = new CommandExecutor(session, client);

            while (true) {
                String input = terminal.readLine();

                if (input == null) {
                    break;
                }

                String trimmed = input.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }

                CommandResult result = executor.execute(trimmed);

                if (result.shouldExit()) {
                    log.info("{}  Bye!{}", DIM, RESET);
                    break;
                }

                if (result.getOutput() != null && !result.getOutput().isEmpty()) {
                    terminal.println(result.getOutput());
                }
            }

        } catch (Exception e) {
            log.error("REPL error", e);
            log.error("Fatal error: {}", e.getMessage());
            System.exit(1);
        }
    }

    private static String getArg(String[] args, String flag, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }

    private static int parsePort(String portStr) {
        try {
            int port = Integer.parseInt(portStr);
            if (port < 1 || port > 65535) {
                log.error("  Invalid port number: {}. Must be between 1 and 65535.", port);
                System.exit(1);
            }
            return port;
        } catch (NumberFormatException e) {
            log.error("  Invalid port number: {}", portStr);
            System.exit(1);
            return -1;
        }
    }
}
