package io.github.rawwise.client.repl;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

@Slf4j
public class ReplTerminal implements AutoCloseable {

    private static final String CONTINUATION_PROMPT = "           -> ";

    private final ReplSession session;
    private final Terminal terminal;
    private final LineReader lineReader;

    public ReplTerminal(ReplSession session) throws IOException {
        this.session = session;
        this.terminal = TerminalBuilder.builder().system(true).build();

        this.lineReader =
                LineReaderBuilder.builder()
                        .terminal(terminal)
                        .completer(createCompleter())
                        .parser(new DefaultParser())
                        .variable(LineReader.SECONDARY_PROMPT_PATTERN, CONTINUATION_PROMPT)
                        .variable(LineReader.INDENTATION, 2)
                        .build();
    }

    public String readLine() {
        String prompt = buildPrompt();
        try {
            return lineReader.readLine(prompt);
        } catch (UserInterruptException e) {
            return null;
        } catch (EndOfFileException e) {
            return "\\q";
        }
    }

    private String buildPrompt() {
        AttributedStyle magentaBold =
                AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA).bold();
        AttributedStyle cyan = AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN);
        AttributedStyle dim = AttributedStyle.DEFAULT.faint();

        String dbName = session.getCurrentDatabase();
        return new AttributedStringBuilder()
                .style(magentaBold)
                .append("rawwise:")
                .style(cyan)
                .append(dbName)
                .style(dim)
                .append("> ")
                .toAnsi(terminal);
    }

    public void println(String message) {
        if (message != null && !message.isEmpty()) {
            terminal.writer().println(message);
        }
        terminal.writer().flush();
    }

    private Completer createCompleter() {
        StringsCompleter sqlKeywords =
                new StringsCompleter(
                        "SELECT",
                        "FROM",
                        "WHERE",
                        "INSERT",
                        "INTO",
                        "VALUES",
                        "UPDATE",
                        "SET",
                        "DELETE",
                        "CREATE",
                        "TABLE",
                        "ALTER",
                        "DROP",
                        "TRUNCATE",
                        "WITH",
                        "AS",
                        "JOIN",
                        "LEFT",
                        "RIGHT",
                        "INNER",
                        "OUTER",
                        "ON",
                        "AND",
                        "OR",
                        "NOT",
                        "IN",
                        "BETWEEN",
                        "LIKE",
                        "IS",
                        "NULL",
                        "ORDER",
                        "BY",
                        "GROUP",
                        "HAVING",
                        "LIMIT",
                        "OFFSET",
                        "UNION",
                        "ALL",
                        "DISTINCT",
                        "SHOW",
                        "DESCRIBE",
                        "EXPLAIN",
                        "ANALYZE",
                        "USE",
                        "MERGE",
                        "OVER",
                        "PARTITION",
                        "WINDOW");

        StringsCompleter metaCommands =
                new StringsCompleter(
                        "\\l", "\\dt", "\\d", "\\dp", "\\du", "\\q", "\\?", "\\timing", "\\format");

        StringsCompleter controlCommands = new StringsCompleter("exit", "quit");

        return new AggregateCompleter(sqlKeywords, metaCommands, controlCommands);
    }

    @Override
    public void close() {
        try {
            terminal.close();
        } catch (IOException e) {
            log.warn("Failed to close terminal", e);
        }
    }
}
