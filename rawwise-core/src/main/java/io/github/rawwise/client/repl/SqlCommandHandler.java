package io.github.rawwise.client.repl;

import static io.github.rawwise.client.repl.AnsiColors.*;

import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SqlCommandHandler implements CommandHandler {

    private final ReplSession session;
    private final FrontendHttpClient client;

    public SqlCommandHandler(ReplSession session, FrontendHttpClient client) {
        this.session = session;
        this.client = client;
    }

    @Override
    public CommandResult handle(String sql) {
        long startTime = System.currentTimeMillis();

        QueryResponseDto response =
                client.executeSync(sql, session.getCurrentDatabase(), session.getQueryTimeout());

        long elapsedMs = System.currentTimeMillis() - startTime;
        session.setLastQueryTimeMs(elapsedMs);

        if (response.getError() != null) {
            String output = BRIGHT_RED + "Error: " + response.getError().getMessage() + RESET;
            if (session.isTimingEnabled()) {
                output += "\n" + formatTime(elapsedMs);
            }
            return CommandResult.of(output);
        }

        if (response.getData() != null) {
            String formatted =
                    ResultFormatter.format(response.getData(), session.getOutputFormat());
            int rowCount =
                    response.getData().getRowCount() != null ? response.getData().getRowCount() : 0;
            if (session.isTimingEnabled()) {
                String timeInfo = formatTime(elapsedMs);
                formatted +=
                        "\n"
                                + timeInfo
                                + DIM
                                + " | "
                                + rowCount
                                + " row"
                                + (rowCount != 1 ? "s" : "")
                                + RESET;
            }
            return CommandResult.of(formatted);
        }

        String state = response.getState() != null ? response.getState() : "UNKNOWN";
        String output = BRIGHT_CYAN + "Query " + state.toLowerCase() + RESET;
        if (session.isTimingEnabled()) {
            output += "\n" + formatTime(elapsedMs);
        }
        return CommandResult.of(output);
    }

    private String formatTime(long elapsedMs) {
        String timeStr;
        if (elapsedMs < 1000) {
            timeStr = elapsedMs + " ms";
        } else {
            timeStr = String.format("%.2f s", elapsedMs / 1000.0);
        }

        String color;
        if (elapsedMs < 500) {
            color = BRIGHT_GREEN;
        } else if (elapsedMs < 3000) {
            color = BRIGHT_YELLOW;
        } else {
            color = BRIGHT_RED;
        }

        return DIM + "(" + color + timeStr + DIM + ")" + RESET;
    }
}
