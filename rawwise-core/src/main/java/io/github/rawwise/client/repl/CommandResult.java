package io.github.rawwise.client.repl;

public class CommandResult {
    private final String output;
    private final boolean shouldExit;

    private CommandResult(String output, boolean shouldExit) {
        this.output = output;
        this.shouldExit = shouldExit;
    }

    public static CommandResult of(String output) {
        return new CommandResult(output, false);
    }

    public static CommandResult exit() {
        return new CommandResult(null, true);
    }

    public String getOutput() {
        return output;
    }

    public boolean shouldExit() {
        return shouldExit;
    }
}
