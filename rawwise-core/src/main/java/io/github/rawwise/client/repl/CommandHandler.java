package io.github.rawwise.client.repl;

public interface CommandHandler {
    CommandResult handle(String input);
}
