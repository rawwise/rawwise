package io.github.rawwise.model.record;

public record Raw(Record.Key key, Record.Header header, Record.Body body) {
    public static Raw of(Record.Key key, Record.Header header, Record.Body body) {
        return new Raw(key, header, body);
    }
}
