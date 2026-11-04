package io.github.rawwise.model.record;

public interface RecordCodec {
    byte[] encode(Record record);

    Record decode(byte[] data, SchemaResolver schemaResolver);

    byte[] extractKey(byte[] data);

    long extractSequenceNumber(byte[] data);

    long extractCommitTimestamp(byte[] data);

    Record.Kind extractKind(byte[] data);

    boolean extractVisibility(byte[] data);

    int extractFlags(byte[] data);

    boolean isRecordCodecFormat(byte[] data);

    @FunctionalInterface
    interface SchemaResolver {
        Schema resolve(long tableId, long schemaVersion);
    }
}
