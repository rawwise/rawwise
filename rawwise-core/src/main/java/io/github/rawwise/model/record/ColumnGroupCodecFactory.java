package io.github.rawwise.model.record;

public final class ColumnGroupCodecFactory {

    private ColumnGroupCodecFactory() {}

    public static ColumnGroupCodec getCodec(ColumnGroup cg) {
        return switch (cg.encoding()) {
            case FIXED_OFFSET -> FixedOffsetCodec.INSTANCE;
            case TLV -> TlvCodec.INSTANCE;
        };
    }
}
