package io.github.rawwise.model.record;

import java.util.concurrent.atomic.AtomicLong;

public interface AppendOnlyTable extends KVTable {

    AtomicLong HIDDEN_PK_GENERATOR = new AtomicLong(System.currentTimeMillis());

    HiddenColumnConfig hiddenColumnConfig();

    default boolean isHiddenPkVisible() {
        return hiddenColumnConfig().hasHiddenPk()
                && hiddenColumnConfig()
                        .findColumn(HiddenColumnConfig.ColumnType.HIDDEN_PRIMARY_KEY)
                        .map(HiddenColumnConfig.HiddenColumnDef::visible)
                        .orElse(false);
    }

    default long nextHiddenPkValue() {
        return HIDDEN_PK_GENERATOR.incrementAndGet();
    }
}
