package io.github.rawwise.engine.clean.strategy;

import io.github.rawwise.engine.clean.CleanAction;
import io.github.rawwise.engine.clean.CleanContext;
import java.io.Serializable;
import java.util.List;

public interface CleanStrategy extends Serializable {

    long serialVersionUID = 1L;

    enum StrategyType {
        ORPHAN_FILE,
        TTL_EXPIRY,
        SNAPSHOT_EXPIRY
    }

    StrategyType strategyType();

    List<CleanAction> plan(CleanContext context);
}
