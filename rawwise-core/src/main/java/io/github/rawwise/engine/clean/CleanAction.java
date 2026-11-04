package io.github.rawwise.engine.clean;

import java.io.Serializable;

public sealed interface CleanAction extends Serializable
        permits DeleteFileAction, RewriteFileAction, ExpireSnapshotAction {

    ActionType actionType();

    enum ActionType {
        DELETE_FILE,
        REWRITE_FILE,
        EXPIRE_SNAPSHOT
    }
}
