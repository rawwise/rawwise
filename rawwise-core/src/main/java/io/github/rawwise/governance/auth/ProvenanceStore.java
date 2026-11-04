package io.github.rawwise.governance.auth;

import java.io.IOException;
import java.util.List;

public interface ProvenanceStore {

    void record(ProvenanceEvent event) throws IOException;

    List<ProvenanceEvent> queryByPrincipal(String principalId, long fromTs, long toTs)
            throws IOException;

    List<ProvenanceEvent> queryByPrincipal(String principalId, long fromTs, long toTs, int limit)
            throws IOException;

    List<ProvenanceEvent> queryByTarget(String targetType, String targetId, long fromTs, long toTs)
            throws IOException;

    List<ProvenanceEvent> queryByTarget(
            String targetType, String targetId, long fromTs, long toTs, int limit)
            throws IOException;

    List<ProvenanceEvent> queryByAction(String action, long fromTs, long toTs) throws IOException;

    List<ProvenanceEvent> queryByAction(String action, long fromTs, long toTs, int limit)
            throws IOException;

    List<ProvenanceEvent> queryByTimeRange(long fromTs, long toTs) throws IOException;

    List<ProvenanceEvent> queryByTimeRange(long fromTs, long toTs, int limit) throws IOException;
}
