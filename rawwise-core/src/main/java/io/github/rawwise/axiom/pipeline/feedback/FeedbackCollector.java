package io.github.rawwise.axiom.pipeline.feedback;

import java.util.List;

/**
 * aggregate feedback history by (view Id, view Version). retrieve from Knowledge Store
 * feedback_fact,view_annotations,view_score.
 */
public interface FeedbackCollector {

    /**
     * aggregate all related feedback by view Id group by view Version(follows Contract 6.1)
     *
     * @param view Id evaluated view ID
     * @param view Version view version
     * @param table Id table identifier
     * @return aggregated feedback list, sorted by time
     */
    List<CollectedFeedback> collectByScene(String viewId, String viewVersion, String tableId);

    /** default noop implementation: returns empty list. */
    static FeedbackCollector noop() {
        return (viewId, viewVersion, tableId) -> List.of();
    }
}
