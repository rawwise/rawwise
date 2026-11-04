package io.github.rawwise.axiom.pipeline.feedback.retrieval;

public record AnnotateRequest(
        String viewId, FeedbackType signal, String correction, AnnotateContext context) {

    public static AnnotateRequest of(String viewId, FeedbackType sig, AnnotateContext ctx) {
        return new AnnotateRequest(viewId, sig, null, ctx == null ? AnnotateContext.empty() : ctx);
    }
}
