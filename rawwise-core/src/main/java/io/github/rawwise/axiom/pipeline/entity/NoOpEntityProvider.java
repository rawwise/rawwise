package io.github.rawwise.axiom.pipeline.entity;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class NoOpEntityProvider implements EntityProvider {

    @Override
    public String name() {
        return "noop";
    }

    @Override
    public EntityView extract(String text, EntityContext context) {
        log.info(
                "NoOpEntityProvider.extract called sampleId={} textLen={}",
                context == null ? null : context.sampleId(),
                text == null ? 0 : text.length());
        String diaId = context == null ? "" : context.tableId() + ":" + context.sampleId();
        String sampleId = context == null ? "" : context.sampleId();
        return new EntityView(
                diaId,
                sampleId,
                java.util.List.of(),
                java.util.List.of(),
                "noop",
                java.time.Instant.now(),
                ExtractionStatus.SUCCESS,
                null);
    }
}
