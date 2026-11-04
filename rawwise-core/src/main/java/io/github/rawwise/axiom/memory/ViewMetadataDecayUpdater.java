package io.github.rawwise.axiom.memory;

import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ViewMetadataDecayUpdater {

    private final ViewMetadataStore store;
    private final DecayPolicy defaultPolicy;

    public ViewMetadataDecayUpdater(ViewMetadataStore store, DecayPolicy defaultPolicy) {
        this.store = store;
        this.defaultPolicy = defaultPolicy;
    }

    public int decayTable(String tableId, DecayPolicy policy) throws IOException {
        List<ViewMetadata> entries = store.scanByTable(tableId, 10_000);
        if (entries.isEmpty()) {
            log.debug("ViewMetadata decay skipped: no entries for tableId={}", tableId);
            return 0;
        }
        int decayed = 0;
        long now = System.currentTimeMillis();
        double halfLifeMs = policy.halfLifeMs();
        for (ViewMetadata entry : entries) {
            long startTime = entry.lastAccessedAt();
            if (startTime <= 0) continue;
            long ageMs = now - startTime;
            if (ageMs < halfLifeMs) continue;
            double newDecayFactor = policy.computeDecayFactor(ageMs, entry.accessCount());
            float newDecay = (float) newDecayFactor;
            if (Math.abs(newDecay - entry.decayFactor()) < 0.01) continue;
            ViewMetadata updated = entry.withDecayFactor(newDecay);
            store.update(tableId, entry.entryKey(), updated);
            decayed++;
        }
        log.info(
                "ViewMetadata decay complete tableId={} decayed={}/{}",
                tableId,
                decayed,
                entries.size());
        return decayed;
    }

    public int decayTable(String tableId) throws IOException {
        return decayTable(tableId, defaultPolicy);
    }
}
