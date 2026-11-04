package io.github.rawwise.axiom.pipeline.graph.distributed;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityBloomFilter {

    private final BloomFilter<CharSequence> filter;
    private final long capacity;
    private final double targetFpp;

    public EntityBloomFilter(long capacity, double targetFpp) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        if (targetFpp <= 0.0 || targetFpp >= 1.0) {
            throw new IllegalArgumentException("targetFpp must be in (0,1)");
        }
        this.capacity = capacity;
        this.targetFpp = targetFpp;
        this.filter =
                BloomFilter.create(
                        Funnels.stringFunnel(StandardCharsets.UTF_8), capacity, targetFpp);
    }

    public boolean mightContain(String entityId) {
        if (entityId == null) return false;
        return filter.mightContain(entityId);
    }

    public void put(String entityId) {
        if (entityId == null) return;
        filter.put(entityId);
    }

    public double expectedFpp() {
        return filter.expectedFpp();
    }

    public long sizeBytes() {
        double n = capacity;
        double p = targetFpp;
        double mBits = -n * Math.log(p) / (Math.log(2) * Math.log(2));
        return (long) Math.ceil(mBits / 8.0);
    }

    public long capacity() {
        return capacity;
    }

    public double targetFpp() {
        return targetFpp;
    }
}
