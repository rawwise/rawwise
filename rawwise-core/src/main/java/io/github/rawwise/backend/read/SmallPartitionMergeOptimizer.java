package io.github.rawwise.backend.read;

import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.model.record.TableMeta;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SmallPartitionMergeOptimizer implements ReadOptimizer {

    private final int minBlocksPerPartition;
    private final int maxBlocksPerPartition;

    public SmallPartitionMergeOptimizer() {
        this(2, 64);
    }

    public SmallPartitionMergeOptimizer(int minBlocksPerPartition, int maxBlocksPerPartition) {
        if (minBlocksPerPartition < 1) {
            throw new IllegalArgumentException("minBlocksPerPartition must be >= 1");
        }
        if (maxBlocksPerPartition < minBlocksPerPartition) {
            throw new IllegalArgumentException(
                    "maxBlocksPerPartition must be >= minBlocksPerPartition");
        }
        this.minBlocksPerPartition = minBlocksPerPartition;
        this.maxBlocksPerPartition = maxBlocksPerPartition;
    }

    @Override
    public String name() {
        return "SmallPartitionMerge";
    }

    @Override
    public int priority() {
        return 100;
    }

    @Override
    public ReadPlan optimize(ReadPlan plan) {
        if (plan.skipScanWithRowGeneration() || plan.partitions().size() <= 1) {
            return plan;
        }

        List<ReadPartition> merged = new ArrayList<>();
        List<ReadPartition> small = new ArrayList<>();

        for (ReadPartition p : plan.partitions()) {
            if (p.blockCount() < minBlocksPerPartition) {
                small.add(p);
            } else {
                merged.add(p);
            }
        }

        if (small.isEmpty()) {
            return plan;
        }

        small.sort(Comparator.comparing(p -> p.primaryHost() != null ? p.primaryHost() : "\uffff"));

        List<ReadPartition> hostGroups = mergeByHost(small);
        merged.addAll(hostGroups);

        return plan.toBuilder().partitions(merged).build();
    }

    private List<ReadPartition> mergeByHost(List<ReadPartition> smallPartitions) {
        List<ReadPartition> result = new ArrayList<>();
        List<DataBlock> currentBlocks = new ArrayList<>();
        String currentHost = null;
        TableMeta currentMeta = null;
        LinkedHashSet<UpdateLogEntry> currentLogs = new LinkedHashSet<>();

        for (ReadPartition p : smallPartitions) {
            String host = p.primaryHost();
            boolean sameHost = currentHost != null && currentHost.equals(host);
            boolean metaMismatch =
                    currentMeta != null
                            && p.tableMeta() != null
                            && !currentMeta.equals(p.tableMeta());
            boolean wouldExceedMax = currentBlocks.size() + p.blockCount() > maxBlocksPerPartition;

            if (!sameHost || metaMismatch || wouldExceedMax) {
                if (!currentBlocks.isEmpty()) {
                    result.add(
                            new ReadPartition(
                                    currentHost != null
                                            ? new String[] {currentHost}
                                            : new String[0],
                                    new ArrayList<>(currentBlocks),
                                    currentMeta,
                                    new ArrayList<>(currentLogs)));
                    currentBlocks.clear();
                    currentLogs.clear();
                }
                currentHost = host;
                currentMeta = p.tableMeta();
            }

            currentBlocks.addAll(p.blocks());
            if (p.updateLogs() != null) {
                currentLogs.addAll(p.updateLogs());
            }
        }

        if (!currentBlocks.isEmpty()) {
            result.add(
                    new ReadPartition(
                            currentHost != null ? new String[] {currentHost} : new String[0],
                            new ArrayList<>(currentBlocks),
                            currentMeta,
                            new ArrayList<>(currentLogs)));
        }

        return result;
    }
}
