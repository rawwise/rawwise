package io.github.rawwise.backend;

import io.github.rawwise.backend.lazyupdate.LazyUpdateReadEnhancer;
import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import io.github.rawwise.backend.read.*;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.TableMeta;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultPlanner implements ReadPlanner {

    private final DataBlockListProvider blockListProvider;
    private final ReadOptimizerChain optimizerChain;

    public DefaultPlanner() {
        this(new LocalTableStoreBlockListProvider(), new ReadOptimizerChain());
    }

    public DefaultPlanner(
            DataBlockListProvider blockListProvider, ReadOptimizerChain optimizerChain) {
        this.blockListProvider = Objects.requireNonNull(blockListProvider);
        this.optimizerChain = optimizerChain != null ? optimizerChain : new ReadOptimizerChain();
    }

    @Override
    public ReadPlan planReads(ScanContext context) {
        Objects.requireNonNull(context, "ScanContext must not be null");

        Expression predicate = context.predicate();
        TableMeta tableMeta = context.tableMeta();
        List<UpdateLogEntry> updateLogs = context.updateLogs();
        LazyUpdateReadEnhancer enhancer = context.enhancer();

        if (enhancer != null && updateLogs != null && !updateLogs.isEmpty() && predicate != null) {
            ReadPlan lazyPlan =
                    tryLazyUpdatePlan(
                            enhancer,
                            predicate,
                            tableMeta,
                            updateLogs,
                            context.projectionColumns());
            if (lazyPlan != null) {
                return lazyPlan;
            }
        }

        return buildScanPlan(context, predicate, tableMeta, updateLogs);
    }

    private ReadPlan tryLazyUpdatePlan(
            LazyUpdateReadEnhancer enhancer,
            Expression predicate,
            TableMeta tableMeta,
            List<UpdateLogEntry> updateLogs,
            List<String> projectionColumns) {
        try {
            boolean hasDelete = false;
            for (UpdateLogEntry entry : updateLogs) {
                if (entry.updates().isEmpty()) {
                    hasDelete = true;
                    break;
                }
            }
            if (hasDelete) {
                return null;
            }

            if (enhancer.canSkipScanWithRowGeneration(predicate, updateLogs)) {
                java.util.Map<String, Comparable<?>> defaults = buildSchemaDefaults(tableMeta);
                java.util.Set<String> updateLogColumns = new java.util.HashSet<>();
                for (UpdateLogEntry entry : updateLogs) {
                    updateLogColumns.addAll(entry.updates().keySet());
                }
                if (canProvideDefaultsForRequiredFields(
                        tableMeta, defaults, projectionColumns, updateLogColumns)) {
                    java.util.List<java.util.Map<String, Comparable<?>>> rows =
                            enhancer.generateRowsFromUpdateLogs(predicate, updateLogs, defaults);
                    if (!projectionColumns.isEmpty()) {
                        rows = projectRows(rows, projectionColumns);
                    }
                    return ReadPlan.builder()
                            .predicate(predicate)
                            .skipScanWithRowGeneration(true)
                            .generatedRows(rows)
                            .build();
                }
            }
            LazyUpdateReadEnhancer.recordRowGenerationFallback();
        } catch (Exception e) {
            log.debug("LazyUpdate plan failed, falling back to scan: {}", e.getMessage());
        }
        return null;
    }

    private java.util.List<java.util.Map<String, Comparable<?>>> projectRows(
            java.util.List<java.util.Map<String, Comparable<?>>> rows,
            List<String> projectionColumns) {
        java.util.List<java.util.Map<String, Comparable<?>>> result =
                new java.util.ArrayList<>(rows.size());
        for (java.util.Map<String, Comparable<?>> row : rows) {
            java.util.Map<String, Comparable<?>> projected = new java.util.LinkedHashMap<>();
            for (String col : projectionColumns) {
                projected.put(col, row.get(col));
            }
            result.add(projected);
        }
        return result;
    }

    private ReadPlan buildScanPlan(
            ScanContext context,
            Expression predicate,
            TableMeta tableMeta,
            List<UpdateLogEntry> updateLogs) {
        List<DataBlock> allBlocks =
                blockListProvider.listBlocks(
                        context.basePath(), context.storeId(), context.tableIdentifier());

        ReadPruning.KeyPruningSpec pruning = context.pruningSpec();
        List<DataBlock> hitBlocks = ReadPruning.pruneBlocks(allBlocks, pruning);
        if (hitBlocks.isEmpty()) {
            return ReadPlan.builder().predicate(predicate).build();
        }

        LinkedHashMap<String, List<DataBlock>> byHost = new LinkedHashMap<>();
        for (DataBlock block : hitBlocks) {
            String host = ReadPruning.preferredHost(block);
            byHost.computeIfAbsent(host, ignored -> new ArrayList<>()).add(block);
        }

        List<ReadPartition> partitions = new ArrayList<>(byHost.size());
        for (Map.Entry<String, List<DataBlock>> e : byHost.entrySet()) {
            String host = e.getKey();
            String[] preferred = host == null ? new String[0] : new String[] {host};
            partitions.add(new ReadPartition(preferred, e.getValue(), tableMeta, updateLogs));
        }

        ReadPlan plan = ReadPlan.builder().partitions(partitions).predicate(predicate).build();

        return optimizerChain.optimize(plan);
    }

    private java.util.Map<String, Comparable<?>> buildSchemaDefaults(TableMeta tableMeta) {
        if (tableMeta == null || tableMeta.schema() == null) {
            return java.util.Map.of();
        }
        java.util.Map<String, Comparable<?>> defaults = new java.util.LinkedHashMap<>();
        for (var field : tableMeta.schema().structType().fields()) {
            Comparable<?> defaultValue = typeDefault(field.dataType());
            defaults.put(field.name(), defaultValue);
        }
        return defaults;
    }

    private boolean canProvideDefaultsForRequiredFields(
            TableMeta tableMeta,
            java.util.Map<String, Comparable<?>> defaults,
            List<String> projectionColumns,
            java.util.Set<String> updateLogColumns) {
        if (tableMeta == null || tableMeta.schema() == null) {
            return false;
        }
        java.util.Set<String> requiredColumns = new java.util.HashSet<>(projectionColumns);
        requiredColumns.addAll(updateLogColumns);
        for (var field : tableMeta.schema().structType().fields()) {
            if (!field.dataType().isNullable()
                    && requiredColumns.contains(field.name())
                    && !defaults.containsKey(field.name())) {
                return false;
            }
        }
        return true;
    }

    private static Comparable<?> typeDefault(io.github.rawwise.model.record.RawWiseType dt) {
        if (dt == null) return null;
        return switch (dt.name()) {
            case "int32", "int8" -> 0;
            case "int64" -> 0L;
            case "float64" -> 0.0;
            case "float32" -> 0.0f;
            case "string" -> "";
            case "boolean" -> false;
            case "decimal" -> java.math.BigDecimal.ZERO;
            case "date" -> 0;
            case "timestamp" -> 0L;
            default -> null;
        };
    }
}
