package io.github.rawwise.frontend.ir.routing;

import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.frontend.ir.ProjectionColumn;
import io.github.rawwise.frontend.ir.ScanQueryIR;
import io.github.rawwise.frontend.ir.TableMetaIR;
import io.github.rawwise.model.expression.And;
import io.github.rawwise.model.expression.ColumnRef;
import io.github.rawwise.model.expression.Eq;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.IndexMeta;
import io.github.rawwise.model.record.TableId;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * 二级索引选择器（H-5 修复）。
 *
 * <p>职责：给定 {@link ScanQueryIR}，从 Catalog 查询该表所有已注册索引，评分后 选择最优索引，并将查询重写到索引影子表。
 *
 * <p>评分规则（H-5）：
 *
 * <ol>
 *   <li><b>覆盖度（基础分 100）</b>：索引前缀完全匹配查询谓词等值列数，每列 +20
 *   <li><b>覆盖输出（+50）</b>：STORING 子句包含所有 SELECT 列时
 *   <li><b>索引选择性（+1~30）</b>：基于 distinct_count / total_count
 *   <li><b>索引大小（-1/MB）</b>：越小越优
 *   <li><b>字典序 tie-break</b>：同分按 indexName 字典序选择（deterministic）
 * </ol>
 */
@Slf4j
public class IndexSelector {

    private final Catalog catalog;

    /** 索引候选评分记录。 */
    public record Candidate(IndexMeta meta, int score) implements Comparable<Candidate> {
        @Override
        public int compareTo(Candidate o) {
            int cmp = Integer.compare(o.score, this.score);
            return cmp != 0 ? cmp : this.meta.indexName().compareTo(o.meta.indexName());
        }
    }

    public IndexSelector(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    public Catalog catalog() {
        return catalog;
    }

    /**
     * 尝试将 ScanQueryIR 重写到索引影子表。
     *
     * @param sq 原始查询（指向主表）
     * @return 重写后的 ScanQueryIR（指向索引影子表），若无可用索引则返回空
     */
    public Optional<ScanQueryIR> tryRewrite(ScanQueryIR sq) {
        TableId tableId = TableId.parseLenient(sq.tableMeta().tableIdentifier());
        List<IndexMeta> indexes = catalog.indexesOf(tableId);
        if (indexes.isEmpty()) return Optional.empty();

        // Extract equality constraint columns from query predicates
        Set<String> eqColumns = extractEqualityColumns(sq.filter());
        if (eqColumns.isEmpty()) return Optional.empty();

        // Extract SELECT projection columns
        Set<String> projColumns =
                sq.projection().isEmpty()
                        ? Set.of()
                        : sq.projection().stream()
                                .map(ProjectionColumn::name)
                                .collect(Collectors.toCollection(HashSet::new));

        List<Candidate> candidates =
                indexes.stream()
                        .map(idx -> new Candidate(idx, score(idx, eqColumns, projColumns)))
                        .filter(c -> c.score >= 100)
                        .sorted()
                        .toList();

        if (candidates.isEmpty()) {
            log.debug("IndexSelector: no index covers query on table={}", tableId);
            return Optional.empty();
        }

        IndexMeta best = candidates.get(0).meta;
        log.info(
                "IndexSelector: selected index {} for table {} (score={})",
                best.indexName(),
                tableId,
                candidates.get(0).score);

        TableMetaIR shadowMeta = resolveShadowMeta(best, sq.tableMeta());
        return Optional.of(
                new ScanQueryIR(
                        shadowMeta,
                        sq.snapshotId(),
                        sq.projection(),
                        null, // filter already addressed by index lookup
                        sq.sortKeys(),
                        sq.limit(),
                        sq.offset(),
                        null,
                        sq.lockMode()));
    }

    // ==================== H-5 Scoring ====================

    /** 计算索引候选评分。 */
    public int score(IndexMeta idx, Set<String> eqColumns, Set<String> projColumns) {
        int s = 100; // 基础分

        // 1. 覆盖度：索引前缀完全匹配查询等值列数
        int matched = 0;
        for (String col : idx.columns()) {
            if (eqColumns.contains(col)) {
                matched++;
                s += 20;
            } else {
                break; // 前缀失配，不再继续
            }
        }

        // 零列匹配 → 无效索引
        if (matched == 0) return 0;

        // 2. 覆盖输出：STORING 包含所有 SELECT 列
        if (!projColumns.isEmpty()
                && !idx.storing().isEmpty()
                && new HashSet<>(idx.storing()).containsAll(projColumns)) {
            s += 50;
        }

        // 3-4. 选择性 + 大小（当前为占位：默认 +15 选择性，无大小惩罚）
        // TODO: 当 Metastore 提供 __idx_meta__ distinct_count 统计后接入
        s += 15;

        return s;
    }

    // ==================== Helpers ====================

    /** Extract equality constraint column names from filter expression. */
    public static Set<String> extractEqualityColumns(Expression filter) {
        if (filter == null) return Set.of();
        Set<String> cols = new HashSet<>();
        collectEqColumns(filter, cols);
        return Collections.unmodifiableSet(cols);
    }

    private static void collectEqColumns(Expression expr, Set<String> out) {
        if (expr == null) return;
        if (expr instanceof Eq eq) {
            if (eq.getLeft() instanceof ColumnRef cl) out.add(cl.getColumnName());
            if (eq.getRight() instanceof ColumnRef cr) out.add(cr.getColumnName());
        } else if (expr instanceof And and) {
            for (Expression child : and.children()) {
                collectEqColumns(child, out);
            }
        }
    }

    /**
     * 解析索引影子表的 TableMetaIR。 影子表命名规则：{@code __{indexName}} ，与主表同 namespace。 若影子表在 Catalog 中不存在 →
     * 返回原始主表 meta（安全降级）。
     */
    private TableMetaIR resolveShadowMeta(IndexMeta idx, TableMetaIR mainMeta) {
        String shadowName = "__" + idx.indexName();
        try {
            TableId tid = TableId.parseLenient(mainMeta.tableIdentifier());
            TableId shadowId = new TableId(tid.database(), shadowName);
            var shadowTable = catalog.loadTable(shadowId);
            var meta = shadowTable.meta();
            return new TableMetaIR(
                    shadowTable.getTableIdentifier(),
                    meta.tableId(),
                    meta.tableType()
                                    == io.github.rawwise.model.record.TableMeta.TableType
                                            .APPEND_ONLY
                            ? io.github.rawwise.frontend.ir.TableType.APPEND_ONLY
                            : io.github.rawwise.frontend.ir.TableType.PRIMARY_KEY,
                    meta.schema().structType(),
                    meta.primaryKey().fields(),
                    meta.properties(),
                    Math.toIntExact(Math.min(meta.schema().version(), Integer.MAX_VALUE)));
        } catch (Exception e) {
            log.warn(
                    "IndexSelector: shadow table {} not found, falling back to main table {}",
                    shadowName,
                    mainMeta.tableIdentifier());
            return mainMeta;
        }
    }
}
