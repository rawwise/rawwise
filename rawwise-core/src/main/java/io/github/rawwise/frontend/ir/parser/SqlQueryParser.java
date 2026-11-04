package io.github.rawwise.frontend.ir.parser;

import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.exception.CatalogException;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.model.expression.ColumnRef;
import io.github.rawwise.model.expression.Eq;
import io.github.rawwise.model.expression.Gt;
import io.github.rawwise.model.expression.GtEq;
import io.github.rawwise.model.expression.Lt;
import io.github.rawwise.model.expression.LtEq;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.TableMeta;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

@Slf4j
public class SqlQueryParser implements QueryParser {

    private final Catalog catalog;
    private final SnapshotManager snapshotManager;
    private final boolean fastPathEnabled;
    private final Map<String, TableMetaIR> tableMetaCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    public SqlQueryParser(Catalog catalog, SnapshotManager snapshotManager) {
        this(catalog, snapshotManager, false);
    }

    public SqlQueryParser(
            Catalog catalog, SnapshotManager snapshotManager, boolean fastPathEnabled) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.snapshotManager = Objects.requireNonNull(snapshotManager, "snapshotManager");
        this.fastPathEnabled = fastPathEnabled;
    }

    @Override
    public ParseResult parse(String sql) {
        return parse(sql, null);
    }

    private static final Pattern SELECT_POINT_QUERY =
            Pattern.compile(
                    "SELECT\\s+(.+?)\\s+FROM\\s+(\\S+)\\s+WHERE\\s+(\\S+)\\s*=\\s*(\\S+)\\s*$",
                    Pattern.CASE_INSENSITIVE);

    @Override
    public ParseResult parse(String sql, @Nullable Long requestedSnapshotId) {
        if (fastPathEnabled) {
            PointQueryIR fastPq = tryMatchPointQueryFast(sql, requestedSnapshotId);
            if (fastPq != null) return new ParseResult.IRResult(fastPq);
        }

        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);

            if (stmt instanceof PlainSelect ps) {
                JoinQueryIR jq = tryMatchJoinQuery(ps, requestedSnapshotId);
                if (jq != null) return new ParseResult.IRResult(jq);

                if (fastPathEnabled) {
                    PointQueryIR pq = tryMatchPointQuery(ps, requestedSnapshotId);
                    if (pq != null) return new ParseResult.IRResult(pq);
                }

                ScanQueryIR sq = tryMatchScanQuery(ps, requestedSnapshotId);
                if (sq != null) return new ParseResult.IRResult(sq);

                if (fastPathEnabled) {
                    MetadataQueryIR mq = tryMatchMetadataQuery(ps, requestedSnapshotId);
                    if (mq != null) return new ParseResult.IRResult(mq);
                }

                return new ParseResult.FallbackResult("Unsupported SELECT pattern", sql);
            }

            if (stmt instanceof Insert insert) {
                WriteQueryIR wq = tryMatchInsert(insert, requestedSnapshotId);
                if (wq != null) return new ParseResult.IRResult(wq);
                return new ParseResult.FallbackResult(
                        "INSERT via JSqlParser, table resolution needs catalog fix", sql);
            }

            if (stmt instanceof Update update) {
                QueryIR uq = tryMatchUpdate(update, requestedSnapshotId);
                if (uq != null) return new ParseResult.IRResult(uq);
                return new ParseResult.FallbackResult("UPDATE not matchable to point IR", sql);
            }

            if (stmt instanceof Delete delete) {
                QueryIR dq = tryMatchDelete(delete, requestedSnapshotId);
                if (dq != null) return new ParseResult.IRResult(dq);
                return new ParseResult.FallbackResult("DELETE not matchable to point IR", sql);
            }

            if (stmt instanceof net.sf.jsqlparser.statement.create.index.CreateIndex ci) {
                DDLIR ddl = tryMatchCreateIndex(ci);
                if (ddl != null) return new ParseResult.IRResult(ddl);
                return new ParseResult.FallbackResult("CREATE INDEX not matchable", sql);
            }

            if (stmt instanceof net.sf.jsqlparser.statement.alter.Alter alter) {
                DDLIR ddl = tryMatchAlterTableConstraint(alter);
                if (ddl != null) return new ParseResult.IRResult(ddl);
                return new ParseResult.FallbackResult("ALTER TABLE constraint not matchable", sql);
            }

            return new ParseResult.FallbackResult(
                    "Unsupported SQL: " + stmt.getClass().getSimpleName(), sql);

        } catch (net.sf.jsqlparser.JSQLParserException e) {
            log.debug("SQL parse failed, fallback: {}", e.getMessage());
            return new ParseResult.FallbackResult(e.getMessage(), sql);
        } catch (Exception e) {
            log.warn("Unexpected error in SqlQueryParser, fallback: {}", e.getMessage(), e);
            return new ParseResult.FallbackResult(e.getMessage(), sql);
        }
    }

    /**
     * 匹配 2 表 INNER JOIN：{@code SELECT ... FROM a [INNER] JOIN b ON a.x = b.y [WHERE ...] [LIMIT
     * ...]}。 不支持的形态（多于 2 表、子查询、CROSS/OUTER、ON 含子查询/复合谓词等）一律 fallback 返回 null。
     *
     * <p>H-3 修复：snapshotId = min(latestSnapshotId(t1), latestSnapshotId(t2))，按 tableId 字典序排序后取最小值，
     * 任一表无 snapshot 时回退到 currentTs（这里以 0 表示 "读 LMDB 最新"）。
     */
    private JoinQueryIR tryMatchJoinQuery(PlainSelect ps, @Nullable Long requestedSnapshotId) {
        if (ps.getJoins() == null || ps.getJoins().isEmpty()) return null;
        if (ps.getJoins().size() != 1) {
            log.debug(
                    "Join match skipped: {} joins (only 2-table supported in Phase 1)",
                    ps.getJoins().size());
            return null;
        }
        Join join = ps.getJoins().get(0);

        // CROSS / NATURAL / OUTER / SEMI / ANTI 暂不支持 → fallback
        if (join.isCross()
                || join.isNatural()
                || join.isOuter()
                || join.isFull()
                || join.isLeft()
                || join.isRight()
                || join.isSemi()
                || join.isStraight()
                || join.isApply()) {
            log.debug("Join match skipped: only INNER JOIN supported in Phase 1");
            return null;
        }
        if (!join.isInnerJoin() && !join.isSimple()) {
            log.debug("Join match skipped: not an INNER/SIMPLE join");
            return null;
        }

        // 左/右两侧必须是裸表（不支持子查询）
        FromItem leftFrom = ps.getFromItem();
        FromItem rightFrom = join.getFromItem();
        if (!(leftFrom instanceof Table leftTable) || !(rightFrom instanceof Table rightTable)) {
            log.debug("Join match skipped: subquery in JOIN side not supported");
            return null;
        }

        String leftTableRef = leftTable.getFullyQualifiedName();
        String rightTableRef = rightTable.getFullyQualifiedName();
        TableMetaIR leftMeta = resolveTableMeta(leftTableRef);
        TableMetaIR rightMeta = resolveTableMeta(rightTableRef);
        if (leftMeta == null || rightMeta == null) {
            log.debug(
                    "Join match skipped: cannot resolve TableMeta (left={}, right={})",
                    leftTableRef,
                    rightTableRef);
            return null;
        }

        // ON 表达式：必须存在，不支持 USING (...)
        if (join.getUsingColumns() != null && !join.getUsingColumns().isEmpty()) {
            log.debug("Join match skipped: USING clause not supported");
            return null;
        }
        List<Expression> onExprs =
                join.getOnExpressions() == null
                        ? List.of()
                        : new ArrayList<>(join.getOnExpressions());
        if (onExprs.isEmpty()) {
            log.debug("Join match skipped: missing ON clause");
            return null;
        }
        // 支持单一 ON 表达式（包括 AND 串联）
        Expression onExpr = onExprs.size() == 1 ? onExprs.get(0) : combineWithAnd(onExprs);
        io.github.rawwise.model.expression.Expression joinCondition = convertExpression(onExpr);
        if (joinCondition == null) {
            log.debug("Join match skipped: ON expression not convertible");
            return null;
        }

        // WHERE 作为 postFilter
        io.github.rawwise.model.expression.Expression postFilter = null;
        if (ps.getWhere() != null) {
            postFilter = convertExpression(ps.getWhere());
            if (postFilter == null) {
                log.debug("Join match skipped: WHERE expression not convertible");
                return null;
            }
        }

        // 投影
        List<ProjectionColumn> projections = extractProjections(ps);
        if (projections == null) return null;

        // LIMIT
        int limit = extractLimit(ps);

        // H-3 修复：按 tableId 字典序排序后取 latestSnapshotId 的最小值
        List<TableMetaIR> orderedMetas = new ArrayList<>(List.of(leftMeta, rightMeta));
        orderedMetas.sort(Comparator.comparing(TableMetaIR::tableIdentifier));
        long snapshotId =
                requestedSnapshotId != null
                        ? requestedSnapshotId
                        : resolveJoinedSnapshotId(orderedMetas);

        // 左/右子树：构造 ScanQueryIR（不带 filter，filter 已合并为 postFilter）
        ScanQueryIR leftScan =
                new ScanQueryIR(leftMeta, snapshotId, List.of(), null, List.of(), 0, 0, null);
        ScanQueryIR rightScan =
                new ScanQueryIR(rightMeta, snapshotId, List.of(), null, List.of(), 0, 0, null);

        return new JoinQueryIR(
                orderedMetas,
                snapshotId,
                leftScan,
                rightScan,
                JoinType.INNER,
                joinCondition,
                projections,
                postFilter,
                limit);
    }

    private Expression combineWithAnd(List<Expression> exprs) {
        Expression cur = exprs.get(0);
        for (int i = 1; i < exprs.size(); i++) {
            cur = new AndExpression(cur, exprs.get(i));
        }
        return cur;
    }

    /** H-3 修复：取所有表 latestSnapshotId 的最小值；任一表无 snapshot 则回退到 0（语义为 "读 LMDB 最新"）。 */
    private long resolveJoinedSnapshotId(List<TableMetaIR> sortedTables) {
        long minSnapshot = Long.MAX_VALUE;
        for (TableMetaIR t : sortedTables) {
            long sid;
            try {
                sid =
                        snapshotManager
                                .latestSnapshot(t.tableIdentifier())
                                .map(io.github.rawwise.model.record.Snapshot::snapshotId)
                                .orElse(-1L);
            } catch (Exception e) {
                log.debug(
                        "resolveJoinedSnapshotId: cannot resolve snapshot for {}: {}",
                        t.tableIdentifier(),
                        e.getMessage());
                sid = -1L;
            }
            if (sid < 0) {
                // 任一表无 snapshot → 回退到 0（latest LMDB）
                return 0L;
            }
            minSnapshot = Math.min(minSnapshot, sid);
        }
        return minSnapshot == Long.MAX_VALUE ? 0L : minSnapshot;
    }

    private ScanQueryIR tryMatchScanQuery(PlainSelect ps, @Nullable Long requestedSnapshotId) {
        // 多表 JOIN 已交给 tryMatchJoinQuery 处理；这里仅负责单表 SCAN
        if (ps.getJoins() != null && !ps.getJoins().isEmpty()) return null;
        String tableRef = extractTableRef(ps);
        if (tableRef == null) return null;

        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        long snapshotId =
                requestedSnapshotId != null
                        ? requestedSnapshotId
                        : resolveLatestSnapshotId(tableRef);
        if (snapshotId < 0) return null;

        List<ProjectionColumn> projections = extractProjections(ps);
        if (projections == null) return null;
        io.github.rawwise.model.expression.Expression filter = extractFilter(ps);
        if (filter == null && ps.getWhere() != null) return null;

        int limit = extractLimit(ps);
        int offset = extractOffset(ps);
        List<SortKey> sortKeys = extractSortKeys(ps);

        // 识别 FOR UPDATE / FOR SHARE
        LockMode lockMode = extractLockMode(ps);

        return new ScanQueryIR(
                meta, snapshotId, projections, filter, sortKeys, limit, offset, null, lockMode);
    }

    private PointQueryIR tryMatchPointQueryFast(String sql, @Nullable Long requestedSnapshotId) {
        Matcher m = SELECT_POINT_QUERY.matcher(sql.trim());
        if (!m.matches()) return null;

        String projections = m.group(1).trim();
        String tableRef = m.group(2);
        String pkCol = m.group(3);
        String pkValStr = m.group(4);

        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        List<String> pkCols = meta.primaryKeyColumns();
        if (pkCols == null || pkCols.isEmpty() || !pkCols.contains(pkCol)) return null;

        Comparable<?> pkVal;
        try {
            pkVal = Long.parseLong(pkValStr);
        } catch (NumberFormatException e) {
            try {
                pkVal = Integer.parseInt(pkValStr);
            } catch (NumberFormatException e2) {
                return null;
            }
        }

        Map<String, Comparable<?>> pkValues =
                normalizeBySchema(Map.of(pkCol, pkVal), meta.schema());
        // encodedKey 留空：server 端用 pkValues + primaryKeyCols 经 keyCodec 重建，避免 BIGINT PK 4-byte 截断
        byte[] encodedKey = new byte[0];
        List<String> projection =
                projections.equals("*") ? List.of() : List.of(projections.split("\\s*,\\s*"));

        return new PointQueryIR(
                meta, null, 0L, null, pkValues, encodedKey, projection, "driver", 0, 0L);
    }

    private PointQueryIR tryMatchPointQuery(PlainSelect ps, @Nullable Long requestedSnapshotId) {
        if (ps.getJoins() != null && !ps.getJoins().isEmpty()) return null;
        String tableRef = extractTableRef(ps);
        if (tableRef == null) return null;

        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        List<String> pkCols = meta.primaryKeyColumns();
        if (pkCols == null || pkCols.isEmpty()) return null;

        if (ps.getWhere() == null) return null;

        Map<String, Comparable<?>> pkValues = extractPkEqualityValues(ps.getWhere(), pkCols);
        if (pkValues == null) return null;
        pkValues = normalizeBySchema(pkValues, meta.schema());

        List<String> projection = extractPointProjectionColumns(ps);

        // encodedKey 留空：server 端会用 pkValues + primaryKeyCols 经 keyCodec.encodePrimaryKey 重建。
        // 这统一了 INSERT/UPDATE/DELETE/SELECT 四条路径的 PK 编码格式，避免 BIGINT PK
        // 在 frontend 侧 4-byte 截断 vs server 侧 8-byte 编码导致写后读不一致。
        byte[] encodedKey = new byte[0];

        return new PointQueryIR(
                meta, null, 0L, null, pkValues, encodedKey, projection, "driver", 0, 0L);
    }

    private List<String> extractPointProjectionColumns(PlainSelect ps) {
        List<SelectItem<?>> items = ps.getSelectItems();
        if (items == null) return List.of();
        List<String> result = new ArrayList<>();
        for (SelectItem<?> item : items) {
            Expression expr = item.getExpression();
            if (expr instanceof AllColumns || expr instanceof AllTableColumns) return List.of();
            if (expr instanceof Column col) {
                result.add(col.getColumnName());
            } else if (item.getAlias() != null) {
                result.add(item.getAlias().getName());
            } else {
                result.add(expr.toString());
            }
        }
        return result;
    }

    private LockMode extractLockMode(PlainSelect ps) {
        net.sf.jsqlparser.statement.select.ForMode forMode = ps.getForMode();
        if (forMode == null) return LockMode.NONE;
        return switch (forMode) {
            case UPDATE, NO_KEY_UPDATE -> LockMode.FOR_UPDATE;
            case SHARE, KEY_SHARE -> LockMode.FOR_SHARE;
        };
    }

    private MetadataQueryIR tryMatchMetadataQuery(
            PlainSelect ps, @Nullable Long requestedSnapshotId) {
        if (ps.getGroupBy() != null && !ps.getGroupBy().getGroupByExpressions().isEmpty())
            return null;
        List<SelectItem<?>> items = ps.getSelectItems();
        if (items == null || items.size() != 1) return null;

        String tableRef = extractTableRef(ps);
        if (tableRef == null) return null;

        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        long snapshotId =
                requestedSnapshotId != null
                        ? requestedSnapshotId
                        : resolveLatestSnapshotId(tableRef);
        if (snapshotId < 0) return null;

        SelectItem<?> item = items.get(0);
        Expression expr = item.getExpression();
        if (!(expr instanceof Function func)) return null;
        String funcName = func.getName().toLowerCase();
        if (!funcName.equals("count") || func.getParameters() == null) return null;

        if (func.isDistinct() || func.getParameters().getExpressions().size() != 1) return null;

        Expression arg = func.getParameters().getExpressions().get(0);
        if (arg instanceof AllColumns || arg instanceof AllTableColumns) {
            return new MetadataQueryIR.CountAll(meta, snapshotId);
        }
        if (arg instanceof Column col) {
            return new MetadataQueryIR.CountColumn(
                    meta, snapshotId, col.getColumnName(), func.isDistinct());
        }
        return null;
    }

    private WriteQueryIR tryMatchInsert(Insert insert, @Nullable Long requestedSnapshotId) {
        Table table = insert.getTable();
        if (table == null) return null;

        String tableRef = table.getFullyQualifiedName();
        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        List<String> columns =
                insert.getColumns() != null
                        ? insert.getColumns().stream().map(Column::getColumnName).toList()
                        : List.of();

        // 若 INSERT 不显式指定列名（如 INSERT INTO t VALUES (1, 'a')），
        // 用表 schema 的字段顺序作为列名，否则会用 col0/col1 占位符导致 PK 解析失败。
        List<String> effectiveColumns =
                !columns.isEmpty()
                        ? columns
                        : meta.schema().fields().stream().map(f -> f.name()).toList();

        @SuppressWarnings({"unchecked", "rawtypes"})
        ExpressionList items = insert.getValues().getExpressions();
        List<Map<String, Comparable<?>>> rows = new ArrayList<>();

        int itemIdx = 0;
        for (Object itemObj : items) {
            Expression expr = (Expression) itemObj;
            // 多行 INSERT VALUES (1,'a'), (2,'b') 的每个括号组在 jsqlparser 5.x 中
            // 表现为 ParenthesedExpressionList（而非 RowConstructor）。两者都视为一行。
            List<?> rowExprs = null;
            if (expr instanceof RowConstructor rc) {
                rowExprs = rc.getExpressions();
            } else if (expr
                    instanceof
                    net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList
                                    pel) {
                rowExprs = pel;
            }
            if (rowExprs != null) {
                Map<String, Comparable<?>> row = new LinkedHashMap<>();
                for (int i = 0; i < rowExprs.size(); i++) {
                    String colName =
                            i < effectiveColumns.size() ? effectiveColumns.get(i) : "col" + i;
                    Comparable<?> value = extractLiteralValue((Expression) rowExprs.get(i));
                    if (value != null) {
                        row.put(colName, value);
                    }
                }
                if (!row.isEmpty()) rows.add(row);
            } else {
                // Single value per VALUES parenthesized group:
                // INSERT INTO t VALUES (1, "hello") may be parsed as
                // ExpressionList[1, "hello"] (no RowConstructor wrapper).
                // We need to collect values into one row using positional column names.
                if (itemIdx == 0 || rows.isEmpty()) {
                    rows.add(new LinkedHashMap<>());
                }
                Map<String, Comparable<?>> row = rows.getLast();
                int colIdx = row.size(); // next available position in current row
                String colName =
                        colIdx < effectiveColumns.size()
                                ? effectiveColumns.get(colIdx)
                                : "col" + colIdx;
                Comparable<?> value = extractLiteralValue(expr);
                if (value != null) {
                    row.put(colName, value);
                }
            }
            itemIdx++;
        }

        if (rows.isEmpty()) return null;

        WriteType writeType =
                (rows.size() > 1 || (columns != null && columns.size() > 1))
                        ? WriteType.BATCH_INSERT
                        : WriteType.INSERT;

        return new WriteQueryIR(
                meta,
                writeType,
                rows.stream().map(r -> normalizeBySchema(r, meta.schema())).toList(),
                null);
    }

    private QueryIR tryMatchUpdate(Update update, @Nullable Long requestedSnapshotId) {
        Table table = update.getTable();
        if (table == null) return null;

        String tableRef = table.getFullyQualifiedName();
        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        List<String> pkCols = meta.primaryKeyColumns();
        boolean hasPk = pkCols != null && !pkCols.isEmpty();

        Map<String, Comparable<?>> setClauses = extractSetClauses(update.getUpdateSets());
        if (setClauses == null || setClauses.isEmpty()) return null;

        io.github.rawwise.model.expression.Expression whereExpr = null;
        if (update.getWhere() != null) {
            whereExpr = convertExpression(update.getWhere());
        }

        long snapshotId =
                requestedSnapshotId != null
                        ? requestedSnapshotId
                        : resolveLatestSnapshotId(tableRef);

        if (hasPk && update.getWhere() != null) {
            Map<String, Comparable<?>> pkValues =
                    extractPkEqualityValues(update.getWhere(), pkCols);
            if (pkValues != null) {
                pkValues = normalizeBySchema(pkValues, meta.schema());
                Map<String, Comparable<?>> normalizedSet =
                        normalizeBySchema(setClauses, meta.schema());
                return new UpdateQueryIR(
                        meta,
                        pkValues,
                        new byte[0],
                        normalizedSet,
                        snapshotId,
                        UpdateMode.POINT,
                        "driver",
                        0,
                        0L);
            }
        }

        // Phase 2: 非 PK 等值或无 WHERE → UpdateRangeIR (LOGICAL_PLAN → LazyUpdate pipeline)
        return new UpdateRangeIR(meta, setClauses, whereExpr);
    }

    private QueryIR tryMatchDelete(Delete delete, @Nullable Long requestedSnapshotId) {
        Table table = delete.getTable();
        if (table == null) return null;

        String tableRef = table.getFullyQualifiedName();
        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        List<String> pkCols = meta.primaryKeyColumns();
        boolean hasPk = pkCols != null && !pkCols.isEmpty();

        io.github.rawwise.model.expression.Expression whereExpr = null;
        if (delete.getWhere() != null) {
            whereExpr = convertExpression(delete.getWhere());
        }

        long snapshotId =
                requestedSnapshotId != null
                        ? requestedSnapshotId
                        : resolveLatestSnapshotId(tableRef);

        if (hasPk && delete.getWhere() != null) {
            Map<String, Comparable<?>> pkValues =
                    extractPkEqualityValues(delete.getWhere(), pkCols);
            if (pkValues != null) {
                pkValues = normalizeBySchema(pkValues, meta.schema());
                return new DeleteQueryIR(
                        meta, pkValues, new byte[0], snapshotId, UpdateMode.POINT, "driver", 0, 0L);
            }
        }

        // Phase 2: 非 PK 等值或无 WHERE → DeleteRangeIR (LOGICAL_PLAN → LazyUpdate pipeline)
        return new DeleteRangeIR(meta, whereExpr);
    }

    /**
     * 从 WHERE 表达式中提取 PK 等值谓词的值。 仅当 WHERE 是 PK 列的等值 AND 连接（覆盖全部 PK 列）时返回非 null。 支持：WHERE pk1=v1 AND
     * pk2=v2（顺序无关）。
     */
    private Map<String, Comparable<?>> extractPkEqualityValues(
            Expression where, List<String> pkCols) {
        if (where == null) return null;
        Map<String, Comparable<?>> collected = new LinkedHashMap<>();
        List<Expression> conjuncts = new ArrayList<>();
        flattenConjuncts(where, conjuncts);
        for (Expression conjunct : conjuncts) {
            if (!(conjunct instanceof EqualsTo eq)) continue;
            String colName = null;
            Comparable<?> value = null;
            if (eq.getLeftExpression() instanceof Column col
                    && isLiteral(eq.getRightExpression())) {
                colName = col.getColumnName();
                value = extractLiteralValue(eq.getRightExpression());
            } else if (eq.getRightExpression() instanceof Column col
                    && isLiteral(eq.getLeftExpression())) {
                colName = col.getColumnName();
                value = extractLiteralValue(eq.getLeftExpression());
            } else {
                continue;
            }
            if (!pkCols.contains(colName)) continue;
            collected.put(colName, value);
        }
        if (collected.size() != pkCols.size()) return null;
        for (String pk : pkCols) {
            if (!collected.containsKey(pk)) return null;
        }
        return collected;
    }

    private void flattenConjuncts(Expression expr, List<Expression> out) {
        if (expr instanceof AndExpression and) {
            flattenConjuncts(and.getLeftExpression(), out);
            flattenConjuncts(and.getRightExpression(), out);
        } else if (expr instanceof Parenthesis p && p.getExpression() != null) {
            flattenConjuncts(p.getExpression(), out);
        } else {
            out.add(expr);
        }
    }

    private boolean isLiteral(Expression expr) {
        return expr instanceof LongValue
                || expr instanceof StringValue
                || expr instanceof DoubleValue
                || expr instanceof NullValue;
    }

    private Map<String, Comparable<?>> extractSetClauses(List<UpdateSet> updateSets) {
        if (updateSets == null || updateSets.isEmpty()) return null;
        Map<String, Comparable<?>> result = new LinkedHashMap<>();
        for (UpdateSet set : updateSets) {
            List<Column> cols = set.getColumns();
            if (cols == null || cols.isEmpty()) return null;
            ExpressionList<?> valList = set.getValues();
            if (valList == null || valList.size() != cols.size()) return null;
            for (int i = 0; i < cols.size(); i++) {
                String colName = cols.get(i).getColumnName();
                Expression valExpr = (Expression) valList.get(i);
                Comparable<?> value = extractLiteralValue(valExpr);
                result.put(colName, value);
            }
        }
        return result;
    }

    /**
     * 按 schema 字段类型把 SQL 字面量值规范化为 Spark 写入路径相同的运行时类型。
     *
     * <p>背景：jsqlparser 把所有整数字面量解析为 LongValue → fromProtoLiteral 返回 Long → keyCodec 走 TYPE_LONG
     * 8-byte 编码；而 Spark INSERT 路径 RawWiseDataSource 把 IntegerType 字段读为 Integer → keyCodec 走
     * TYPE_INT 4-byte 编码。结果：同一 INT PK 表 Spark 写 vs FastPath 读 PK 字节不同 → Miss。 本方法在 frontend parser
     * 层把值类型对齐 schema 字段类型，让两条路径用同一 keyCodec 分支。
     */
    private static Map<String, Comparable<?>> normalizeBySchema(
            Map<String, Comparable<?>> values, io.github.rawwise.model.record.StructType schema) {
        if (schema == null || values == null || values.isEmpty()) return values;
        Map<String, Comparable<?>> out = new LinkedHashMap<>();
        for (var e : values.entrySet()) {
            Comparable<?> v = e.getValue();
            io.github.rawwise.model.record.StructField f = schema.field(e.getKey()).orElse(null);
            if (v != null && f != null) {
                String typeName =
                        f.dataType().name(); // "int8" / "int32" / "int64" / "float32" / "float64" /
                // "string"...
                v = coerceByTypeName(v, typeName);
            }
            out.put(e.getKey(), v);
        }
        return out;
    }

    private static Comparable<?> coerceByTypeName(Comparable<?> v, String typeName) {
        if (v == null || typeName == null) return v;
        return switch (typeName) {
            case "int8" -> v instanceof Number n ? Byte.valueOf(n.byteValue()) : v;
            case "int16" -> v instanceof Number n ? Short.valueOf(n.shortValue()) : v;
            case "int32" -> v instanceof Number n ? Integer.valueOf(n.intValue()) : v;
            case "int64" -> v instanceof Number n ? Long.valueOf(n.longValue()) : v;
            case "float32" -> v instanceof Number n ? Float.valueOf(n.floatValue()) : v;
            case "float64" -> v instanceof Number n ? Double.valueOf(n.doubleValue()) : v;
            default -> v;
        };
    }

    private String extractTableRef(PlainSelect ps) {
        FromItem from = ps.getFromItem();
        if (from instanceof Table table) {
            return table.getFullyQualifiedName();
        }
        return null;
    }

    private List<ProjectionColumn> extractProjections(PlainSelect ps) {
        if (ps.getSelectItems() == null) return List.of();
        List<ProjectionColumn> result = new ArrayList<>();
        for (SelectItem<?> item : ps.getSelectItems()) {
            Expression expr = item.getExpression();
            if (expr instanceof AllColumns || expr instanceof AllTableColumns) return List.of();
            if (expr instanceof Column col) {
                result.add(new ProjectionColumn(col.getColumnName(), -1, null, null));
            } else if (expr instanceof Function func) {
                String name = func.getName() != null ? func.getName() : expr.toString();
                String alias = item.getAlias() != null ? item.getAlias().getName() : name;
                result.add(new ProjectionColumn(alias, -1, null, null));
            } else {
                String alias =
                        item.getAlias() != null ? item.getAlias().getName() : expr.toString();
                result.add(new ProjectionColumn(alias, -1, null, null));
            }
        }
        return result;
    }

    private io.github.rawwise.model.expression.Expression extractFilter(PlainSelect ps) {
        Expression where = ps.getWhere();
        if (where == null) return null;
        return convertExpression(where);
    }

    private int extractLimit(PlainSelect ps) {
        Limit limit = ps.getLimit();
        if (limit != null && limit.getRowCount() instanceof LongValue lv) {
            return (int) lv.getValue();
        }
        return 0;
    }

    private int extractOffset(PlainSelect ps) {
        Limit limit = ps.getLimit();
        if (limit != null && limit.getOffset() instanceof LongValue lv) {
            return (int) lv.getValue();
        }
        return 0;
    }

    private List<SortKey> extractSortKeys(PlainSelect ps) {
        List<OrderByElement> orderBy = ps.getOrderByElements();
        if (orderBy == null || orderBy.isEmpty()) return List.of();

        List<SortKey> keys = new ArrayList<>();
        for (OrderByElement element : orderBy) {
            Expression expr = element.getExpression();
            if (expr instanceof Column col) {
                keys.add(
                        new SortKey(
                                col.getColumnName(),
                                element.isAsc(),
                                element.getNullOrdering()
                                        == OrderByElement.NullOrdering.NULLS_FIRST));
            } else {
                return null;
            }
        }
        return keys;
    }

    private io.github.rawwise.model.expression.Expression convertExpression(Expression expr) {
        if (expr instanceof EqualsTo eq) {
            io.github.rawwise.model.expression.Expression left =
                    convertExpression(eq.getLeftExpression());
            io.github.rawwise.model.expression.Expression right =
                    convertExpression(eq.getRightExpression());
            if (left == null || right == null) return null;
            return new Eq(left, right);
        }
        if (expr instanceof MinorThan mt) {
            io.github.rawwise.model.expression.Expression left =
                    convertExpression(mt.getLeftExpression());
            io.github.rawwise.model.expression.Expression right =
                    convertExpression(mt.getRightExpression());
            if (left == null || right == null) return null;
            return new Lt(left, right);
        }
        if (expr instanceof MinorThanEquals mte) {
            io.github.rawwise.model.expression.Expression left =
                    convertExpression(mte.getLeftExpression());
            io.github.rawwise.model.expression.Expression right =
                    convertExpression(mte.getRightExpression());
            if (left == null || right == null) return null;
            return new LtEq(left, right);
        }
        if (expr instanceof GreaterThan gt) {
            io.github.rawwise.model.expression.Expression left =
                    convertExpression(gt.getLeftExpression());
            io.github.rawwise.model.expression.Expression right =
                    convertExpression(gt.getRightExpression());
            if (left == null || right == null) return null;
            return new Gt(left, right);
        }
        if (expr instanceof GreaterThanEquals gte) {
            io.github.rawwise.model.expression.Expression left =
                    convertExpression(gte.getLeftExpression());
            io.github.rawwise.model.expression.Expression right =
                    convertExpression(gte.getRightExpression());
            if (left == null || right == null) return null;
            return new GtEq(left, right);
        }
        if (expr instanceof AndExpression and) {
            io.github.rawwise.model.expression.Expression left =
                    convertExpression(and.getLeftExpression());
            io.github.rawwise.model.expression.Expression right =
                    convertExpression(and.getRightExpression());
            if (left == null || right == null) return null;
            List<io.github.rawwise.model.expression.Expression> children = new ArrayList<>();
            flattenAnd(left, children);
            flattenAnd(right, children);
            return new io.github.rawwise.model.expression.And(children);
        }
        if (expr instanceof OrExpression or) {
            io.github.rawwise.model.expression.Expression left =
                    convertExpression(or.getLeftExpression());
            io.github.rawwise.model.expression.Expression right =
                    convertExpression(or.getRightExpression());
            if (left == null || right == null) return null;
            List<io.github.rawwise.model.expression.Expression> children = new ArrayList<>();
            flattenOr(left, children);
            flattenOr(right, children);
            return new io.github.rawwise.model.expression.Or(children);
        }
        if (expr instanceof NotExpression not) {
            io.github.rawwise.model.expression.Expression child =
                    not.getExpression() != null ? convertExpression(not.getExpression()) : null;
            if (child == null) return null;
            return new io.github.rawwise.model.expression.Not(child);
        }
        if (expr instanceof IsNullExpression isNull) {
            io.github.rawwise.model.expression.Expression child =
                    convertExpression(isNull.getLeftExpression());
            if (child == null) return null;
            return new io.github.rawwise.model.expression.IsNull(child);
        }
        if (expr instanceof Column col) {
            return new ColumnRef(col.getColumnName());
        }
        if (expr instanceof LongValue lv) {
            return new io.github.rawwise.model.expression.Literal(lv.getValue());
        }
        if (expr instanceof StringValue sv) {
            return new io.github.rawwise.model.expression.Literal(sv.getValue());
        }
        if (expr instanceof DoubleValue dv) {
            return new io.github.rawwise.model.expression.Literal(dv.getValue());
        }
        if (expr instanceof NullValue) {
            return new io.github.rawwise.model.expression.Literal(null);
        }
        if (expr instanceof Parenthesis p) {
            return convertExpression(p.getExpression());
        }
        return null;
    }

    private void flattenAnd(
            io.github.rawwise.model.expression.Expression expr,
            List<io.github.rawwise.model.expression.Expression> out) {
        if (expr instanceof io.github.rawwise.model.expression.And and) {
            out.addAll(and.children());
        } else {
            out.add(expr);
        }
    }

    private void flattenOr(
            io.github.rawwise.model.expression.Expression expr,
            List<io.github.rawwise.model.expression.Expression> out) {
        if (expr instanceof io.github.rawwise.model.expression.Or or) {
            out.addAll(or.children());
        } else {
            out.add(expr);
        }
    }

    private Comparable<?> extractLiteralValue(Expression expr) {
        if (expr instanceof LongValue lv) return lv.getValue();
        if (expr instanceof StringValue sv) return sv.getValue();
        if (expr instanceof DoubleValue dv) return dv.getValue();
        if (expr instanceof NullValue) return null;
        return expr.toString();
    }

    private TableMetaIR resolveTableMeta(String tableRef) {
        TableMetaIR cached = tableMetaCache.get(tableRef);
        if (cached != null) return cached;
        try {
            TableId id = TableId.parseLenient(tableRef);
            RawWiseTable table = catalog.loadTable(id);
            TableMetaIR meta = convertToTableMetaIR(table);
            tableMetaCache.put(tableRef, meta);
            return meta;
        } catch (CatalogException e) {
            log.info(
                    "resolveTableMeta: Cannot resolve table meta for {}: {}",
                    tableRef,
                    e.getMessage());
            return null;
        }
    }

    private TableMetaIR convertToTableMetaIR(RawWiseTable table) {
        TableMeta meta = table.meta();
        List<String> pkFields = meta.primaryKey().fields();
        if (pkFields.isEmpty() && meta.properties().containsKey("primary-key")) {
            pkFields = List.of(meta.properties().get("primary-key").split(","));
        }
        return new TableMetaIR(
                table.getTableIdentifier(),
                meta.tableId(),
                meta.tableType() == TableMeta.TableType.APPEND_ONLY
                        ? TableType.APPEND_ONLY
                        : TableType.PRIMARY_KEY,
                meta.schema().structType(),
                pkFields,
                meta.properties(),
                Math.toIntExact(Math.min(meta.schema().version(), Integer.MAX_VALUE)));
    }

    private DDLIR tryMatchCreateIndex(net.sf.jsqlparser.statement.create.index.CreateIndex ci) {
        net.sf.jsqlparser.schema.Table table = ci.getTable();
        if (table == null) return null;
        String tableRef = table.getFullyQualifiedName();
        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        net.sf.jsqlparser.statement.create.table.Index idx = ci.getIndex();
        if (idx == null || idx.getName() == null) return null;
        String indexName = String.join("_", idx.getName());
        List<String> columns = idx.getColumnsNames();

        Map<String, Object> options = new LinkedHashMap<>();
        options.put("indexName", indexName);
        options.put("columns", columns);
        return new DDLIR(meta, DDLType.CREATE_INDEX, options);
    }

    private DDLIR tryMatchAlterTableConstraint(net.sf.jsqlparser.statement.alter.Alter alter) {
        net.sf.jsqlparser.schema.Table table = alter.getTable();
        if (table == null) return null;
        String tableRef = table.getFullyQualifiedName();
        TableMetaIR meta = resolveTableMeta(tableRef);
        if (meta == null) return null;

        List<net.sf.jsqlparser.statement.alter.AlterExpression> exps = alter.getAlterExpressions();
        if (exps == null || exps.isEmpty()) return null;

        for (net.sf.jsqlparser.statement.alter.AlterExpression exp : exps) {
            if (exp.getOperation() == net.sf.jsqlparser.statement.alter.AlterOperation.DROP_UNIQUE
                    || exp.getOperation()
                            == net.sf.jsqlparser.statement.alter.AlterOperation.DROP_FOREIGN_KEY) {
                Map<String, Object> options = new LinkedHashMap<>();
                options.put("alterOp", exp.getOperation().name());
                options.put("parameters", exp.getParameters());
                return new DDLIR(meta, DDLType.ALTER_TABLE, options);
            }
        }
        return null;
    }

    private long resolveLatestSnapshotId(String tableRef) {
        try {
            return snapshotManager
                    .latestSnapshot(tableRef)
                    .map(io.github.rawwise.model.record.Snapshot::snapshotId)
                    .orElse(0L);
        } catch (Exception e) {
            log.debug("Cannot resolve latest snapshot for {}: {}", tableRef, e.getMessage());
            return 0L;
        }
    }
}
