package io.github.rawwise.frontend.ir.adaptor;

import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.StructType;
import lombok.extern.slf4j.Slf4j;

/**
 * DuckDB IR adaptor placeholder.
 *
 * <p>Produces SQL text strings (to be replaced with DuckDB Appender API / native plan objects when
 * the DuckDB integration is built).
 */
@Slf4j
public class DuckDBIRAdaptor implements IRAdaptor<String> {

    @Override
    public String adapt(QueryIR ir) {
        return switch (ir.type()) {
            case SCAN -> buildDuckDBSQL((ScanQueryIR) ir);
            case AGGREGATE -> buildDuckDBAggregateSQL((AggregateQueryIR) ir);
            case JOIN -> buildDuckDBJoinSQL((JoinQueryIR) ir);
            case INSERT, BATCH_INSERT -> buildDuckDBWriteSQL((WriteQueryIR) ir);
            case UPDATE_RANGE -> buildDuckDBUpdateSQL((UpdateRangeIR) ir);
            case DELETE_RANGE -> buildDuckDBDeleteSQL((DeleteRangeIR) ir);
            case CREATE_TABLE,
                            DROP_TABLE,
                            ALTER_TABLE,
                            TRUNCATE,
                            REFRESH,
                            CREATE_INDEX,
                            DROP_INDEX ->
                    buildDuckDBDDLSQL((DDLIR) ir);
            case POINT_QUERY,
                    BATCH_POINT_QUERY,
                    POINT_UPDATE,
                    POINT_DELETE,
                    METADATA_COUNT_ALL,
                    METADATA_COUNT_COLUMN,
                    METADATA_MIN,
                    METADATA_MAX,
                    METADATA_TABLE_INFO -> {
                log.error("{} should not reach DuckDBIRAdaptor", ir.type());
                throw new UnsupportedOperationException(
                        ir.type() + " not supported by DuckDB adaptor");
            }
        };
    }

    private String buildDuckDBSQL(ScanQueryIR q) {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (q.projection() == null || q.projection().isEmpty()) {
            sb.append("*");
        } else {
            sb.append(
                    q.projection().stream()
                            .map(p -> p.alias() != null ? p.name() + " AS " + p.alias() : p.name())
                            .reduce((a, b) -> a + ", " + b)
                            .orElse("*"));
        }
        sb.append(" FROM ").append(q.tableMeta().tableIdentifier());
        if (q.filter() != null) {
            sb.append(" WHERE ").append(q.filter());
        }
        if (q.limit() > 0) {
            sb.append(" LIMIT ").append(q.limit());
        }
        return sb.toString();
    }

    private String buildDuckDBAggregateSQL(AggregateQueryIR aq) {
        return "SELECT COUNT(*) FROM " + aq.tableMetas().get(0).tableIdentifier();
    }

    private String buildDuckDBJoinSQL(JoinQueryIR jq) {
        return "SELECT * FROM "
                + jq.tableMetas().get(0).tableIdentifier()
                + " "
                + jq.joinType()
                + " JOIN ...";
    }

    private String buildDuckDBWriteSQL(WriteQueryIR wq) {
        return "INSERT INTO " + wq.tableMeta().tableIdentifier() + " ...";
    }

    private String buildDuckDBUpdateSQL(UpdateRangeIR ur) {
        return "UPDATE " + ur.tableMeta().tableIdentifier() + " SET ...";
    }

    private String buildDuckDBDeleteSQL(DeleteRangeIR dr) {
        return "DELETE FROM " + dr.tableMeta().tableIdentifier() + " ...";
    }

    private String buildDuckDBDDLSQL(DDLIR dd) {
        return dd.ddlType() + " " + dd.tableMeta().tableIdentifier();
    }

    @Override
    public Object adaptPredicate(Expression predicate) {
        return predicate.toString();
    }

    @Override
    public Object adaptSchema(StructType schema) {
        return schema.toString();
    }
}
