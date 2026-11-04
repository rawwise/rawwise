package io.github.rawwise.frontend.ir.adaptor;

import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.StructType;
import lombok.extern.slf4j.Slf4j;

/**
 * Trino IR adaptor placeholder.
 *
 * <p>Produces String representations of Trino plan nodes (to be replaced with actual Trino IR /
 * plan node types when the Trino connector is built).
 */
@Slf4j
public class TrinoIRAdaptor implements IRAdaptor<Object> {

    @Override
    public Object adapt(QueryIR ir) {
        return switch (ir.type()) {
            case SCAN -> adaptScan((ScanQueryIR) ir);
            case AGGREGATE -> adaptAggregate((AggregateQueryIR) ir);
            case JOIN -> adaptJoin((JoinQueryIR) ir);
            case INSERT, BATCH_INSERT -> adaptWrite((WriteQueryIR) ir);
            case UPDATE_RANGE -> adaptUpdateRange((UpdateRangeIR) ir);
            case DELETE_RANGE -> adaptDeleteRange((DeleteRangeIR) ir);
            case CREATE_TABLE,
                            DROP_TABLE,
                            ALTER_TABLE,
                            TRUNCATE,
                            REFRESH,
                            CREATE_INDEX,
                            DROP_INDEX ->
                    adaptDDL((DDLIR) ir);
            case POINT_QUERY,
                    BATCH_POINT_QUERY,
                    POINT_UPDATE,
                    POINT_DELETE,
                    METADATA_COUNT_ALL,
                    METADATA_COUNT_COLUMN,
                    METADATA_MIN,
                    METADATA_MAX,
                    METADATA_TABLE_INFO -> {
                log.error("{} should not reach TrinoIRAdaptor", ir.type());
                throw new UnsupportedOperationException(
                        ir.type() + " not supported by Trino adaptor");
            }
        };
    }

    private Object adaptScan(ScanQueryIR q) {
        return "TrinoPlanNode(Scan: " + q.tableMeta().tableIdentifier() + ")";
    }

    private Object adaptAggregate(AggregateQueryIR aq) {
        return "TrinoPlanNode(Aggregate: " + aq.tableMetas().get(0).tableIdentifier() + ")";
    }

    private Object adaptJoin(JoinQueryIR jq) {
        return "TrinoPlanNode(Join: " + jq.joinType() + ")";
    }

    private Object adaptWrite(WriteQueryIR wq) {
        return "TrinoPlanNode(Write: " + wq.tableMeta().tableIdentifier() + ")";
    }

    private Object adaptUpdateRange(UpdateRangeIR ur) {
        return "TrinoPlanNode(Update: " + ur.tableMeta().tableIdentifier() + ")";
    }

    private Object adaptDeleteRange(DeleteRangeIR dr) {
        return "TrinoPlanNode(Delete: " + dr.tableMeta().tableIdentifier() + ")";
    }

    private Object adaptDDL(DDLIR dd) {
        return "TrinoPlanNode(DDL: " + dd.ddlType() + " " + dd.tableMeta().tableIdentifier() + ")";
    }

    @Override
    public Object adaptPredicate(Expression predicate) {
        return "TrinoPredicate(" + predicate.getClass().getSimpleName() + ")";
    }

    @Override
    public Object adaptSchema(StructType schema) {
        return "TrinoSchema(" + schema.fields().size() + " fields)";
    }
}
