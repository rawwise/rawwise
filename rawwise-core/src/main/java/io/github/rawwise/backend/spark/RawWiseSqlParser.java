package io.github.rawwise.backend.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Seq;

public class RawWiseSqlParser implements ParserInterface {

    private static final Logger log = LoggerFactory.getLogger(RawWiseSqlParser.class);

    private final SparkSession sparkSession;
    private final ParserInterface delegate;

    public RawWiseSqlParser(SparkSession sparkSession, ParserInterface delegate) {
        this.sparkSession = sparkSession;
        this.delegate = delegate;
    }

    @Override
    public LogicalPlan parsePlan(String sqlText) throws ParseException {
        LogicalPlan plan = delegate.parsePlan(sqlText);
        if (plan instanceof UpdateTable) {
            log.info("RawWiseSqlParser: detected UPDATE statement via ParserInterface injection");
        }
        return plan;
    }

    @Override
    public Expression parseExpression(String sqlText) throws ParseException {
        return delegate.parseExpression(sqlText);
    }

    @Override
    public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
        return delegate.parseTableIdentifier(sqlText);
    }

    @Override
    public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
        return delegate.parseFunctionIdentifier(sqlText);
    }

    @Override
    public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
        return delegate.parseMultipartIdentifier(sqlText);
    }

    @Override
    public StructType parseTableSchema(String sqlText) throws ParseException {
        return delegate.parseTableSchema(sqlText);
    }

    @Override
    public DataType parseDataType(String sqlText) throws ParseException {
        return delegate.parseDataType(sqlText);
    }

    @Override
    public LogicalPlan parseQuery(String sqlText) throws ParseException {
        return delegate.parseQuery(sqlText);
    }

    @Override
    public StructType parseRoutineParam(String sqlText) throws ParseException {
        return delegate.parseRoutineParam(sqlText);
    }
}
