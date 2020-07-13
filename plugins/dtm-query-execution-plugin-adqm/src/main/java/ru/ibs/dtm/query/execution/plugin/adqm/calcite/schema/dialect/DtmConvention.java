package ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.dialect;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Кастомизированный Relation Trait
 */
public class DtmConvention extends Convention.Impl {

    private Datamart datamart;
    private Expression schemaExpression;
    private Collection<String> functions = new ArrayList<>();
    private Collection<String> aggregateFunctions = new ArrayList<>();

    public DtmConvention(Datamart datamart, Expression schemaExpression) {
        super("DtmConvention", DtmRelation.class);
        this.datamart = datamart;
        this.schemaExpression = schemaExpression;
    }

    @Override
    public void register(RelOptPlanner planner) {
        //TODO: доделать задаче исполнения
    }

    public static SqlDialect getDialect() {
        SqlDialect.Context CONTEXT = SqlDialect.EMPTY_CONTEXT
                .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
                .withIdentifierQuoteString("")
                .withUnquotedCasing(Casing.TO_LOWER)
                .withCaseSensitive(false)
                .withQuotedCasing(Casing.UNCHANGED);
        return new PostgresqlSqlDialect(CONTEXT);
    }

    public Datamart getDatamart() {
        return datamart;
    }

    public Expression getSchemaExpression() {
        return schemaExpression;
    }

    public Collection<String> getFunctions() {
        return functions;
    }

    public Collection<String> getAggregateFunctions() {
        return aggregateFunctions;
    }
}
