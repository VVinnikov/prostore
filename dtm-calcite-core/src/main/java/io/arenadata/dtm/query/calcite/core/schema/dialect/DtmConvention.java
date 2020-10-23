package io.arenadata.dtm.query.calcite.core.schema.dialect;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Кастомизированный Relation Trait
 */
public abstract class DtmConvention extends Convention.Impl {

    protected final Datamart datamart;
    protected final Expression schemaExpression;
    protected final Collection<String> functions = new ArrayList<>();
    protected final Collection<String> aggregateFunctions = new ArrayList<>();

    public DtmConvention(Datamart datamart, Expression schemaExpression) {
        super("DtmConvention", DtmRelation.class);
        this.datamart = datamart;
        this.schemaExpression = schemaExpression;
    }

    @Override
    public void register(RelOptPlanner planner) {
        //TODO: доделать задаче исполнения
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
