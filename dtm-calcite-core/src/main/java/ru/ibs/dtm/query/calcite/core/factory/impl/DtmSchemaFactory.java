package ru.ibs.dtm.query.calcite.core.factory.impl;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import ru.ibs.dtm.query.calcite.core.factory.SchemaFactory;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.calcite.core.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

public abstract class DtmSchemaFactory implements SchemaFactory {
    @Override
    public QueryableSchema create(SchemaPlus parentSchema, Datamart datamart) {
        Expression expression = Schemas.subSchemaExpression(parentSchema, datamart.getMnemonic(), QueryableSchema.class);
        DtmConvention convention = createDtmConvention(datamart, expression);
        return new QueryableSchema(convention);
    }

    protected abstract DtmConvention createDtmConvention(Datamart datamart, Expression expression);
}