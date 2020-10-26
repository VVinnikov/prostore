package io.arenadata.dtm.query.calcite.core.factory.impl;

import io.arenadata.dtm.query.calcite.core.factory.SchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;

public abstract class DtmSchemaFactory implements SchemaFactory {
    @Override
    public QueryableSchema create(SchemaPlus parentSchema, Datamart datamart) {
        Expression expression = Schemas.subSchemaExpression(parentSchema, datamart.getMnemonic(), QueryableSchema.class);
        DtmConvention convention = createDtmConvention(datamart, expression);
        return new QueryableSchema(convention);
    }

    protected abstract DtmConvention createDtmConvention(Datamart datamart, Expression expression);
}
