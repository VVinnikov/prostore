package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.schema.dialect.DtmConvention;
import ru.ibs.dtm.query.execution.plugin.adb.factory.SchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adb.model.metadata.Datamart;

@Service
public class SchemaFactoryImpl implements SchemaFactory {

  @Override
  public QueryableSchema create(SchemaPlus parentSchema, Datamart datamart) {
    Expression expression = Schemas.subSchemaExpression(parentSchema, datamart.getMnemonic(), QueryableSchema.class);
    DtmConvention convention = new DtmConvention(datamart, expression);
    return new QueryableSchema(convention);
  }
}
