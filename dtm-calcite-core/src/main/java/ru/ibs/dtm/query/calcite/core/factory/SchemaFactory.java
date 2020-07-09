package ru.ibs.dtm.query.calcite.core.factory;

import org.apache.calcite.schema.SchemaPlus;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

public interface SchemaFactory {
  QueryableSchema create(SchemaPlus parentSchema, Datamart datamart);
}
