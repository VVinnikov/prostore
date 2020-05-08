package ru.ibs.dtm.query.execution.plugin.adb.factory;

import org.apache.calcite.schema.SchemaPlus;
import ru.ibs.dtm.query.execution.plugin.adb.calcite.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.plugin.adb.model.metadata.Datamart;

public interface SchemaFactory {
  QueryableSchema create(SchemaPlus parentSchema, Datamart datamart);
}
