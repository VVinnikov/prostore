package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.schema.QueryableSchema;
import org.apache.calcite.schema.SchemaPlus;

public interface SchemaFactory {
  QueryableSchema create(SchemaPlus parentSchema, Datamart datamart);
}
