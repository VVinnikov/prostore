package io.arenadata.dtm.query.calcite.core.factory;

import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.schema.SchemaPlus;

public interface SchemaFactory {
  QueryableSchema create(SchemaPlus parentSchema, Datamart datamart);
}
