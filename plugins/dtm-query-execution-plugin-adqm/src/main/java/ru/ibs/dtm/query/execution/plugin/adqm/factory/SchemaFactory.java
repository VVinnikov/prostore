package ru.ibs.dtm.query.execution.plugin.adqm.factory;

import org.apache.calcite.schema.SchemaPlus;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.QueryableSchema;

public interface SchemaFactory {
    QueryableSchema create(SchemaPlus parentSchema, Datamart datamart);
}
