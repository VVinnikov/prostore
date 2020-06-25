package ru.ibs.dtm.query.execution.plugin.adqm.factory;

import org.apache.calcite.schema.SchemaPlus;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.plugin.adqm.model.metadata.Datamart;

public interface SchemaFactory {
    QueryableSchema create(SchemaPlus parentSchema, Datamart datamart);
}
