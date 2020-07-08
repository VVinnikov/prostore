package ru.ibs.dtm.query.calcite.core.factory.impl;

import org.apache.calcite.schema.SchemaPlus;
import ru.ibs.dtm.query.calcite.core.factory.SchemaFactory;
import ru.ibs.dtm.query.calcite.core.schema.DtmTable;
import ru.ibs.dtm.query.calcite.core.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartClass;

public abstract class CalciteSchemaFactory {
    private final SchemaFactory schemaFactory;

    public CalciteSchemaFactory(SchemaFactory schemaFactory) {
        this.schemaFactory = schemaFactory;
    }

    public SchemaPlus addSchema(SchemaPlus parent, Datamart root) {
        QueryableSchema dtmSchema = schemaFactory.create(parent, root);
        SchemaPlus schemaPlus = parent.add(root.getMnemonic(), dtmSchema);
        root.getDatamartClassess().forEach(it -> {
            try {
                DtmTable table = createTable(dtmSchema, it);
                schemaPlus.add(it.getMnemonic(), table);
            } catch (Exception e) {
                throw new RuntimeException("Ошибка инициализации таблицы $metaTable", e);
            }
        });
        return schemaPlus;
    }

    protected abstract DtmTable createTable(QueryableSchema schema, DatamartClass datamartClass);
}
