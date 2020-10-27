package io.arenadata.dtm.query.calcite.core.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.factory.SchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.schema.SchemaPlus;

public abstract class CalciteSchemaFactory {
    private final SchemaFactory schemaFactory;

    public CalciteSchemaFactory(SchemaFactory schemaFactory) {
        this.schemaFactory = schemaFactory;
    }

    public SchemaPlus addSchema(SchemaPlus parent, Datamart root) {
        QueryableSchema dtmSchema = schemaFactory.create(parent, root);
        SchemaPlus schemaPlus = parent.add(root.getMnemonic(), dtmSchema);
        root.getEntities().forEach(it -> {
            try {
                DtmTable table = createTable(dtmSchema, it);
                schemaPlus.add(it.getName(), table);
            } catch (Exception e) {
                throw new RuntimeException("Table initialization error $metaTable", e);
            }
        });
        return schemaPlus;
    }

    protected abstract DtmTable createTable(QueryableSchema schema, Entity entity);
}