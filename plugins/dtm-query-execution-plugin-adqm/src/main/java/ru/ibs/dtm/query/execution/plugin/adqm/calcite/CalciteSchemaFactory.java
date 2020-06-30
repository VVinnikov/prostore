package ru.ibs.dtm.query.execution.plugin.adqm.calcite;

import org.apache.calcite.schema.SchemaPlus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.CustomTable;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.SchemaFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.model.metadata.Datamart;

@Service
public class CalciteSchemaFactory {
    private SchemaFactory schemaFactory;

    @Autowired
    public CalciteSchemaFactory(SchemaFactory schemaFactory) {
        this.schemaFactory = schemaFactory;
    }

    public SchemaPlus addSchema(SchemaPlus parent, Datamart root) {
        QueryableSchema dtmSchema = schemaFactory.create(parent, root);
        SchemaPlus schemaPlus = parent.add(root.getMnemonic(), dtmSchema);
        root.getDatamartClassess().forEach(it -> {
            try {
                CustomTable table = new CustomTable(dtmSchema, it);
                schemaPlus.add(it.getMnemonic(), table);
            } catch (Exception e) {
                throw new RuntimeException("Ошибка инициализации таблицы $metaTable", e);
            }
        });
        return schemaPlus;
    }
}
