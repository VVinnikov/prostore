package ru.ibs.dtm.query.execution.plugin.adg.calcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.schema.CustomTable;
import ru.ibs.dtm.query.execution.plugin.adg.calcite.schema.QueryableSchema;
import ru.ibs.dtm.query.execution.plugin.adg.factory.SchemaFactory;

@Service
public class CalciteSchemaFactory {
  private SchemaFactory schemaFactory;

  @Autowired
  public CalciteSchemaFactory(SchemaFactory schemaFactory) {
    this.schemaFactory = schemaFactory;
  }

  public Schema addRootSchema(SchemaPlus parent, Datamart schema) {
    QueryableSchema dtmSchema = schemaFactory.create(parent, schema);
    schema.getDatamartTableClassesses().forEach(it -> {
      try {
        CustomTable table = new CustomTable(dtmSchema, it);
        parent.add(it.getSchema(), table);
      } catch (Exception e) {
        throw new RuntimeException("Ошибка инициализации таблицы $metaTable", e);
      }
    });
    return parent;
  }

  public Schema addSubSchema(SchemaPlus parent, Datamart root) {
    QueryableSchema dtmSchema = schemaFactory.create(parent, root);
    SchemaPlus rootSchema = parent.add(root.getMnemonic(), dtmSchema);
    root.getDatamartTableClassesses().forEach(it -> {
      try {
        CustomTable table = new CustomTable(dtmSchema, it);
        rootSchema.add(it.getSchema(), table);
      } catch (Exception e) {
        throw new RuntimeException("Ошибка инициализации таблицы $metaTable", e);
      }
    });
    return rootSchema;
  }
}
