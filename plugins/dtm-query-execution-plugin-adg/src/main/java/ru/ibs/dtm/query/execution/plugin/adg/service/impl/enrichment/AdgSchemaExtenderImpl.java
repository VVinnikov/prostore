package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.*;
import ru.ibs.dtm.query.execution.plugin.adg.service.SchemaExtender;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;


/**
 * Реализация преобразования для логической схемы в физическую
 */
@Service("adgSchemaExtender")
public class AdgSchemaExtenderImpl implements SchemaExtender {

  @Override
  public Datamart generatePhysicalSchema(Datamart datamart, QueryRequest queryRequest) {
    Datamart extendedSchema = new Datamart();
    extendedSchema.setMnemonic(datamart.getMnemonic());
    extendedSchema.setId(UUID.randomUUID());
    List<DatamartTable> extendedDatamartTables = new ArrayList<>();
    String prefix = queryRequest.getSystemName() + "_" + queryRequest.getDatamartMnemonic() + "_";

    datamart.getDatamartTables().forEach(dmClass -> {
      dmClass.setSchema(dmClass.getSchema());
      dmClass.getTableAttributes().addAll(getExtendedColumns());
      extendedDatamartTables.add(dmClass);
      extendedDatamartTables.add(getExtendedSchema(dmClass, prefix, HISTORY_POSTFIX));
      extendedDatamartTables.add(getExtendedSchema(dmClass, prefix, STAGING_POSTFIX));
      extendedDatamartTables.add(getExtendedSchema(dmClass, prefix, ACTUAL_POSTFIX));
    });
    extendedSchema.setDatamartTables(extendedDatamartTables);

    return extendedSchema;
  }

  private DatamartTable getExtendedSchema(DatamartTable datamartTable, String prefix, String tablePostfix) {
    DatamartTable datamartTableExtended = new DatamartTable();
    datamartTableExtended.setLabel(datamartTable.getLabel());
    datamartTableExtended.setSchema(prefix + datamartTable.getLabel() + tablePostfix);
    datamartTableExtended.setId(UUID.randomUUID());
    List<TableAttribute> tableAttributeList = new ArrayList<>();
    datamartTable.getTableAttributes().forEach(classAttr -> {
      TableAttribute tableAttribute = new TableAttribute();
      tableAttribute.setId(UUID.randomUUID());
      tableAttribute.setMnemonic(classAttr.getMnemonic());
      tableAttribute.setType(classAttr.getType());
      tableAttributeList.add(tableAttribute);
    });
    datamartTableExtended.setTableAttributes(tableAttributeList);
    return datamartTableExtended;
  }

  private List<TableAttribute> getExtendedColumns() {
    List<TableAttribute> tableAttributeList = new ArrayList<>();
    tableAttributeList.add(generateNewField(SYS_OP_FIELD, ColumnType.INTEGER));
    tableAttributeList.add(generateNewField(SYS_TO_FIELD, ColumnType.INTEGER));
    tableAttributeList.add(generateNewField(SYS_FROM_FIELD, ColumnType.INTEGER));
    return tableAttributeList;
  }

  private TableAttribute generateNewField(String mnemonic, ColumnType columnType) {
    TableAttribute tableAttribute = new TableAttribute();
    tableAttribute.setId(UUID.randomUUID());
    tableAttribute.setMnemonic(mnemonic);
    tableAttribute.setType(new AttributeType(UUID.randomUUID(), columnType));
    return tableAttribute;
  }

}
