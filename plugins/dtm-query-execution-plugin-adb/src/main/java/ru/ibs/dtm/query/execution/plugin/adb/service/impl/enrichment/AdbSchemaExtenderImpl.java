package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.model.metadata.*;
import ru.ibs.dtm.query.execution.plugin.adb.service.SchemaExtender;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static ru.ibs.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl.*;


/**
 * Реализация преобразования для логической схемы в физическую
 */
@Service("adbSchemaExtender")
public class AdbSchemaExtenderImpl implements SchemaExtender {

  @Override
  public Datamart generatePhysicalSchema(Datamart datamart) {
    Datamart extendedSchema = new Datamart();
    extendedSchema.setMnemonic(datamart.getMnemonic());
    extendedSchema.setId(UUID.randomUUID());
    List<DatamartTable> extendedDatamartTables = new ArrayList<>();
    datamart.getDatamartTables().forEach(dmClass -> {
      dmClass.getTableAttributes().addAll(getExtendedColumns());
      extendedDatamartTables.add(dmClass);
      extendedDatamartTables.add(getExtendedSchema(dmClass, "_".concat(HISTORY_TABLE)));
      extendedDatamartTables.add(getExtendedSchema(dmClass, "_".concat(STAGING_TABLE)));
      extendedDatamartTables.add(getExtendedSchema(dmClass, "_".concat(ACTUAL_TABLE)));
    });
    extendedSchema.setDatamartTables(extendedDatamartTables);

    return extendedSchema;
  }

  private DatamartTable getExtendedSchema(DatamartTable datamartTable, String tablePostfix) {
    DatamartTable datamartTableExtended = new DatamartTable();
    datamartTableExtended.setLabel(datamartTable.getLabel());
    datamartTableExtended.setSchema(datamartTable.getLabel() + tablePostfix);
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
    tableAttributeList.add(generateNewField(SYS_OP_ATTR, ColumnType.INTEGER));
    tableAttributeList.add(generateNewField(SYS_TO_ATTR, ColumnType.INTEGER));
    tableAttributeList.add(generateNewField(SYS_FROM_ATTR, ColumnType.INTEGER));
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
