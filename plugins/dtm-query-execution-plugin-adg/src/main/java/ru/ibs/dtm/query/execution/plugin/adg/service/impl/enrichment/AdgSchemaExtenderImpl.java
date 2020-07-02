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
    List<DatamartClass> extendedDatamartClasses = new ArrayList<>();
    String prefix = queryRequest.getSystemName() + "_" + queryRequest.getDatamartMnemonic() + "_";

    datamart.getDatamartClassess().forEach(dmClass -> {
      dmClass.setMnemonic(dmClass.getMnemonic());
      dmClass.getClassAttributes().addAll(getExtendedColumns());
      extendedDatamartClasses.add(dmClass);
      extendedDatamartClasses.add(getExtendedSchema(dmClass, prefix, HISTORY_POSTFIX));
      extendedDatamartClasses.add(getExtendedSchema(dmClass, prefix, STAGING_POSTFIX));
      extendedDatamartClasses.add(getExtendedSchema(dmClass, prefix, ACTUAL_POSTFIX));
    });
    extendedSchema.setDatamartClassess(extendedDatamartClasses);

    return extendedSchema;
  }

  private DatamartClass getExtendedSchema(DatamartClass datamartClass, String prefix, String tablePostfix) {
    DatamartClass datamartClassExtended = new DatamartClass();
    datamartClassExtended.setLabel(datamartClass.getLabel());
    datamartClassExtended.setMnemonic(prefix + datamartClass.getLabel() + tablePostfix);
    datamartClassExtended.setId(UUID.randomUUID());
    List<ClassAttribute> classAttributeList = new ArrayList<>();
    datamartClass.getClassAttributes().forEach(classAttr -> {
      ClassAttribute classAttribute = new ClassAttribute();
      classAttribute.setId(UUID.randomUUID());
      classAttribute.setMnemonic(classAttr.getMnemonic());
      classAttribute.setType(classAttr.getType());
      classAttributeList.add(classAttribute);
    });
    datamartClassExtended.setClassAttributes(classAttributeList);
    return datamartClassExtended;
  }

  private List<ClassAttribute> getExtendedColumns() {
    List<ClassAttribute> classAttributeList = new ArrayList<>();
    classAttributeList.add(generateNewField(SYS_OP_FIELD, ColumnType.INTEGER));
    classAttributeList.add(generateNewField(SYS_TO_FIELD, ColumnType.INTEGER));
    classAttributeList.add(generateNewField(SYS_FROM_FIELD, ColumnType.INTEGER));
    return classAttributeList;
  }

  private ClassAttribute generateNewField(String mnemonic, ColumnType columnType) {
    ClassAttribute classAttribute = new ClassAttribute();
    classAttribute.setId(UUID.randomUUID());
    classAttribute.setMnemonic(mnemonic);
    classAttribute.setType(new TypeMessage(UUID.randomUUID(), columnType));
    return classAttribute;
  }

}
